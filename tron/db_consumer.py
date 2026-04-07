import logging
from queue import Empty
import threading

from psycopg_pool import ConnectionPool

from metrics import timed
from model.Tron import TRON_QUEUE, TransactionDTO, TransferDTO

MERGE_LOCK = threading.Lock()

def __create_staging_table(cur, staging_table: str):
    cur.execute(f"""
                    CREATE TEMP TABLE tx_{staging_table} (
                        id bigint,
                        result bool,
                        ts timestamp,
                        transaction_t transaction_type,
                        fee_limit bigint,
                        fee bigint,
                        energy_usage bigint,
                        net_fee bigint
                    )
                """)
    cur.execute(f"""
                    CREATE TEMP TABLE tf_{staging_table} (
                        transaction bigint,
                        index smallint,
                        token_asset_id bigint,
                        token_contract_addr bytea,
                        token_t token_type,
                        value_lo bigint,
                        value_hi bigint,
                        from_addr bytea,
                        from_type addr_type,
                        to_addr bytea,
                        to_type addr_type,
                        success bool
                    )
                """)
    
def __merge_staging_table(cur, staging_table: str):
    with MERGE_LOCK:
        with timed("merging", "db"):
            cur.execute(f"""
                            INSERT INTO transactions (id, result, ts, transaction_t, fee_limit, fee, energy_usage, net_fee)
                            SELECT id, result, ts, transaction_t, fee_limit, fee, energy_usage, net_fee
                            FROM (
                                SELECT DISTINCT ON (id)
                                    id,
                                    result,
                                    ts,
                                    transaction_t,
                                    fee_limit,
                                    fee,
                                    energy_usage,
                                    net_fee
                                FROM tx_{staging_table}
                                ORDER BY id, ts DESC NULLS LAST
                            ) deduped
                            ON CONFLICT (id) DO UPDATE SET
                                result = EXCLUDED.result, 
                                ts = EXCLUDED.ts,
                                transaction_t = EXCLUDED.transaction_t,
                                fee_limit = EXCLUDED.fee_limit,
                                fee = EXCLUDED.fee,
                                energy_usage = EXCLUDED.energy_usage,
                                net_fee = EXCLUDED.net_fee
                        """)
            cur.execute(f"""
                            INSERT INTO addresses (addr, addr_t)
                            SELECT DISTINCT s.from_addr, s.from_type FROM tf_{staging_table} s
                                                WHERE s.from_addr IS NOT NULL
                            ON CONFLICT (addr) DO NOTHING
                        """)
            cur.execute(f"""
                            INSERT INTO addresses (addr, addr_t)
                            SELECT DISTINCT s.to_addr, s.to_type FROM tf_{staging_table} s
                                                WHERE s.to_addr IS NOT NULL
                            ON CONFLICT (addr) DO NOTHING
                        """)
            cur.execute(f"""
                            INSERT INTO addresses (addr, addr_t)
                            SELECT DISTINCT s.token_contract_addr, 'Contract'::addr_type FROM tf_{staging_table} s
                                                WHERE s.token_contract_addr IS NOT NULL
                            ON CONFLICT (addr) DO UPDATE SET addr_t = 'Contract'::addr_type
                        """)
            cur.execute(f"""
                            INSERT INTO tokens (contract_addr, asset_id, token_t)
                            SELECT DISTINCT a.id, s.token_asset_id, s.token_t FROM tf_{staging_table} s
                                LEFT JOIN addresses a ON s.token_contract_addr = a.addr
                            ON CONFLICT DO NOTHING
                        """)
            cur.execute(f"""
                            INSERT INTO transfers (transaction, index, token, value_lo, value_hi, from_addr, to_addr, success)
                            SELECT s.transaction, s.index, COALESCE(t.id, 0), s.value_lo, s.value_hi, from_a.id, to_a.id, s.success
                            FROM tf_{staging_table} s
                            LEFT JOIN addresses from_a ON s.from_addr = from_a.addr
                            LEFT JOIN addresses to_a ON s.to_addr = to_a.addr
                            LEFT JOIN addresses token_a ON s.token_contract_addr = token_a.addr
                            LEFT JOIN tokens t ON (s.token_contract_addr IS NOT NULL AND token_a.id = t.contract_addr) OR (s.token_asset_id IS NOT NULL AND s.token_asset_id = t.asset_id)
                            ON CONFLICT (transaction, index) DO NOTHING
                        """)
    
def __copy_tx_to_staging_table(cur, buffer: list[TransactionDTO], staging_table: str):
    with timed("copy_tx", "db"):
        with cur.copy(f"COPY tx_{staging_table} (id, result, ts, transaction_t, fee_limit, fee, energy_usage, net_fee) FROM STDIN") as copy:
            for record in buffer:
                copy.write_row(record)

def __copy_tf_to_staging_table(cur, buffer: list[TransferDTO], staging_table: str):
    with timed("copy_tf", "db"):
        with cur.copy(f"COPY tf_{staging_table} (transaction, index, token_asset_id, token_contract_addr, token_t, value_lo, value_hi, from_addr, from_type, to_addr, to_type, success) FROM STDIN") as copy:
            for record in buffer:
                copy.write_row(record)

def db_consumer(pool: ConnectionPool, consumer_id: int, stop_event: threading.Event, timeout: float | None, commit_size: int, merge_size: int, metrics: int):
    logger = logging.getLogger(f"db-consumer-{consumer_id}")
    logger.debug("started")

    tx_buffer: list[TransactionDTO] = []
    tf_buffer: list[TransferDTO] = []
    staging_table = f"staging_{consumer_id}"

    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                __create_staging_table(cur, staging_table)

                uncommited_tx = 0
                while not stop_event.is_set() or not TRON_QUEUE.empty():
                    tx, tfs = None, None
                    try:
                        tx, tfs = TRON_QUEUE.get(timeout=timeout)
                    except Empty:
                        pass

                    if tx is None and tfs is None:
                        logger.warning("timed out")
                        continue

                    if tx is not None:
                        tx_buffer.append(tx)

                    if tfs is not None:
                        tf_buffer.extend(tfs)

                    if len(tx_buffer) >= commit_size:
                        __copy_tx_to_staging_table(cur, tx_buffer, staging_table)
                        __copy_tf_to_staging_table(cur, tf_buffer, staging_table)
                        uncommited_tx += len(tx_buffer)
                        tx_buffer.clear()
                        tf_buffer.clear()

                    if uncommited_tx >= merge_size:
                        logger.debug("merging staging table after processing %d transactions", uncommited_tx)
                        __merge_staging_table(cur, staging_table)
                        with timed("commit", "db"):
                            conn.commit()
                        uncommited_tx = 0
                        
                if len(tx_buffer) > 0 or len(tf_buffer) > 0:
                    logger.info("stopping, flushing remaining buffers of size %d transactions and %d transfers", len(tx_buffer), len(tf_buffer))
                    __copy_tx_to_staging_table(cur, tx_buffer, staging_table)
                    __copy_tf_to_staging_table(cur, tf_buffer, staging_table)
                __merge_staging_table(cur, staging_table)
                with timed("commit", "db"):
                    conn.commit()

    except Exception as e:
        logger.fatal("fatal error", exc_info=e)