import logging
import random
import time
from queue import Empty
import threading

import psycopg
from psycopg_pool import ConnectionPool

from metrics import timed
from model.Tron import TF_COPY_TYPES, TRON_QUEUE, TX_COPY_TYPES, TransactionDTO, TransferDTO, to_tf_binary_row, to_tx_binary_row

MERGE_LOCK = threading.Lock()
DB_SYNC_LOCK = threading.Lock()
TOTAL_CONSUMERS = 0

BUFFER_PROGRESS = []
COMMIT_PROGRESS = []
MERGE_PROGRESS = []

def __copy_binary_to_staging_table(cur, buffer: list[tuple], staging_table: str, columns: str, type_names: list[str]):
    if not buffer:
        return

    with timed("copy_binary", "db"):
        with cur.copy(f"COPY {staging_table} ({columns}) FROM STDIN WITH (FORMAT BINARY)") as copy:
            copy.set_types(type_names)
            for row in buffer:
                copy.write_row(row)


def __next_merge_target(base_merge_size: int, rng: random.Random) -> int:
    base = max(1, base_merge_size)
    jitter = max(1, (base * 30) // 100)
    return max(1, base + rng.randint(-jitter, jitter))

def __create_staging_table(cur, staging_table: str):
    cur.execute(f"""
                    SET temp_buffers TO '8GB';
                    SET work_mem TO '4GB';
                """)
    cur.execute(f"""
                    CREATE TEMP TABLE tx_{staging_table} (
                        id bigint,
                        result bool,
                        ts timestamp,
                        transaction_t text,
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
                        token_t text,
                        value_lo bigint,
                        value_hi bigint,
                        from_addr bytea,
                        from_type text,
                        to_addr bytea,
                        to_type text,
                        success bool
                    )
                """)
    
def __merge_staging_table(cur, staging_table: str):
        with timed("merging", "db", log=True):
            cur.execute(f"ANALYZE tx_{staging_table}")
            cur.execute(f"ANALYZE tf_{staging_table}")
            cur.execute(f"""
                            INSERT INTO transactions (id, result, ts, transaction_t, fee_limit, fee, energy_usage, net_fee)
                            SELECT id, result, ts, transaction_t::transaction_type, fee_limit, fee, energy_usage, net_fee
                            FROM tx_{staging_table}
                            ON CONFLICT (id) DO NOTHING
                        """)
            cur.execute(f"""
                            INSERT INTO addresses (addr, addr_t)
                            SELECT DISTINCT addr, addr_t FROM (
                                SELECT from_addr AS addr, from_type::addr_type AS addr_t FROM tf_{staging_table} WHERE from_addr IS NOT NULL
                                UNION
                                SELECT to_addr, to_type::addr_type FROM tf_{staging_table} WHERE to_addr IS NOT NULL
                                UNION
                                SELECT token_contract_addr, 'Contract'::addr_type FROM tf_{staging_table} WHERE token_contract_addr IS NOT NULL
                            ) combined
                            ORDER BY addr
                            ON CONFLICT (addr) DO NOTHING
                        """)
            cur.execute(f"""
                            INSERT INTO tokens (contract_addr, asset_id, token_t)
                            SELECT DISTINCT a.id, s.token_asset_id, s.token_t::token_type FROM tf_{staging_table} s
                                LEFT JOIN addresses a ON s.token_contract_addr = a.addr
                            ON CONFLICT DO NOTHING
                        """)
            cur.execute(f"""
                            INSERT INTO transfers (transaction, index, token, value_lo, value_hi, from_addr, to_addr, success)
                            SELECT 
                              s.transaction,
                              s.index,
                              COALESCE(t1.id, t2.id, 0),
                              s.value_lo,
                              s.value_hi,
                              from_a.id,
                              to_a.id,
                              s.success
                            FROM tf_{staging_table} s
                            LEFT JOIN addresses from_a  ON s.from_addr            = from_a.addr
                            LEFT JOIN addresses to_a    ON s.to_addr              = to_a.addr
                            LEFT JOIN addresses token_a ON s.token_contract_addr  = token_a.addr
                            LEFT JOIN tokens t1         ON s.token_contract_addr IS NOT NULL 
                                                       AND token_a.id             = t1.contract_addr
                            LEFT JOIN tokens t2         ON s.token_asset_id IS NOT NULL 
                                                       AND s.token_asset_id       = t2.asset_id
                            ON CONFLICT (transaction, index) DO NOTHING;
                        """)
            cur.execute(f"TRUNCATE TABLE tx_{staging_table}, tf_{staging_table}")


def __merge_staging_table_with_retry(cur, staging_table: str, logger: logging.Logger, retries: int = 3):
    for attempt in range(1, retries + 1):
        cur.execute("SAVEPOINT merge_staging")
        try:
            __merge_staging_table(cur, staging_table)
            cur.execute("RELEASE SAVEPOINT merge_staging")
            return
        except psycopg.errors.DeadlockDetected:
            cur.execute("ROLLBACK TO SAVEPOINT merge_staging")
            if attempt >= retries:
                raise

            delay = 0.25 * attempt
            logger.warning(
                "deadlock while merging staging table, retrying attempt %d/%d in %.2fs",
                attempt,
                retries,
                delay,
                exc_info=True
            )
            time.sleep(delay)

def db_consumer(pool: ConnectionPool, consumer_id: int, stop_event: threading.Event, timeout: float | None, commit_size: int, merge_size: int, metrics: int):
    logger = logging.getLogger(f"db-consumer-{consumer_id}")
    logger.debug("started")

    staging_table = f"staging_{consumer_id}"
    tx_buffer: list[tuple] = []
    tf_buffer: list[tuple] = []
    tx_buffer_rows = 0

    try:
        with pool.connection() as conn:
            conn.execute(f"SET application_name TO 'db_consumer_{consumer_id}';")
            with conn.cursor() as cur:
                __create_staging_table(cur, staging_table)

                uncommited_tx = 0

                def sync_staging(force: bool) -> bool:
                    nonlocal uncommited_tx, tx_buffer_rows
                    
                    try:
                        if tx_buffer_rows > 0 and (force or tx_buffer_rows >= commit_size):
                            logger.debug("copying binary buffers to staging after processing %d transactions", tx_buffer_rows)
                            __copy_binary_to_staging_table(cur, tx_buffer, f"tx_{staging_table}", "id, result, ts, transaction_t, fee_limit, fee, energy_usage, net_fee", TX_COPY_TYPES)
                            __copy_binary_to_staging_table(cur, tf_buffer, f"tf_{staging_table}", "transaction, index, token_asset_id, token_contract_addr, token_t, value_lo, value_hi, from_addr, from_type, to_addr, to_type, success", TF_COPY_TYPES)
                            uncommited_tx += tx_buffer_rows
                            COMMIT_PROGRESS[consumer_id] += tx_buffer_rows
                            tx_buffer.clear()
                            tf_buffer.clear()
                            tx_buffer_rows = 0
                    finally:
                        pass

                    if uncommited_tx == 0:
                        return False
                    
                    if not DB_SYNC_LOCK.acquire():
                        return False

                    try:
                        if uncommited_tx > 0:
                            logger.info("merging staging table after processing %d transactions", uncommited_tx)
                            __merge_staging_table_with_retry(cur, staging_table, logger)
                            with timed("commit", "db"):
                                conn.commit()
                            MERGE_PROGRESS[consumer_id] += uncommited_tx
                            uncommited_tx = 0
                            logger.debug("next randomized merge size set to %d transactions", merge_size)
                        return True
                    finally:
                        DB_SYNC_LOCK.release()

                while True:
                    with timed("queue", "db"):
                        try:
                            block_data = TRON_QUEUE.get(timeout=timeout)
                        except Empty:
                            if not stop_event.is_set():
                                logger.warning("timed out")
                                continue
                            else:
                                break

                        try:
                            if block_data is None:
                                logger.debug("received shutdown sentinel")
                                break

                            for tx, tfs in block_data:
                                tx_buffer.append(to_tx_binary_row(tx))
                                for tf in tfs:
                                    tf_buffer.append(to_tf_binary_row(tf))
                            tx_buffer_rows += len([tx for tx, _ in block_data])
                            BUFFER_PROGRESS[consumer_id] += 1
                        finally:
                            TRON_QUEUE.task_done()

                    force = uncommited_tx > merge_size
                    if force:
                        logger.info("force syncing staging table due to buffer size %d exceeding merge target %d", uncommited_tx, merge_size)
                    sync_staging(force=force)

                if tx_buffer_rows > 0 or uncommited_tx > 0:
                    logger.info("stopping, flushing remaining %d transactions to DB", tx_buffer_rows + uncommited_tx)

                while tx_buffer_rows > 0 or uncommited_tx > 0:
                    sync_staging(force=True)

                logger.debug("stopped")

    except Exception as e:
        logger.fatal("fatal error", exc_info=e)