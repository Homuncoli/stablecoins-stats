import logging
import random
import time
from queue import Empty
import threading
from datetime import datetime, timezone

import psycopg
from psycopg_pool import ConnectionPool

from metrics import timed
from model.Tron import TRON_QUEUE, TransactionDTO, TransferDTO

MERGE_LOCK = threading.Lock()


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
    with MERGE_LOCK:
        with timed("merging", "db"):
            cur.execute(f"""
                            INSERT INTO transactions (id, result, ts, transaction_t, fee_limit, fee, energy_usage, net_fee)
                            SELECT id, result, ts, transaction_t::transaction_type, fee_limit, fee, energy_usage, net_fee
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
                            SELECT DISTINCT s.from_addr, s.from_type::addr_type FROM tf_{staging_table} s
                                                WHERE s.from_addr IS NOT NULL
                            ON CONFLICT (addr) DO NOTHING
                        """)
            cur.execute(f"""
                            INSERT INTO addresses (addr, addr_t)
                            SELECT DISTINCT s.to_addr, s.to_type::addr_type FROM tf_{staging_table} s
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
                            SELECT DISTINCT a.id, s.token_asset_id, s.token_t::token_type FROM tf_{staging_table} s
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
            )
            time.sleep(delay)

def db_consumer(pool: ConnectionPool, consumer_id: int, stop_event: threading.Event, timeout: float | None, commit_size: int, merge_size: int, metrics: int):
    logger = logging.getLogger(f"db-consumer-{consumer_id}")
    logger.debug("started")

    staging_table = f"staging_{consumer_id}"
    tx_buffer: list[tuple] = []
    tf_buffer: list[tuple] = []
    tx_buffer_rows = 0
    rng = random.Random((consumer_id + 1) * 9973)

    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                __create_staging_table(cur, staging_table)
                tx_copy_types = ["int8", "bool", "timestamp", "text", "int8", "int8", "int8", "int8"]
                tf_copy_types = [
                    "int8",
                    "int2",
                    "int8",
                    "bytea",
                    "text",
                    "int8",
                    "int8",
                    "bytea",
                    "text",
                    "bytea",
                    "text",
                    "bool",
                ]

                def to_tx_binary_row(row: TransactionDTO):
                    return (
                        row[0],
                        row[1],
                        row[2].astimezone(timezone.utc).replace(tzinfo=None),
                        row[3],
                        row[4],
                        row[5],
                        row[6],
                        row[7],
                    )

                def to_tf_binary_row(row: TransferDTO):
                    return (
                        row[0],
                        row[1],
                        row[2],
                        row[3],
                        row[4],
                        row[5],
                        row[6],
                        row[7],
                        row[8],
                        row[9],
                        row[10],
                        row[11],
                    )

                uncommited_tx = 0
                merge_target = __next_merge_target(merge_size, rng)
                commit_target = __next_merge_target(commit_size, rng)
                logger.debug("initial randomized sizes to %d commits %d merges", commit_target, merge_target)
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
                        finally:
                            TRON_QUEUE.task_done()

                    if tx_buffer_rows >= commit_target:
                        logger.debug("copying binary buffers to staging after processing %d transactions", tx_buffer_rows)
                        __copy_binary_to_staging_table(cur, tx_buffer, f"tx_{staging_table}", "id, result, ts, transaction_t, fee_limit, fee, energy_usage, net_fee", tx_copy_types)
                        __copy_binary_to_staging_table(cur, tf_buffer, f"tf_{staging_table}", "transaction, index, token_asset_id, token_contract_addr, token_t, value_lo, value_hi, from_addr, from_type, to_addr, to_type, success", tf_copy_types)
                        uncommited_tx += tx_buffer_rows
                        tx_buffer.clear()
                        tf_buffer.clear()
                        tx_buffer_rows = 0
                        commit_target = __next_merge_target(commit_size, rng)

                    if uncommited_tx >= merge_target:
                        logger.debug("merging staging table after processing %d transactions", uncommited_tx)
                        __merge_staging_table_with_retry(cur, staging_table, logger)
                        with timed("commit", "db"):
                            conn.commit()
                        uncommited_tx = 0
                        merge_target = __next_merge_target(merge_size, rng)
                        logger.debug("next randomized merge size set to %d transactions", merge_target)

                if tx_buffer_rows > 0:
                    logger.info("stopping, flushing remaining binary buffers")
                    __copy_binary_to_staging_table(cur, tx_buffer, f"tx_{staging_table}", "id, result, ts, transaction_t, fee_limit, fee, energy_usage, net_fee", tx_copy_types)
                    __copy_binary_to_staging_table(cur, tf_buffer, f"tf_{staging_table}", "transaction, index, token_asset_id, token_contract_addr, token_t, value_lo, value_hi, from_addr, from_type, to_addr, to_type, success", tf_copy_types)
                    uncommited_tx += tx_buffer_rows
                    tx_buffer_rows = 0
                logger.debug("merging staging table for the last time with %d uncommited transactions", uncommited_tx)
                __merge_staging_table_with_retry(cur, staging_table, logger)
                with timed("commit", "db"):
                    conn.commit()
                logger.debug("stopped")

    except Exception as e:
        logger.fatal("fatal error", exc_info=e)