import csv
import logging
import random
from queue import Empty
from pathlib import Path
import tempfile
import threading
from datetime import datetime, timezone
from typing import TextIO

from psycopg_pool import ConnectionPool

from metrics import timed
from model.Tron import TRON_QUEUE, TransactionDTO, TransferDTO

MERGE_LOCK = threading.Lock()


def __reset_open_csv(csv_file: TextIO):
    csv_file.seek(0)
    csv_file.truncate(0)
    csv_file.flush()


def __serialize_timestamp(value: datetime) -> str:
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value.isoformat(sep=" ", timespec="seconds")


def __serialize_bytea(value: bytes | None) -> str:
    if value is None:
        return ""
    return f"\\x{value.hex()}"


def __serialize_tx_row(record: TransactionDTO):
    return (
        record[0],
        record[1],
        __serialize_timestamp(record[2]),
        record[3],
        record[4],
        record[5],
        record[6],
        record[7],
    )


def __serialize_tf_row(record: TransferDTO):
    return (
        record[0],
        record[1],
        record[2],
        __serialize_bytea(record[3]),
        record[4],
        record[5],
        record[6],
        __serialize_bytea(record[7]),
        record[8],
        __serialize_bytea(record[9]),
        record[10],
        record[11],
    )


def __append_rows_to_csv(writer: csv.writer, rows: list, serializer):
    if not rows:
        return

    writer.writerows(serializer(row) for row in rows)


def __copy_csv_to_staging_table(cur, csv_file: TextIO, staging_table: str, columns: str):
    csv_file.flush()
    if csv_file.tell() == 0:
        return

    with timed("copy_csv", "db"):
        csv_file.seek(0)
        with cur.copy(f"COPY {staging_table} ({columns}) FROM STDIN WITH (FORMAT csv, DELIMITER ',', NULL '')") as copy:
            copy.write(csv_file.read())


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

def db_consumer(pool: ConnectionPool, consumer_id: int, stop_event: threading.Event, timeout: float | None, commit_size: int, merge_size: int, metrics: int):
    logger = logging.getLogger(f"db-consumer-{consumer_id}")
    logger.debug("started")

    staging_table = f"staging_{consumer_id}"
    tx_csv_path = Path(tempfile.gettempdir()) / f"stablecoins-stats-db-consumer-{consumer_id}-tx.csv"
    tf_csv_path = Path(tempfile.gettempdir()) / f"stablecoins-stats-db-consumer-{consumer_id}-tf.csv"
    tx_csv_rows = 0
    rng = random.Random((consumer_id + 1) * 9973)

    try:
        with tx_csv_path.open("w+", newline="", encoding="utf-8") as tx_csv_file, tf_csv_path.open("w+", newline="", encoding="utf-8") as tf_csv_file:
            tx_csv_writer = csv.writer(tx_csv_file)
            tf_csv_writer = csv.writer(tf_csv_file)

            with pool.connection() as conn:
                with conn.cursor() as cur:
                    __create_staging_table(cur, staging_table)

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
                                    TRON_QUEUE.task_done()
                                    break

                                block_tx_rows: list[TransactionDTO] = []
                                block_tf_rows: list[TransferDTO] = []
                                for tx, tfs in block_data:
                                    block_tx_rows.append(tx)
                                    block_tf_rows.extend(tfs)

                                __append_rows_to_csv(tx_csv_writer, block_tx_rows, __serialize_tx_row)
                                __append_rows_to_csv(tf_csv_writer, block_tf_rows, __serialize_tf_row)
                                tx_csv_rows += len(block_tx_rows)
                            finally:
                                TRON_QUEUE.task_done()

                        if tx_csv_rows >= commit_target:
                            logger.debug("copying csv buffers to staging after processing %d transactions", tx_csv_rows)
                            __copy_csv_to_staging_table(cur, tx_csv_file, f"tx_{staging_table}", "id, result, ts, transaction_t, fee_limit, fee, energy_usage, net_fee")
                            __copy_csv_to_staging_table(cur, tf_csv_file, f"tf_{staging_table}", "transaction, index, token_asset_id, token_contract_addr, token_t, value_lo, value_hi, from_addr, from_type, to_addr, to_type, success")
                            uncommited_tx += tx_csv_rows
                            tx_csv_rows = 0
                            __reset_open_csv(tx_csv_file)
                            __reset_open_csv(tf_csv_file)

                        if uncommited_tx >= merge_target:
                            logger.debug("merging staging table after processing %d transactions (randomized target=%d)", uncommited_tx, merge_target)
                            __merge_staging_table(cur, staging_table)
                            with timed("commit", "db"):
                                conn.commit()
                            uncommited_tx = 0
                            merge_target = __next_merge_target(merge_size, rng)
                            logger.debug("next randomized merge size set to %d transactions", merge_target)

                    if tx_csv_rows > 0:
                        logger.info("stopping, flushing remaining csv rows")
                        __copy_csv_to_staging_table(cur, tx_csv_file, f"tx_{staging_table}", "id, result, ts, transaction_t, fee_limit, fee, energy_usage, net_fee")
                        __copy_csv_to_staging_table(cur, tf_csv_file, f"tf_{staging_table}", "transaction, index, token_asset_id, token_contract_addr, token_t, value_lo, value_hi, from_addr, from_type, to_addr, to_type, success")
                        uncommited_tx += tx_csv_rows
                        tx_csv_rows = 0
                    logger.debug("merging staging table for the last time with %d uncommited transactions", uncommited_tx)
                    __merge_staging_table(cur, staging_table)
                    with timed("commit", "db"):
                        conn.commit()
                    __reset_open_csv(tx_csv_file)
                    __reset_open_csv(tf_csv_file)
                    logger.debug("stopped")

    except Exception as e:
        logger.fatal("fatal error", exc_info=e)