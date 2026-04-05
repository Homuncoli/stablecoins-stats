import logging
from queue import Empty
import threading

from psycopg_pool import ConnectionPool

from model.Tron import TX_QUEUE, TransactionDTO

def __create_staging_table(cur, staging_table: str):
    cur.execute(f"""
                    CREATE TEMP TABLE {staging_table} (
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
    
def __merge_staging_table(cur, staging_table: str):
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
                        FROM {staging_table}
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

def __copy_to_staging_table(cur, buffer: list[TransactionDTO], staging_table: str):
    with cur.copy(f"COPY {staging_table} (id, result, ts, transaction_t, fee_limit, fee, energy_usage, net_fee) FROM STDIN") as copy:
        for record in buffer:
            copy.write_row(record)

def tx_consumer(pool: ConnectionPool, consumer_id: int, stop_event: threading.Event, timeout: float | None, commit_size: int, merge_size: int, metrics: int):
    logger = logging.getLogger(f"tx-consumer-{consumer_id}")
    logger.info("started")

    staging_table = f"tx_staging_{consumer_id}"

    buffer = []
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                __create_staging_table(cur, staging_table)

                merge_counter = 0
                while not stop_event.is_set():
                    try:
                        tx = TX_QUEUE.get(timeout=timeout)
                    except Empty:
                        logger.warning("timed out")
                        continue

                    buffer.append(tx)

                    if len(buffer) >= commit_size:
                        __copy_to_staging_table(cur, buffer, staging_table)
                        merge_counter += len(buffer)
                        buffer.clear()

                        if merge_counter >= merge_size:
                            logger.info("merging staging table after processing %d transactions", merge_counter)
                            __merge_staging_table(cur, staging_table)
                            conn.commit()
                            merge_counter = 0
                        

                logger.info("stopping, flushing remaining buffer of size %d", len(buffer))
                __copy_to_staging_table(cur, buffer, staging_table)
                __merge_staging_table(cur, staging_table)
                conn.commit()

    except Exception as e:
        logger.fatal("Fatal error", exc_info=e)