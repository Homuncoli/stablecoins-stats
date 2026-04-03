import logging
from queue import Empty
import threading

from psycopg_pool import ConnectionPool

from model.Tron import TX_QUEUE, TransactionDTO

def __create_staging_table(cur, staging_table: str):
    cur.execute(f"""
                    CREATE TEMP TABLE {staging_table} (
                        id bigint,
                        block bigint,
                        result bool,
                        ts timestamp,
                        transaction_t transaction_type,
                        fee_limit bigint,
                        fee bigint,
                        energy_usage bigint,
                        net_fee bigint
                    )
                """)
    
def __merge_staging_table(cur, staging_table: str, final_table: str):
    cur.execute(f"""
                    INSERT INTO {final_table} (id, block, result, ts, transaction_t, fee_limit, fee, energy_usage, net_fee)
                    SELECT id, block, result, ts, transaction_t, fee_limit, fee, energy_usage, net_fee
                    FROM (
                        SELECT DISTINCT ON (id)
                            id,
                            block,
                            result,
                            ts,
                            transaction_t,
                            fee_limit,
                            fee,
                            energy_usage,
                            net_fee
                        FROM {staging_table}
                        ORDER BY id, ts DESC NULLS LAST, block DESC NULLS LAST
                    ) deduped
                    ON CONFLICT (id) DO UPDATE SET
                        block = EXCLUDED.block,
                        result = EXCLUDED.result,
                        ts = EXCLUDED.ts,
                        transaction_t = EXCLUDED.transaction_t,
                        fee_limit = EXCLUDED.fee_limit,
                        fee = EXCLUDED.fee,
                        energy_usage = EXCLUDED.energy_usage,
                        net_fee = EXCLUDED.net_fee
                """)

def __copy_to_staging_table(cur, buffer: list[TransactionDTO], staging_table: str):
    with cur.copy(f"COPY {staging_table} (id, block, result, ts, transaction_t, fee_limit, fee, energy_usage, net_fee) FROM STDIN") as copy:
        for record in buffer:
            copy.write_row(record)

def tx_consumer(pool: ConnectionPool, consumer_id: int, stop_event: threading.Event, timeout: float | None, batch_size: int, metrics: int):
    logger = logging.getLogger(f"tx-consumer-{consumer_id}")
    logger.info("Transaction consumer %d started", consumer_id)

    staging_table = f"tx_staging_{consumer_id}"

    buffer = []
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                __create_staging_table(cur, staging_table)

                while not stop_event.is_set():
                    try:
                        tx = TX_QUEUE.get(timeout=timeout)
                    except Empty:
                        logging.warning("Transaction consumer %d timed out", consumer_id)
                        continue

                    buffer.append(tx)

                    if len(buffer) >= batch_size:
                        logger.info("Transaction consumer %d flushing buffer of size %d", consumer_id, len(buffer))
                        __copy_to_staging_table(cur, buffer, staging_table)
                        conn.commit()
                        buffer.clear()

                logger.info("Transaction consumer %d stopping, flushing remaining buffer of size %d", consumer_id, len(buffer))
                __copy_to_staging_table(cur, buffer, staging_table)
                __merge_staging_table(cur, staging_table, "transactions")
                conn.commit()

    except Exception as e:
        logger.fatal("Fatal error in transaction consumer %d", consumer_id, exc_info=e)