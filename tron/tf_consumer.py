import logging
from queue import Empty
import threading

from psycopg_pool import ConnectionPool

from model.Tron import TF_QUEUE

def __create_staging_table(cur, staging_table: str):
    cur.execute(f"""
                    CREATE TEMP TABLE {staging_table} (
                        transaction bigint,
                        idx int,
                        transfer_type transfer_type,
                        token_asset_name bytea,
                        token_contract_addr bytea,
                        token_type text,
                        value bigint,
                        from_addr bytea,
                        to_addr bytea,
                        success bool
                    )
                """)
    
def __copy_to_staging_table(cur, buffer: list, staging_table: str):
    with cur.copy(f"COPY {staging_table} (transaction, idx, transfer_type, token_asset_name, token_contract_addr, token_type, value, from_addr, to_addr, success) FROM STDIN") as copy:
        for record in buffer:
            copy.write_row(record)

def __merge_staging_table(cur, staging_table: str, final_table: str):
    pass

def tf_consumer(pool: ConnectionPool, consumer_id: int, stop_event: threading.Event, timeout: float | None, batch_size: int, metrics: int):
    logger = logging.getLogger(f"tf-consumer-{consumer_id}")
    logger.info("Transfer consumer %d started", consumer_id)

    buffer = []
    staging_table = f"tf_staging_{consumer_id}"

    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                __create_staging_table(cur, staging_table)

                while not stop_event.is_set():
                    try:
                        tx = TF_QUEUE.get(timeout=timeout)
                    except Empty:
                        logging.warning("Transfer consumer %d timed out", consumer_id)
                        continue

                    buffer.append(tx)

                    if len(buffer) >= batch_size:
                        logger.info("Transfer consumer %d flushing buffer of size %d", consumer_id, len(buffer))
                        __copy_to_staging_table(cur, buffer, staging_table)
                        conn.commit()
                        buffer.clear()

                logger.info("Transfer consumer %d stopping, flushing remaining buffer of size %d", consumer_id, len(buffer))
                __copy_to_staging_table(cur, buffer, staging_table)
                __merge_staging_table(cur, staging_table, "transfers")
                conn.commit()

    except Exception as e:
        logger.fatal("Fatal error in transfer consumer %d", consumer_id, exc_info=e)