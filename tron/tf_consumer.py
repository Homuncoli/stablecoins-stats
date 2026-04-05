import logging
from queue import Empty
import threading

from psycopg_pool import ConnectionPool

from model.Tron import TF_QUEUE

def __create_staging_table(cur, staging_table: str):
    cur.execute(f"""
                    CREATE TEMP TABLE {staging_table} (
                        transaction bigint,
                        index smallint,
                        token_asset_id bigint,
                        token_contract_addr bytea,
                        token_t token_type,
                        value bigint,
                        from_addr bytea,
                        from_type addr_type,
                        to_addr bytea,
                        to_type addr_type,
                        success bool
                    )
                """)
    
def __copy_to_staging_table(cur, buffer: list, staging_table: str):
    with cur.copy(f"COPY {staging_table} (transaction, index, token_asset_id, token_contract_addr, token_t, value, from_addr, from_type, to_addr, to_type, success) FROM STDIN") as copy:
        for record in buffer:
            copy.write_row(record)

def __merge_staging_table(cur, staging_table: str):
    cur.execute(f"""
                    INSERT INTO addresses (addr, addr_t)
                    SELECT DISTINCT s.from_addr, s.from_type FROM {staging_table} s
                                        WHERE s.from_addr IS NOT NULL
                    ON CONFLICT (addr) DO NOTHING
                """)
    cur.execute(f"""
                    INSERT INTO addresses (addr, addr_t)
                    SELECT DISTINCT s.to_addr, s.to_type FROM {staging_table} s
                                        WHERE s.to_addr IS NOT NULL
                    ON CONFLICT (addr) DO NOTHING
                """)
    cur.execute(f"""
                    INSERT INTO addresses (addr, addr_t)
                    SELECT DISTINCT s.token_contract_addr, 'Contract'::addr_type FROM {staging_table} s
                                        WHERE s.token_contract_addr IS NOT NULL
                    ON CONFLICT (addr) DO UPDATE SET addr_t = 'Contract'::addr_type
                """)
    cur.execute(f"""
                    INSERT INTO tokens (contract_addr, asset_id, token_t)
                    SELECT DISTINCT a.id, s.token_asset_id, s.token_t FROM {staging_table} s
                        LEFT JOIN addresses a ON s.token_contract_addr = a.addr
                    ON CONFLICT (contract_addr) DO NOTHING
                """)
    cur.execute(f"""
                    INSERT INTO transfers (transaction, index, token, value, from_addr, to_addr, success)
                    SELECT s.transaction, s.index, COALESCE(t.id, 0), s.value, from_a.id, to_a.id, s.success
                    FROM {staging_table} s
                    LEFT JOIN addresses from_a ON s.from_addr = from_a.addr
                    LEFT JOIN addresses to_a ON s.to_addr = to_a.addr
                    LEFT JOIN addresses token_a ON s.token_contract_addr = token_a.addr
                    LEFT JOIN tokens t ON (s.token_contract_addr IS NOT NULL AND token_a.id = t.contract_addr) OR (s.token_asset_id IS NOT NULL AND s.token_asset_id = t.asset_id)
                    ON CONFLICT (transaction, index) DO NOTHING
                """)

def tf_consumer(pool: ConnectionPool, consumer_id: int, stop_event: threading.Event, timeout: float | None, commit_size: int, merge_size: int, metrics: int):
    logger = logging.getLogger(f"tf-consumer-{consumer_id}")
    logger.info("started")

    buffer = []
    staging_table = f"tf_staging_{consumer_id}"

    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                __create_staging_table(cur, staging_table)

                merge_count = 0

                while not stop_event.is_set():
                    try:
                        tx = TF_QUEUE.get(timeout=timeout)
                    except Empty:
                        logger.warning("timed out")
                        continue

                    buffer.append(tx)

                    if len(buffer) >= commit_size:
                        logger.debug("flushing buffer of size %d", len(buffer))
                        __copy_to_staging_table(cur, buffer, staging_table)
                        merge_count += len(buffer)
                        buffer.clear()

                        if merge_count >= merge_size:
                            logger.info("merging staging table after processing %d transfers", merge_count)
                            __merge_staging_table(cur, staging_table)
                            conn.commit()
                            merge_count = 0

                logger.info("stopping, flushing remaining buffer of size %d", len(buffer))
                __copy_to_staging_table(cur, buffer, staging_table)
                __merge_staging_table(cur, staging_table)
                conn.commit()

    except Exception as e:
        logger.fatal("Fatal error", exc_info=e)