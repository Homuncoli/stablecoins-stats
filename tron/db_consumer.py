import logging
import queue
import random
import sys
import time
from queue import Empty
import threading

import psycopg
from psycopg_pool import ConnectionPool

from metrics import timed
from model.Tron import TF_COPY_TYPES, TX_COPY_TYPES, to_tx_binary_row
from tron import address as address_module
from tron import token as token_module
from db_schema import copy_binary_rows



MERGE_LOCK = threading.Lock()
DB_SYNC_LOCK = threading.Lock()
TOTAL_CONSUMERS = 0

BUFFERED_BLOCKS = []
COMMITED_BLOCKS = []
MERGED_BLOCKS   = []
LOOKUP_HITS = []
LOOKUP_MISSES = []
DB_STATE = []


def __initialize_shared_lookups(cur, logger):
    address_module.initialize_address_cache(cur, logger)
    token_module.initialize_token_cache(cur, logger) 

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
                        token bigint,
                        value_lo bigint,
                        value_hi bigint,
                        from_addr bigint,
                        to_addr bigint,
                        success bool
                    )
                """)
    cur.execute(f"""
                    CREATE TEMP TABLE addr_{staging_table} (
                        addr bytea,
                        addr_t text
                    )
                """)
    cur.execute(f"""
                    CREATE TEMP TABLE token_{staging_table} (
                        contract_addr_id bigint,
                        asset_id bigint,
                        token_type text
                    )
                """)


def __merge_staging_table(cur, staging_table: str):
    cur.execute(f"ANALYZE tx_{staging_table}")
    cur.execute(f"ANALYZE tf_{staging_table}")
    cur.execute(f"""
                    INSERT INTO transactions (id, result, ts, transaction_t, fee_limit, fee, energy_usage, net_fee)
                    SELECT id, result, ts, transaction_t::transaction_type, fee_limit, fee, energy_usage, net_fee
                    FROM tx_{staging_table}
                    ON CONFLICT (id) DO NOTHING
                """)
    cur.execute(f"""
                    INSERT INTO transfers (transaction, index, token, value_lo, value_hi, from_addr, to_addr, success)
                    SELECT transaction, index, token, value_lo, value_hi, from_addr, to_addr, success
                    FROM tf_{staging_table}
                    ON CONFLICT (transaction, index) DO NOTHING
                """)
    cur.execute(f"TRUNCATE TABLE tx_{staging_table}")
    cur.execute(f"TRUNCATE TABLE tf_{staging_table}")


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

def sync_staging(consumer_id, conn, cur, staging_table: str, uncommited_blocks: int, unmerged_blocks: int, tx: list, resolved_tf: list, unresolved_tf: list, force: bool, commit_size: int, logger: logging.Logger) -> tuple[int, int]:
    buffered_rows = len(tx) + len(resolved_tf) + len(unresolved_tf)
    should_flush_new_rows = buffered_rows > 0 and (force or uncommited_blocks >= commit_size)

    if not should_flush_new_rows:
        return uncommited_blocks, unmerged_blocks
    
    released = False
    if should_flush_new_rows:
        released = True

        DB_STATE[consumer_id] = "FORCE_FLUSH" if force else "FLUSH"
        logger.debug("flushing to staging table, buffered blocks: %d", uncommited_blocks)
        
        with timed("syncing", "db"):
            token_new, token_unresolved_new = token_module.TOKEN_CACHE.new_snapshot(logger)
            addr_new = address_module.ADDRESS_CACHE.new_snapshot(logger)
            address_module.ADDRESS_CACHE.commit(cur, staging_table, addr_new, logger)
            token_module.TOKEN_CACHE.commit(cur, staging_table, token_new, token_unresolved_new, logger)

        with timed("resolving", "db"):
            tfs = resolved_tf
            resolved_squared = [resolve_tf(tf) for tf in unresolved_tf]
            tfs.extend([row for row, insert in resolved_squared if insert])
            for row, insert in resolved_squared:
                if not insert:
                    logger.warning(f"unresolved transfer after resolving {row=}")
                    unresolved_tf.append(row)
           
        with timed("copying", "db"):
            copy_binary_rows(cur, tx, f"tx_{staging_table}", "id, result, ts, transaction_t, fee_limit, fee, energy_usage, net_fee", TX_COPY_TYPES)
            copy_binary_rows(cur, tfs, f"tf_{staging_table}", "transaction, index, token, value_lo, value_hi, from_addr, to_addr, success", TF_COPY_TYPES)
            conn.commit()

        COMMITED_BLOCKS[consumer_id] += uncommited_blocks
        unmerged_blocks += uncommited_blocks
        uncommited_blocks = 0

        tx.clear()
        resolved_tf.clear()
        unresolved_tf.clear()
        
    if unmerged_blocks == 0:
        DB_STATE[consumer_id] = "QUEUE"
        return uncommited_blocks, unmerged_blocks
    
    DB_STATE[consumer_id] = "LOCKING" if force else DB_STATE[consumer_id]
    if not DB_SYNC_LOCK.acquire(blocking=force):
        DB_STATE[consumer_id] = "QUEUE"
        return uncommited_blocks, unmerged_blocks
    
    DB_STATE[consumer_id] = "MERGING"
    try:
        logger.info("merging staging table after processing %d blocks", unmerged_blocks)
        with timed("merging", "db"):
            __merge_staging_table_with_retry(cur, staging_table, logger)
            conn.commit()
        MERGED_BLOCKS[consumer_id] += unmerged_blocks
        unmerged_blocks = 0
        return uncommited_blocks, 0
    finally:
        DB_SYNC_LOCK.release()
        DB_STATE[consumer_id] = "QUEUE"

def db_consumer(pool: ConnectionPool, consumer_id: int, stop_event: threading.Event, timeout: float | None, commit_size: int, merge_size: int, metrics: int):
    logger = logging.getLogger(f"db-consumer-{consumer_id}")
    logger.debug("started")

    staging_table = f"staging_{consumer_id}"
    tx_buffer: list[tuple] = []
    unresolved_tf_buffer: list[tuple] = []
    resolved_tf_buffer: list[tuple] = []

    try:
        with pool.connection() as conn:
            conn.execute(f"SET application_name TO 'db_consumer_{consumer_id}';")
            conn.execute(f"SET synchronous_commit = off;")
            with conn.cursor() as cur:
                logger.info("initializing shared lookups")
                __initialize_shared_lookups(cur, logger)
                __create_staging_table(cur, staging_table)

                uncommited_blocks = 0
                unmerged_blocks = 0

                while True:  # Each consumer can loop independently without global lock
                    with timed("queue", "db"):
                        DB_STATE[consumer_id] = "QUEUE"
                        try:
                            block_data = queue.TRON_QUEUE.get(timeout=timeout)
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
                                    row, insert = resolve_tf(tf)

                                    if insert:
                                        LOOKUP_HITS[consumer_id] += 1
                                        resolved_tf_buffer.append(row)
                                    else:
                                        LOOKUP_MISSES[consumer_id] += 1
                                        unresolved_tf_buffer.append(tf)

                            uncommited_blocks += 1
                            BUFFERED_BLOCKS[consumer_id] += 1
                        finally:
                            queue.TRON_QUEUE.task_done()

                    force = unmerged_blocks > merge_size
                    if force:
                        logger.info("force syncing staging table due to buffer size %d exceeding merge target %d", unmerged_blocks, merge_size)
                    uncommited_blocks, unmerged_blocks = sync_staging(consumer_id, conn, cur, staging_table, uncommited_blocks, unmerged_blocks, tx_buffer, resolved_tf_buffer, unresolved_tf_buffer, force, commit_size, logger)

                while uncommited_blocks > 0 or unmerged_blocks > 0:
                    logger.info("stopping, flushing and merging remaining blocks, uncommited: %d, unmerged: %d", uncommited_blocks, unmerged_blocks)
                    uncommited_blocks, unmerged_blocks = sync_staging(consumer_id, conn, cur, staging_table, uncommited_blocks, unmerged_blocks, tx_buffer, resolved_tf_buffer, unresolved_tf_buffer, True, commit_size, logger)

                logger.debug("stopped")

    except Exception as e:
        logger.fatal("fatal error", exc_info=e)

def resolve_tf(tf):
    insert = True
    row = (tf[0], tf[1], 0, tf[5], tf[6], None, None, tf[11])
    if tf[2]: # asset_id
        token_id_by_asset = token_module.TOKEN_CACHE.get_by_asset_id(tf[2])
        if token_id_by_asset is not None:
            row = row[:2] + (token_id_by_asset,) + row[3:]
        else:
            token_module.TOKEN_CACHE.try_new(None, tf[2], tf[4])
            insert = False
    if tf[3]: # contract_addr
        token_id_by_address = token_module.TOKEN_CACHE.get_by_address(tf[3])
        if token_id_by_address is not None:
            row = row[:2] + (token_id_by_address,) + row[3:]
        else:
            contract_addr_id = address_module.ADDRESS_CACHE.get(tf[3])
            if contract_addr_id is not None:
                token_module.TOKEN_CACHE.try_new(contract_addr_id, None, tf[4])
            else:
                address_module.ADDRESS_CACHE.try_new(tf[3], "Contract")
                token_module.TOKEN_CACHE.try_new_unknown_contract(tf[3], tf[2], tf[4])
            insert = False

    if tf[7]: # from_addr
        from_addr_id = address_module.ADDRESS_CACHE.get(tf[7])
        if from_addr_id is not None:
            row = row[:5] + (from_addr_id,) + row[6:]
        else:
            address_module.ADDRESS_CACHE.try_new(tf[7], "EOA")
            insert = False
    if tf[9]: # to_addr
        to_addr_id = address_module.ADDRESS_CACHE.get(tf[9])
        if to_addr_id is not None:
            row = row[:6] + (to_addr_id,) + row[7:]
        else:
            address_module.ADDRESS_CACHE.try_new(tf[9], "Unknown")
            insert = False
    return row,insert