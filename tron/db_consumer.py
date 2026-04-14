import logging
import random
import time
from queue import Empty
import threading

import psycopg
from psycopg_pool import ConnectionPool

from metrics import timed
from model.Tron import TRON_QUEUE, TX_COPY_TYPES, to_tx_binary_row

MERGE_LOCK = threading.Lock()
DB_SYNC_LOCK = threading.Lock()
TOTAL_CONSUMERS = 0

BUFFER_PROGRESS = []
COMMIT_PROGRESS = []
MERGE_PROGRESS = []

ADDRESS_LOOKUP = []
TOKEN_LOOKUP = []


def __load_address_lookup(cur) -> tuple[dict[bytes, int], int]:
    address_lookup: dict[bytes, int] = {}
    last_address_id = 0

    cur.execute("SELECT id, addr FROM addresses ORDER BY id")
    for address_id, address in cur:
        address_lookup[address] = address_id
        if address_id > last_address_id:
            last_address_id = address_id

    return address_lookup, last_address_id


def __refresh_address_lookup(cur, address_lookup: dict[bytes, int], last_address_id: int) -> int:
    cur.execute("SELECT id, addr FROM addresses WHERE id > %s ORDER BY id", (last_address_id,))
    for address_id, address in cur:
        address_lookup[address] = address_id
        if address_id > last_address_id:
            last_address_id = address_id

    return last_address_id


def __load_token_lookup(cur) -> tuple[dict[int, int], dict[int, int], int]:
    token_lookup_by_contract: dict[int, int] = {}
    token_lookup_by_asset: dict[int, int] = {}
    last_token_id = 0

    cur.execute("SELECT id, asset_id, contract_addr FROM tokens ORDER BY id")
    for token_id, asset_id, contract_addr in cur:
        if asset_id is not None:
            token_lookup_by_asset[asset_id] = token_id
        if contract_addr is not None:
            token_lookup_by_contract[contract_addr] = token_id
        if token_id > last_token_id:
            last_token_id = token_id

    return token_lookup_by_contract, token_lookup_by_asset, last_token_id


def __refresh_token_lookup(cur, token_lookup_by_contract: dict[int, int], token_lookup_by_asset: dict[int, int], last_token_id: int) -> int:
    cur.execute("SELECT id, asset_id, contract_addr FROM tokens WHERE id > %s ORDER BY id", (last_token_id,))
    for token_id, asset_id, contract_addr in cur:
        if asset_id is not None:
            token_lookup_by_asset[asset_id] = token_id
        if contract_addr is not None:
            token_lookup_by_contract[contract_addr] = token_id
        if token_id > last_token_id:
            last_token_id = token_id

    return last_token_id


def __copy_binary_rows(cur, buffer: list[tuple], target_table: str, columns: str, type_names: list[str]):
    if not buffer:
        return

    with timed("copy_binary", "db"):
        with cur.copy(f"COPY {target_table} ({columns}) FROM STDIN WITH (FORMAT BINARY)") as copy:
            copy.set_types(type_names)
            for row in buffer:
                copy.write_row(row)


def __collect_address_rows(tf_rows: list[tuple]) -> dict[bytes, str]:
    address_rows: dict[bytes, str] = {}

    for transfer in tf_rows:
        token_contract_addr = transfer[3]
        from_addr = transfer[7]
        from_type = transfer[8]
        to_addr = transfer[9]
        to_type = transfer[10]

        if from_addr is not None and from_addr not in address_rows:
            address_rows[from_addr] = from_type
        if to_addr is not None and to_addr not in address_rows:
            address_rows[to_addr] = to_type
        if token_contract_addr is not None and token_contract_addr not in address_rows:
            address_rows[token_contract_addr] = "Contract"

    return address_rows


def __insert_addresses(cur, address_rows: dict[bytes, str], address_lookup: dict[bytes, int], last_address_id: int) -> int:
    rows = [(address, address_t) for address, address_t in address_rows.items() if address not in address_lookup]
    if not rows:
        return last_address_id

    addresses = [row[0] for row in rows]
    address_types = [row[1] for row in rows]
    cur.execute(
        """
            INSERT INTO addresses (addr, addr_t)
            SELECT DISTINCT addr, addr_t::addr_type
            FROM unnest(%s::bytea[], %s::text[]) AS t(addr, addr_t)
            ON CONFLICT (addr) DO NOTHING
        """,
        (addresses, address_types),
    )

    last_address_id = __refresh_address_lookup(cur, address_lookup, last_address_id)

    return last_address_id


def __insert_tokens(
    cur,
    tf_rows: list[tuple],
    address_lookup: dict[bytes, int],
    token_lookup_by_contract: dict[int, int],
    token_lookup_by_asset: dict[int, int],
    last_token_id: int,
) -> int:
    rows: list[tuple[int | None, int | None, str]] = []
    seen: set[tuple[int | None, int | None]] = set()

    for transfer in tf_rows:
        token_asset_id = transfer[2]
        token_contract_addr = transfer[3]
        token_t = transfer[4]

        contract_id = address_lookup.get(token_contract_addr) if token_contract_addr is not None else None
        if contract_id is None and token_asset_id is None:
            continue
        if contract_id is not None and contract_id in token_lookup_by_contract:
            continue
        if token_asset_id is not None and token_asset_id in token_lookup_by_asset:
            continue

        key = (contract_id, token_asset_id)
        if key in seen:
            continue

        seen.add(key)
        rows.append((contract_id, token_asset_id, token_t))

    if not rows:
        return last_token_id

    contract_addrs = [row[0] for row in rows]
    asset_ids = [row[1] for row in rows]
    token_types = [row[2] for row in rows]
    cur.execute(
        """
            INSERT INTO tokens (contract_addr, asset_id, token_t)
            SELECT DISTINCT contract_addr, asset_id, token_t::token_type
            FROM unnest(%s::bigint[], %s::bigint[], %s::text[]) AS t(contract_addr, asset_id, token_t)
            ON CONFLICT DO NOTHING
        """,
        (contract_addrs, asset_ids, token_types),
    )

    last_token_id = __refresh_token_lookup(cur, token_lookup_by_contract, token_lookup_by_asset, last_token_id)

    return last_token_id


def __collect_transfer_rows(
    tf_rows: list[tuple],
    address_lookup: dict[bytes, int],
    token_lookup_by_contract: dict[int, int],
    token_lookup_by_asset: dict[int, int],
) -> list[tuple]:
    transfer_rows: list[tuple] = []

    for transfer in tf_rows:
        transaction = transfer[0]
        index = transfer[1]
        token_asset_id = transfer[2]
        token_contract_addr = transfer[3]
        value_lo = transfer[5]
        value_hi = transfer[6]
        from_addr = transfer[7]
        to_addr = transfer[9]
        success = transfer[11]

        token_id = 0
        if token_contract_addr is not None:
            contract_id = address_lookup[token_contract_addr]
            token_id = token_lookup_by_contract.get(contract_id, 0)
        if token_id == 0 and token_asset_id is not None:
            token_id = token_lookup_by_asset.get(token_asset_id, 0)

        transfer_rows.append(
            (
                transaction,
                index,
                token_id,
                value_lo,
                value_hi,
                address_lookup[from_addr],
                address_lookup[to_addr],
                success,
            )
        )

    return transfer_rows

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
                        token int,
                        value_lo bigint,
                        value_hi bigint,
                        from_addr bigint,
                        to_addr bigint,
                        success bool
                    )
                """)

def __merge_staging_table(cur, staging_table: str):
    with timed("merging", "db"):
        cur.execute(f"ANALYZE tx_{staging_table}")
        cur.execute(f"""
                        INSERT INTO transactions (id, result, ts, transaction_t, fee_limit, fee, energy_usage, net_fee)
                        SELECT id, result, ts, transaction_t::transaction_type, fee_limit, fee, energy_usage, net_fee
                        FROM tx_{staging_table}
                        ON CONFLICT (id) DO NOTHING
                    """)
        cur.execute(f"TRUNCATE TABLE tx_{staging_table}")


def __merge_transfer_staging_table(cur, staging_table: str):
    with timed("merging_transfers", "db"):
        cur.execute(f"ANALYZE tf_{staging_table}")
        cur.execute(f"""
                        INSERT INTO transfers (transaction, index, token, value_lo, value_hi, from_addr, to_addr, success)
                        SELECT transaction, index, token, value_lo, value_hi, from_addr, to_addr, success
                        FROM tf_{staging_table}
                        ON CONFLICT (transaction, index) DO NOTHING
                    """)
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
                address_lookup, last_address_id = __load_address_lookup(cur)
                token_lookup_by_contract, token_lookup_by_asset, last_token_id = __load_token_lookup(cur)
                __create_staging_table(cur, staging_table)

                uncommited_tx = 0

                def sync_staging(force: bool) -> bool:
                        nonlocal uncommited_tx, tx_buffer_rows, last_address_id, last_token_id
                        should_flush_new_rows = tx_buffer_rows > 0 and (force or tx_buffer_rows >= commit_size)
    
                        if not should_flush_new_rows and uncommited_tx == 0:
                            return False
    
                        tx_rows = tx_buffer.copy() if should_flush_new_rows else []
                        tf_rows = tf_buffer.copy() if should_flush_new_rows else []
    
                        if should_flush_new_rows:
                            logger.debug("copying binary buffers to staging after processing %d transactions", tx_buffer_rows)
                            __copy_binary_rows(cur, tx_rows, f"tx_{staging_table}", "id, result, ts, transaction_t, fee_limit, fee, energy_usage, net_fee", TX_COPY_TYPES)
                            uncommited_tx += tx_buffer_rows
                            COMMIT_PROGRESS[consumer_id] += tx_buffer_rows
    
                        if uncommited_tx == 0:
                            return False
    
                        if not DB_SYNC_LOCK.acquire():
                            return False
    
                        try:
                            if uncommited_tx > 0:
                                logger.info("merging staging table after processing %d transactions", uncommited_tx)
                                __merge_staging_table_with_retry(cur, staging_table, logger)
    
                                if tf_rows:
                                    last_address_id = __refresh_address_lookup(cur, address_lookup, last_address_id)
                                    last_token_id = __refresh_token_lookup(cur, token_lookup_by_contract, token_lookup_by_asset, last_token_id)
    
                                    address_rows = __collect_address_rows(tf_rows)
                                    last_address_id = __insert_addresses(cur, address_rows, address_lookup, last_address_id)
                                    last_token_id = __insert_tokens(
                                        cur,
                                        tf_rows,
                                        address_lookup,
                                        token_lookup_by_contract,
                                        token_lookup_by_asset,
                                        last_token_id,
                                    )

                                    ADDRESS_LOOKUP[consumer_id] = len(address_lookup)
                                    TOKEN_LOOKUP[consumer_id] = len(token_lookup_by_contract) + len(token_lookup_by_asset)
                                    transfer_rows = __collect_transfer_rows(
                                        tf_rows,
                                        address_lookup,
                                        token_lookup_by_contract,
                                        token_lookup_by_asset,
                                    )
                                    __copy_binary_rows(
                                        cur,
                                        transfer_rows,
                                        f"tf_{staging_table}",
                                        "transaction, index, token, value_lo, value_hi, from_addr, to_addr, success",
                                        ["int8", "int2", "int4", "int8", "int8", "int8", "int8", "bool"],
                                    )
                                    __merge_transfer_staging_table(cur, staging_table)
    
                                with timed("commit", "db"):
                                    conn.commit()
    
                                if should_flush_new_rows:
                                    tx_buffer.clear()
                                    tf_buffer.clear()
                                    tx_buffer_rows = 0
    
                                MERGE_PROGRESS[consumer_id] += uncommited_tx
                                uncommited_tx = 0
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
                                    tf_buffer.append(tf)
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