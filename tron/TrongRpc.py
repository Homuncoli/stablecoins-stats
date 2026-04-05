from dataclasses import dataclass
from datetime import datetime, timezone
import logging
import random
import sys
import os
import time
from pathlib import Path
import re

import psycopg
from filelock import FileLock

from constants import TRON_CHAIN_ID
from scraper import NodeScraper

import base58
import hashlib

import grpc
from metrics import timed

sys.path.insert(0, os.path.abspath('./tron/generated'))
sys.path.insert(0, os.path.abspath('./'))
import api.api_pb2 as api
import api.api_pb2_grpc as tron_api
from tron.generated.core import Tron_pb2 as protocol
from tron.generated.core.contract import asset_issue_contract_pb2
from tron.generated.core.contract import balance_contract_pb2
from tron.generated.core.contract import smart_contract_pb2

import queue
import threading
import time

transaction_queue = queue.Queue()
transfer_queue = queue.Queue()


class BlockDataError(Exception):
    """Raised when a single block contains malformed or incomplete data."""


class ChunkRetryError(Exception):
    """Raised when a transient RPC issue requires reprocessing the remaining chunk."""

    def __init__(self, failed_block: int, chunk_end: int, cause: Exception):
        self.failed_block = failed_block
        self.chunk_end = chunk_end
        self.cause = cause
        super().__init__(
            f"Transient RPC failure at block {failed_block}; retry chunk through {chunk_end}: {cause}"
        )

_RERUN_QUEUE_FILE = Path("./.rerun_queue")
_RERUN_QUEUE_LOCK = FileLock(str(_RERUN_QUEUE_FILE) + ".lock", timeout=10)

def flag_for_rerun(block_nums: list[int] | int):
    try:
        with _RERUN_QUEUE_LOCK:
            with open(_RERUN_QUEUE_FILE, 'a') as f:
                if isinstance(block_nums, list):
                    for block_num in block_nums:
                        f.write(f"{block_num},")
                    f.write("\n")
                else:
                    f.write(f"{block_nums}\n")
                f.flush()
                os.fsync(f.fileno())
    except Exception as e:
        logging.error(f"Failed to write block {block_num} to rerun queue: {e}")

class TronGRpcScraper(NodeScraper):
    def __init__(self, stub: tron_api.WalletStub, conn: psycopg.Connection):
        super().__init__()
        self.stub = stub
        self.conn = conn

    def get_now_block(self) -> int:
        response = self.stub.GetNowBlock(api.EmptyMessage())
        return response.block_header.raw_data.number

    def __calc_trxID(self, trx) -> str:
        raw_bytes = trx.raw_data.SerializeToString()
        return hashlib.sha256(raw_bytes).hexdigest()

    def __pair_transactions_with_infos(self, block, infos):
        info_by_txid = { info.id.hex(): info for info in infos.transactionInfo }
        return [ (trx, info_by_txid.get(self.__calc_trxID(trx))) for trx in block.transactions ]

    def __extract_len_delimited_field(self, payload: bytes, field_number: int) -> bytes | None:
        i = 0
        n = len(payload)

        while i < n:
            key = 0
            shift = 0
            while i < n:
                b = payload[i]
                i += 1
                key |= (b & 0x7F) << shift
                if (b & 0x80) == 0:
                    break
                shift += 7
            else:
                return None

            wire_type = key & 0x07
            number = key >> 3

            if wire_type == 0:
                while i < n and (payload[i] & 0x80):
                    i += 1
                i += 1
            elif wire_type == 1:
                i += 8
            elif wire_type == 2:
                length = 0
                shift = 0
                while i < n:
                    b = payload[i]
                    i += 1
                    length |= (b & 0x7F) << shift
                    if (b & 0x80) == 0:
                        break
                    shift += 7
                else:
                    return None

                if i + length > n:
                    return None

                value = payload[i:i + length]
                i += length
                if number == field_number:
                    return value
            elif wire_type == 5:
                i += 4
            else:
                return None

            if i > n:
                return None

        return None
    
    def __trx_to_model(self, block, trx, info, i) -> Transaction:
        SCALER = 1_000
        return Transaction(
            id=block.block_header.raw_data.number * SCALER + i % SCALER,
            block=block.block_header.raw_data.number,
            result=bool(info.receipt.result),
            ts=datetime.fromtimestamp(block.block_header.raw_data.timestamp / 1000, tz=timezone.utc),
            transaction_t=protocol.Transaction.Contract.ContractType.Name(trx.raw_data.contract[0].type),
            fee_limit=trx.raw_data.fee_limit,
            fee=info.fee if info and info.fee is not None else None,
            # contract_address=info.contract_address if info and info.contract_address is not None else None,
            energy_usage=info.receipt.energy_usage_total if info and info.receipt.energy_usage_total is not None else None,
            net_fee=info.receipt.net_fee if info and info.receipt.net_fee is not None else None,
        )
    
    def __call_info_to_model(self, t_Id, block, trx, owner_address, internal, call_info, i):
        return (t_Id, i, "Internal Transaction", 
                call_info.tokenId if call_info.tokenId else None, 
                None if call_info.tokenId else internal.caller_address, call_info.callValue if call_info.callValue else 0,
                owner_address, internal.transferTo_address, 
                bool(internal.rejected))

    def handle_block(self, block_num: int, cur: psycopg.Cursor, logger: logging.Logger) -> Block:
        with timed("tron.grpc.GRPC"):
            block = self.stub.GetBlockByNum(api.NumberMessage(num=block_num))
            infos = self.stub.GetTransactionInfoByBlockNum(api.NumberMessage(num=block_num))

        with timed("processing"):
            transactions: list[Transaction] = []
            addresses: list[Address] = []
            transfers: list = []
            paired_transactions = self.__pair_transactions_with_infos(block, infos)

            for i, (trx, info) in enumerate(paired_transactions):
                if not info:
                    logger.warning("No transaction info found for TxID %s in block %d", self.__calc_trxID(trx), block_num)
                    flag_for_rerun(block_num)
                    continue
                
                owner_address = None
                match trx.raw_data.contract[0].type:
                    case protocol.Transaction.Contract.ContractType.TriggerSmartContract:
                        msg = smart_contract_pb2.TriggerSmartContract()
                        trx.raw_data.contract[0].parameter.Unpack(msg)
                        owner_address = msg.owner_address
                    case protocol.Transaction.Contract.ContractType.TransferContract:
                        msg = balance_contract_pb2.TransferContract()
                        trx.raw_data.contract[0].parameter.Unpack(msg)
                        owner_address = msg.owner_address
                    case protocol.Transaction.Contract.ContractType.TransferAssetContract:
                        msg = asset_issue_contract_pb2.TransferAssetContract()
                        trx.raw_data.contract[0].parameter.Unpack(msg)
                        owner_address = msg.owner_address
                    case protocol.Transaction.Contract.ContractType.CustomContract:
                        owner_address = self.__extract_len_delimited_field(
                            trx.raw_data.contract[0].parameter.value,
                            1,
                        )
                    case _:
                        # logger.warning("Unhandled transaction type %s in block %d", protocol.Transaction.Contract.ContractType.Name(trx.raw_data.contract[0].type), block_num)
                        continue

                transaction = self.__trx_to_model(block, trx, info, i)
                transactions.append(transaction)

                i = 0
                if info.internal_transactions:
                    for internal_trx in info.internal_transactions:
                        for call_info in internal_trx.callValueInfo:
                            transfers.append(self.__call_info_to_model(transaction.id, block, trx, owner_address, internal_trx, call_info, i))
                            i += 1

                if info.log:
                    pass

        #with timed("db.inserts.transactions"):
        #    cur.executemany("INSERT INTO transactions (id, block, result, ts, transaction_t, fee_limit, fee, energy_usage, net_fee) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING", [tx.as_params() for tx in transactions])
        with timed("db.inserts"):
        #    cur.executemany("CALL insert_transfer(%s, %s, %s, %s, %s, %s, %s, %s, %s)", transfers)
        #with timed("file.write"):
        #    with open("TRANSACTIONS.csv", "a") as f:
        #        for tx in transactions:
        #            f.write(f"{tx.id},{tx.block},{tx.result},{tx.ts.isoformat()},{tx.transaction_t},{tx.fee_limit},{tx.fee},{tx.energy_usage},{tx.net_fee}\n")
        #    with open("IMPORT.csv", "a") as f:
        #        for transfer in transfers:
        #            f.write(f"{transfer[0]},{transfer[1]},{transfer[2]},{transfer[3].hex() if transfer[3] else None},{transfer[4].hex() if transfer[4] else None},{transfer[5] if transfer[5] else None},{transfer[6].hex() if transfer[6] else None},{transfer[7].hex() if transfer[7] else None},{transfer[8]}\n")
            for tx in transactions:
                transaction_queue.put(tx.as_params())
            for transfer in transfers:
                transfer_queue.put(transfer)

        logger.debug(f"Block {block_num}: {len(transactions)} transactions, {len(transfers)} transfers")

        return block, infos
    
    def handle_range(self, start: int, end: int, logger: logging.Logger):
        transient_rpc_codes = {
            grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.DEADLINE_EXCEEDED,
            grpc.StatusCode.CANCELLED,
            grpc.StatusCode.RESOURCE_EXHAUSTED,
        }

        for block_num in range(start, end + 1):
            cur = self.conn.cursor()
            try:
                self.handle_block(block_num, cur, logger)
                self.conn.commit()
            except grpc.RpcError as e:
                self.conn.rollback()
                status = e.code() if hasattr(e, "code") else None
                if status in transient_rpc_codes:
                    logger.warning(
                        "Transient gRPC error while processing block %d (%s); scheduling chunk retry",
                        block_num,
                        status,
                    )
                    flag_for_rerun(list(range(block_num, end + 1)))
                    raise ChunkRetryError(block_num, end, e) from e

                logger.exception("Non-transient gRPC error while processing block %d", block_num)
                flag_for_rerun(block_num)
            except (KeyError, ValueError, TypeError, AttributeError, IndexError) as e:
                self.conn.rollback()
                logger.warning("Malformed/incomplete block %d: %s", block_num, e)
                flag_for_rerun(block_num)
            except Exception as e:
                self.conn.rollback()
                logger.exception("Unexpected block-level error for block %d", block_num)
                flag_for_rerun(block_num)
        logger.info(f"Finished")

def transaction_consumer(
    pg_dsn: str,
    consumer_id: int = 0,
    stop_event: threading.Event | None = None,
    batch_size: int = 50_000,
    merge_batch_size: int = 500_000,
    merge_on_shutdown_only: bool = False,
    merge_strategy: str = "on_conflict",
    stage_commit_batch_size: int = 2_000_000,
    queue_timeout: int = 20,
    sync_commit: bool = False,
    metrics_interval_s: int = 30,
):
    written = 0
    consumer_logger = logging.getLogger(__name__)
    buffered_rows: list[tuple] = []
    staged_rows = 0
    staged_since_commit = 0
    staging_table = f"transactions_stage_{consumer_id}_{os.getpid()}"
    staging_table = re.sub(r"[^a-zA-Z0-9_]", "_", staging_table)
    metrics_interval_s = max(1, metrics_interval_s)
    metrics_last_t = time.monotonic()
    metrics_last_written = 0

    def log_metrics(force: bool = False):
        nonlocal metrics_last_t, metrics_last_written
        now = time.monotonic()
        if (not force) and (now - metrics_last_t < metrics_interval_s):
            return
        interval = max(now - metrics_last_t, 1e-6)
        delta_written = written - metrics_last_written
        consumer_logger.info(
            "tx_consumer[%d] written=%d (+%d, %.1f/s), staged=%d, buffered=%d, qsize=%d",
            consumer_id,
            written,
            delta_written,
            delta_written / interval,
            staged_rows,
            len(buffered_rows),
            transaction_queue.qsize(),
        )
        metrics_last_t = now
        metrics_last_written = written

    def copy_into_staging(cur: psycopg.Cursor) -> int:
        if not buffered_rows:
            return 0

        with cur.copy(
            f"""
            COPY {staging_table}
            (id, block, result, ts, transaction_t, fee_limit, fee, energy_usage, net_fee)
            FROM STDIN
            """
        ) as copy:
            for row in buffered_rows:
                copy.write_row(row)

        copied = len(buffered_rows)
        buffered_rows.clear()
        return copied

    def merge_staging(cur: psycopg.Cursor) -> int:
        nonlocal staged_rows
        if staged_rows == 0:
            return 0

        if merge_strategy == "anti_join":
            cur.execute(
                f"""
                INSERT INTO transactions (id, block, result, ts, transaction_t, fee_limit, fee, energy_usage, net_fee)
                SELECT s.id, s.block, s.result, s.ts, s.transaction_t, s.fee_limit, s.fee, s.energy_usage, s.net_fee
                FROM {staging_table} s
                LEFT JOIN transactions t ON t.id = s.id
                WHERE t.id IS NULL
                ON CONFLICT (id) DO NOTHING
                """
            )
        else:
            cur.execute(
                f"""
                INSERT INTO transactions (id, block, result, ts, transaction_t, fee_limit, fee, energy_usage, net_fee)
                SELECT id, block, result, ts, transaction_t, fee_limit, fee, energy_usage, net_fee
                FROM {staging_table}
                ON CONFLICT (id) DO NOTHING
                """
            )
        cur.execute(f"TRUNCATE {staging_table}")

        inserted = staged_rows
        staged_rows = 0
        return inserted

    try:
        with psycopg.connect(pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    DROP TABLE IF EXISTS {staging_table};
                    CREATE UNLOGGED TABLE {staging_table} (
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
                    """
                )
                if merge_strategy == "anti_join":
                    cur.execute(f"CREATE INDEX {staging_table}_id_idx ON {staging_table}(id)")
                conn.commit()

                if not sync_commit:
                    cur.execute("SET synchronous_commit TO OFF")

                while True:
                    try:
                        tx = transaction_queue.get(timeout=queue_timeout)
                    except queue.Empty:
                        if stop_event is not None and stop_event.is_set() and transaction_queue.empty():
                            copied = copy_into_staging(cur)
                            staged_rows += copied
                            staged_since_commit += copied
                            inserted = merge_staging(cur)
                            if inserted:
                                written += inserted
                            conn.commit()
                            log_metrics(force=True)
                            break
                        if stop_event is None:
                            copied = copy_into_staging(cur)
                            staged_rows += copied
                            staged_since_commit += copied
                            inserted = merge_staging(cur)
                            if inserted:
                                written += inserted
                            conn.commit()
                            log_metrics(force=True)
                            break

                        copied = copy_into_staging(cur)
                        staged_rows += copied
                        staged_since_commit += copied
                        if staged_since_commit >= stage_commit_batch_size:
                            conn.commit()
                            staged_since_commit = 0
                        if (not merge_on_shutdown_only) and staged_rows >= merge_batch_size:
                            inserted = merge_staging(cur)
                            if inserted:
                                written += inserted
                            conn.commit()
                            staged_since_commit = 0
                        log_metrics()
                        continue

                    try:
                        # Allow graceful shutdown via sentinel value.
                        if tx is None:
                            copied = copy_into_staging(cur)
                            staged_rows += copied
                            staged_since_commit += copied
                            inserted = merge_staging(cur)
                            if inserted:
                                written += inserted
                            conn.commit()
                            log_metrics(force=True)
                            break

                        buffered_rows.append(tx)

                        if len(buffered_rows) >= batch_size:
                            copied = copy_into_staging(cur)
                            staged_rows += copied
                            staged_since_commit += copied
                            if staged_since_commit >= stage_commit_batch_size:
                                conn.commit()
                                staged_since_commit = 0
                            if (not merge_on_shutdown_only) and staged_rows >= merge_batch_size:
                                inserted = merge_staging(cur)
                                written += inserted
                                conn.commit()
                                staged_since_commit = 0
                        log_metrics()
                    finally:
                        transaction_queue.task_done()
    except KeyboardInterrupt:
        consumer_logger.info("transaction_consumer interrupted; shutting down cleanly")
    except Exception:
        consumer_logger.exception("transaction_consumer failed")
    finally:
        try:
            with psycopg.connect(pg_dsn) as cleanup_conn:
                with cleanup_conn.cursor() as cleanup_cur:
                    cleanup_cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
                cleanup_conn.commit()
        except Exception:
            consumer_logger.debug("failed to drop staging table %s", staging_table, exc_info=True)
        log_metrics(force=True)
        consumer_logger.info("transaction_consumer stopped after writing %d rows to postgres", written)


def transfer_consumer(
    pg_dsn: str,
    consumer_id: int = 0,
    stop_event: threading.Event | None = None,
    batch_size: int = 100_000,
    merge_batch_size: int = 1_000_000,
    merge_on_shutdown_only: bool = False,
    stage_commit_batch_size: int = 2_000_000,
    queue_timeout: int = 20,
    sync_commit: bool = False,
    metrics_interval_s: int = 30,
):
    written = 0
    consumer_logger = logging.getLogger(__name__)
    buffered_rows: list[tuple] = []
    staged_rows = 0
    staged_since_commit = 0
    staging_table = f"transfers_stage_{consumer_id}_{os.getpid()}"
    staging_table = re.sub(r"[^a-zA-Z0-9_]", "_", staging_table)
    metrics_interval_s = max(1, metrics_interval_s)
    metrics_last_t = time.monotonic()
    metrics_last_written = 0

    def log_metrics(force: bool = False):
        nonlocal metrics_last_t, metrics_last_written
        now = time.monotonic()
        if (not force) and (now - metrics_last_t < metrics_interval_s):
            return
        interval = max(now - metrics_last_t, 1e-6)
        delta_written = written - metrics_last_written
        consumer_logger.info(
            "transfer_consumer[%d] written=%d (+%d, %.1f/s), staged=%d, buffered=%d, qsize=%d",
            consumer_id,
            written,
            delta_written,
            delta_written / interval,
            staged_rows,
            len(buffered_rows),
            transfer_queue.qsize(),
        )
        metrics_last_t = now
        metrics_last_written = written

    def copy_into_staging(cur: psycopg.Cursor) -> int:
        if not buffered_rows:
            return 0

        with cur.copy(
            f"""
            COPY {staging_table}
            (tx_id, transfer_index, transfer_t, asset_id, contract_addr, value, from_addr, to_addr, rejected)
            FROM STDIN
            """
        ) as copy:
            for row in buffered_rows:
                copy.write_row(row)

        copied = len(buffered_rows)
        buffered_rows.clear()
        return copied

    def merge_staging(cur: psycopg.Cursor) -> tuple[int, int]:
        nonlocal staged_rows
        if staged_rows == 0:
            return 0, 0

        cur.execute(
            f"""
            INSERT INTO addresses (addr, addr_t)
            SELECT DISTINCT s.from_addr, 'EOA'::addr_type
            FROM {staging_table} s
            WHERE s.from_addr IS NOT NULL
            ON CONFLICT (addr) DO NOTHING
            """
        )
        cur.execute(
            f"""
            INSERT INTO addresses (addr, addr_t)
            SELECT DISTINCT s.to_addr, 'EOA'::addr_type
            FROM {staging_table} s
            WHERE s.to_addr IS NOT NULL
            ON CONFLICT (addr) DO NOTHING
            """
        )
        cur.execute(
            f"""
            INSERT INTO addresses (addr, addr_t)
            SELECT DISTINCT s.contract_addr, 'Contract'::addr_type
            FROM {staging_table} s
            WHERE s.contract_addr IS NOT NULL
            ON CONFLICT (addr) DO NOTHING
            """
        )

        cur.execute(
            f"""
            INSERT INTO token (asset_name, contract_addr, token_t)
            SELECT DISTINCT s.asset_id, NULL::integer, 'TRC10'::token_type
            FROM {staging_table} s
            WHERE s.asset_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM token t
                  WHERE t.asset_name = s.asset_id
              )
            """
        )
        cur.execute(
            f"""
            INSERT INTO token (asset_name, contract_addr, token_t)
            SELECT DISTINCT NULL::bytea, a.id, 'TRC20'::token_type
            FROM {staging_table} s
            JOIN addresses a ON a.addr = s.contract_addr
            WHERE s.contract_addr IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM token t
                  WHERE t.contract_addr = a.id
              )
            """
        )

        cur.execute(
            f"""
            WITH resolved AS (
                SELECT
                    s.tx_id,
                    s.transfer_index,
                    s.transfer_t,
                    COALESCE(t20.id, t10.id, 0) AS token_id,
                    s.value,
                    to_a.id AS to_addr_id,
                    from_a.id AS from_addr_id,
                    s.rejected
                FROM {staging_table} s
                JOIN transactions tx ON tx.id = s.tx_id
                JOIN addresses from_a ON from_a.addr = s.from_addr
                JOIN addresses to_a ON to_a.addr = s.to_addr
                LEFT JOIN addresses c_a ON c_a.addr = s.contract_addr
                LEFT JOIN token t20
                    ON s.contract_addr IS NOT NULL
                    AND t20.contract_addr = c_a.id
                LEFT JOIN token t10
                    ON s.contract_addr IS NULL
                    AND s.asset_id IS NOT NULL
                    AND t10.asset_name = s.asset_id
            )
                    INSERT INTO transfers ("transaction", index, transfer_t, token, value, to_addr, from_addr, rejected)
            SELECT
                r.tx_id,
                r.transfer_index,
                r.transfer_t,
                r.token_id,
                r.value,
                r.to_addr_id,
                r.from_addr_id,
                r.rejected
            FROM resolved r
            ON CONFLICT DO NOTHING
            """
        )
        inserted = cur.rowcount

        cur.execute(
            f"""
            DELETE FROM {staging_table} s
            USING transactions tx
            WHERE tx.id = s.tx_id
            """
        )
        processed = cur.rowcount
        staged_rows -= processed
        return inserted, processed

    try:
        with psycopg.connect(pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    DROP TABLE IF EXISTS {staging_table};
                    CREATE UNLOGGED TABLE {staging_table} (
                        tx_id bigint,
                        transfer_index smallint,
                        transfer_t transfer_type,
                        asset_id bytea,
                        contract_addr bytea,
                        value bigint,
                        from_addr bytea,
                        to_addr bytea,
                        rejected bool
                    )
                    """
                )
                cur.execute(f"CREATE INDEX {staging_table}_tx_id_idx ON {staging_table}(tx_id)")
                conn.commit()

                if not sync_commit:
                    cur.execute("SET synchronous_commit TO OFF")

                while True:
                    try:
                        transfer = transfer_queue.get(timeout=queue_timeout)
                    except queue.Empty:
                        if stop_event is not None and stop_event.is_set() and transfer_queue.empty():
                            copied = copy_into_staging(cur)
                            staged_rows += copied
                            staged_since_commit += copied
                            inserted, _ = merge_staging(cur)
                            if inserted:
                                written += inserted
                            conn.commit()
                            log_metrics(force=True)
                            break
                        if stop_event is None:
                            copied = copy_into_staging(cur)
                            staged_rows += copied
                            staged_since_commit += copied
                            inserted, _ = merge_staging(cur)
                            if inserted:
                                written += inserted
                            conn.commit()
                            log_metrics(force=True)
                            break

                        copied = copy_into_staging(cur)
                        staged_rows += copied
                        staged_since_commit += copied
                        if staged_since_commit >= stage_commit_batch_size:
                            conn.commit()
                            staged_since_commit = 0
                        if (not merge_on_shutdown_only) and staged_rows >= merge_batch_size:
                            inserted, _ = merge_staging(cur)
                            if inserted:
                                written += inserted
                            conn.commit()
                            staged_since_commit = 0
                        log_metrics()
                        continue

                    try:
                        if transfer is None:
                            copied = copy_into_staging(cur)
                            staged_rows += copied
                            staged_since_commit += copied
                            inserted, _ = merge_staging(cur)
                            if inserted:
                                written += inserted
                            conn.commit()
                            log_metrics(force=True)
                            break

                        buffered_rows.append(transfer)

                        if len(buffered_rows) >= batch_size:
                            copied = copy_into_staging(cur)
                            staged_rows += copied
                            staged_since_commit += copied
                            if staged_since_commit >= stage_commit_batch_size:
                                conn.commit()
                                staged_since_commit = 0
                            if (not merge_on_shutdown_only) and staged_rows >= merge_batch_size:
                                inserted, _ = merge_staging(cur)
                                written += inserted
                                conn.commit()
                                staged_since_commit = 0
                        log_metrics()
                    finally:
                        transfer_queue.task_done()
    except KeyboardInterrupt:
        consumer_logger.info("transfer_consumer interrupted; shutting down cleanly")
    except Exception:
        consumer_logger.exception("transfer_consumer failed")
    finally:
        try:
            with psycopg.connect(pg_dsn) as cleanup_conn:
                with cleanup_conn.cursor() as cleanup_cur:
                    cleanup_cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
                cleanup_conn.commit()
        except Exception:
            consumer_logger.debug("failed to drop staging table %s", staging_table, exc_info=True)
        log_metrics(force=True)
        consumer_logger.info("transfer_consumer stopped after writing %d rows to postgres", written)
