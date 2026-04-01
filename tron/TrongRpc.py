from dataclasses import dataclass
from datetime import datetime, timezone
import csv
import logging
import random
import sys
import os
import time
from pathlib import Path

import psycopg
from filelock import FileLock

from constants import TRON_CHAIN_ID
from scraper import NodeScraper
from model.Block import Block
from model.Transaction import Transaction

import base58
import hashlib

import grpc
from perf_timing import timed

sys.path.insert(0, os.path.abspath('./tron/generated'))
sys.path.insert(0, os.path.abspath('./'))
import api.api_pb2 as api
import api.api_pb2_grpc as tron_api
from tron.generated.core import Tron_pb2 as protocol
from tron.generated.core.contract import asset_issue_contract_pb2
from tron.generated.core.contract import balance_contract_pb2
from tron.generated.core.contract import smart_contract_pb2

VALID_TRANSACTION_TYPES = ['TransferContract', 'TransferAssetContract', 'CustomContract', 'TriggerSmartContract']

import queue
import threading
import time

transaction_queue = queue.Queue()
transfer_queue = queue.Queue()

@dataclass
class Address:
    id: int
    address: bytearray
    addr_t: str # 'EOA', 'Contract'

class Token:
    id: int
    address: Address
    asset_name: str
    token_t: str # 'TRX', 'TRC10', 'TRC20', 'TRC721'

@dataclass
class Transaction:
    id: int
    block: int
    result: bool
    ts: datetime
    transaction_t: str # 'TransferContract', 'TransferAssetContract', 'CustomContract', 'TriggerSmartContract'
    fee_limit: int | None
    fee: int | None
    energy_usage: int | None
    net_fee: int | None

    def __post_init__(self):
        if self.id is None:
            raise ValueError("Transaction ID cannot be None")
        if self.transaction_t not in VALID_TRANSACTION_TYPES:
            raise ValueError(f"Invalid transaction type: {self.transaction_t}")

    def as_params(self):
        return (self.id, self.block, self.result, self.ts, self.transaction_t, self.fee_limit, self.fee, self.energy_usage, self.net_fee)
    
    def __str__(self):
        return f"Transaction(id={self.id}, block={self.block}, result={self.result}, ts={self.ts}, transaction_t={self.transaction_t}, fee_limit={self.fee_limit}, fee={self.fee}, energy_usage={self.energy_usage}, net_fee={self.net_fee})"

@dataclass
class Transfer:
    transaction: Transaction
    index: int
    from_addr: Address
    to_addr: Address
    contract: Address
    reject: bool
    token: Token
    value: int
    transfer_t: str # 'Transaction', 'Internal Transaction', 'Log'

@dataclass
class Logs:
    transaction: Transaction
    index: int
    address: Address
    topic0: bytearray | None
    topic1: bytearray | None
    topic2: bytearray | None
    topic3: bytearray | None
    data: bytearray | None

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
        #with timed("db.inserts.transfers"):
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
        for block_num in range(start, end + 1):
            cur = self.conn.cursor()
            try:
                self.handle_block(block_num, cur, logger)
                self.conn.commit()
            except grpc.RpcError as e:
                self.conn.rollback()
                logger.exception("gRPC error while processing block %d", block_num)
                flag_for_rerun(list(range(block_num, end + 1)))
                raise e
            except Exception as e:
                self.conn.rollback()
                logger.exception("Failed to process block %d", block_num)
                flag_for_rerun(block_num)
        logger.info(f"Finished")

def transaction_consumer(
    output_csv: str = "TRANSACTIONS.csv",
    stop_event: threading.Event | None = None,
    flush_interval: int = 500,
    queue_timeout: int = 20,
):
    written = 0
    consumer_logger = logging.getLogger(__name__)

    try:
        with open(output_csv, "a", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)

            while True:
                try:
                    tx = transaction_queue.get(timeout=queue_timeout)
                except queue.Empty:
                    # Exit when shutdown is requested and nothing is left to consume.
                    if stop_event is not None and stop_event.is_set() and transaction_queue.empty():
                        break
                    # Backwards-compatible behavior: stop if queue stays empty.
                    if stop_event is None:
                        break
                    continue

                try:
                    # Allow graceful shutdown via sentinel value.
                    if tx is None:
                        break

                    writer.writerow(tx)
                    written += 1

                    if written % flush_interval == 0:
                        csv_file.flush()
                        os.fsync(csv_file.fileno())
                finally:
                    transaction_queue.task_done()
    except KeyboardInterrupt:
        consumer_logger.info("transaction_consumer interrupted; shutting down cleanly")
    finally:
        consumer_logger.info("transaction_consumer stopped after writing %d rows to %s", written, output_csv)
