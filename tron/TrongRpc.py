from dataclasses import dataclass
from datetime import datetime, timezone
import sys
import os
import time

import psycopg

from constants import TRON_CHAIN_ID
from scraper import NodeScraper
from model.Block import Block
from model.Transaction import Transaction

import base58
import hashlib

import grpc

sys.path.insert(0, os.path.abspath('./tron/generated'))
sys.path.insert(0, os.path.abspath('./'))
import api.api_pb2 as api
import api.api_pb2_grpc as tron_api
from tron.generated.core import Tron_pb2 as protocol
from tron.generated.core.contract import smart_contract_pb2

VALID_TRANSACTION_TYPES = ['TransferContract', 'TransferAssetContract', 'CustomContract', 'TriggerSmartContract']

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

def flag_for_rerun(block_num: int):
    print(f"Flagging block {block_num} for re-run")

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
                call_info.token_id if call_info.tokenId else None, 
                internal.caller_address, call_info.callValue if call_info.callValue else 0,
                owner_address, internal.transferTo_address, 
                bool(internal.rejected))

    def handle_block(self, block_num: int) -> Block:
        block = self.stub.GetBlockByNum(api.NumberMessage(num=block_num))
        infos = self.stub.GetTransactionInfoByBlockNum(api.NumberMessage(num=block_num))

        transactions: list[Transaction] = []
        addresses: list[Address] = []
        transfers: list = []
        for i, (trx, info) in enumerate(self.__pair_transactions_with_infos(block, infos)):
            if not info:
                print(f"Warning: No transaction info found for TxID {self.__calc_trxID(trx)} in block {block_num}")
                flag_for_rerun(block_num)
                continue
            
            owner_address = None
            match trx.raw_data.contract[0].type:
                case protocol.Transaction.Contract.ContractType.TriggerSmartContract:
                    msg = smart_contract_pb2.TriggerSmartContract()
                    trx.raw_data.contract[0].parameter.Unpack(msg)
                    owner_address = msg.owner_address
                    pass
                case protocol.Transaction.Contract.ContractType.TransferContract:
                    pass
                case protocol.Transaction.Contract.ContractType.TransferAssetContract:
                    pass
                case protocol.Transaction.Contract.ContractType.CustomContract:
                    pass
                case _:
                    # print(f"Warning: Unhandled transaction type {protocol.Transaction.Contract.ContractType.Name(trx.raw_data.contract[0].type)} in block {block_num}")
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

        try:
            cur = self.conn.cursor()
            cur.executemany("INSERT INTO transactions (id, block, result, ts, transaction_t, fee_limit, fee, energy_usage, net_fee) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING", [tx.as_params() for tx in transactions])
            cur.executemany("CALL insert_transfer(%s, %s, %s, %s, %s, %s, %s, %s, %s)", transfers)
            self.conn.commit()
        except Exception as e:
            print(f"Error inserting transactions for block {block_num}: {e}")
            flag_for_rerun(block_num)
            return None

        return block, infos
    
    def handle_range(self, start: int, end: int):
        print(f"Handling blocks {start} to {end}")
        for block_num in range(start, end + 1):
            self.handle_block(block_num)
        pass