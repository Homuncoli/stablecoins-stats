from datetime import datetime, timezone
import sys
import os
import time

import grpc

sys.path.insert(0, os.path.abspath('./tron/generated'))
sys.path.insert(0, os.path.abspath('./'))

from constants import TRON_CHAIN_ID
from scrapper import NodeScrapper
from model.Block import Block
from model.Transaction import Transaction

import generated.api.api_pb2 as api
import generated.api.api_pb2_grpc as tron_api
from generated.core.Tron_pb2 import Block as gRpcBlock

def _tron_transaction_info_to_model(tx) -> Transaction:
    return Transaction(
        chain=TRON_CHAIN_ID,
        block_number=tx.blockNumber,
        tx_index=None,  # Tron does not have a concept of transaction index within a block
        from_id=None,
        to_id=None,
        method_id=None,
        value=None,
        gas_price=None,
        gas_used=None,
        effective_gas_price=0,
        success=True
    )

def _tron_block_to_model(block: gRpcBlock) -> Block:
    return Block(
        chain=TRON_CHAIN_ID,
        number=block.block_header.raw_data.number,
        ts=datetime.fromtimestamp(block.block_header.raw_data.timestamp / 1000, tz=timezone.utc),
    )

class TrongRpc(NodeScrapper):
    def __init__(self, connection_string: str):
        super().__init__()
        self.channel = grpc.insecure_channel(connection_string, options=[('grpc.max_send_message_length', 100 * 1024 * 1024), ('grpc.max_receive_message_length', 100 * 1024 * 1024)])
        self.stub = tron_api.WalletStub(self.channel)

    def get_now_block(self) -> int:
        response = self.stub.GetNowBlock(api.EmptyMessage())
        return response.block_header.raw_data.number
    
    def get_blocks_by_range(self, start: int, end: int) -> list[Block]:
        blocks = []
        for start_num in range(start, end + 1, 100):
            end_num = min(start_num + 99, end) + 1
            response = self.stub.GetBlockByLimitNext(api.BlockLimit(startNum=start_num, endNum=end_num))
            blocks.extend([_tron_block_to_model(block) for block in response.block])
        return blocks
    
    def get_transactions_by_blocks(self, block_numbers: list[int]) -> list[Transaction]:
        transactions = []
        for block_number in block_numbers:
            response = self.stub.GetTransactionInfoByBlockNum(api.NumberMessage(num=block_number))
            transactions.extend([_tron_transaction_info_to_model(tx) for tx in response.transactionInfo])
        return transactions

from concurrent.futures import ThreadPoolExecutor, as_completed

scraper = TrongRpc('10.9.0.3:50051')
n = 10000
BATCH_SIZE = 100
NUM_WORKERS = 10

def fetch_batch(start):
    end = start + BATCH_SIZE
    return scraper.get_blocks_by_range(start, end)

# Fetch blocks in parallel
ranges = range(10000000, 10000000 + n, BATCH_SIZE)
blocks = []

time_start = time.time()
with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
    futures = {executor.submit(fetch_batch, start): start for start in ranges}
    for future in as_completed(futures):
        blocks.extend(future.result())

blocks.sort(key=lambda b: b.number)  # as_completed doesn't preserve order

# Fetch transactions in parallel
def fetch_tx_batch(block_numbers):
    return scraper.get_transactions_by_blocks(block_numbers)

block_numbers = [b.number for b in blocks]
tx_batches = [block_numbers[i:i + BATCH_SIZE] for i in range(0, len(block_numbers), BATCH_SIZE)]
transactions = []

with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
    futures = [executor.submit(fetch_tx_batch, batch) for batch in tx_batches]
    for future in as_completed(futures):
        transactions.extend(future.result())

time_end = time.time()
print(f"Fetched {len(blocks)} blocks and {len(transactions)} transactions in {time_end - time_start:.2f} seconds => {len(blocks) / (time_end - time_start):.2f} blocks/s, {len(transactions) / (time_end - time_start):.2f} tx/s")