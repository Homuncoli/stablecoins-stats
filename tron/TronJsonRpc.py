from json_rpc import JsonRpcScraper
from model.Block import TronBlock
from model.Transaction import Transaction, TronTransaction
from scrapper import NodeScrapper
from datetime import datetime, timezone

def tron_input_to_method_id(input: str) -> str:
    return input[:10]

def tron_timestamp_to_block_ts(ts: int) -> int:
    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)

class TronRpcScrapper(NodeScrapper, JsonRpcScraper):
    def __result_to_block(self, result, fullTrx: bool = False) -> TronBlock:
        return TronBlock(
            int(result["number"], 16),
            tron_timestamp_to_block_ts(int(result["timestamp"], 16)),
            [tx["hash"] for tx in result["transactions"]] if fullTrx else [tx for tx in result["transactions"]]
        )
    
    def __result_to_transaction(self, result) -> TronTransaction:
        return TronTransaction(
            int(result["blockNumber"], 16),
            int(result["transactionIndex"], 16),
            bytearray.fromhex(result["from"][2:]),
            bytearray.fromhex((result["to"] or "0x00")[2:]),
            "", # ToDo: Tron does not have method ids
            int(result["value"], 16),
            int(result["gasPrice"], 16),
            int(result["gas"], 16),
            0, # ToDo: Tron does not have effective gas price
            bool(0), # ToDo: Success unclear

        )

    def get_block_by_number(self, block_number: int, fullTrx: bool = False) -> TronBlock:
        return self.__result_to_block(self._make_request("eth_getBlockByNumber", [hex(block_number), fullTrx])["result"], fullTrx)
    
    def get_blocks_by_numbers(self, block_numbers: list[int], fullTrx: bool = False) -> list[TronBlock]:
        return [self.__result_to_block(block["result"], fullTrx) for block in self._make_batch_request("eth_getBlockByNumber", [ [hex(block_number), fullTrx] for block_number in block_numbers])]

    def get_transaction_receipt(self, tx_hash: str) -> Transaction:
        return self.__result_to_transaction(self._make_request("eth_getTransactionByHash", [tx_hash])["result"])
    
    def get_transaction_receipts(self, tx_hash: list[str]) -> list[Transaction]:
        return [self.__result_to_transaction(receipt["result"]) for receipt in self._make_batch_request("eth_getTransactionByHash", [ [hash] for hash in tx_hash])]
