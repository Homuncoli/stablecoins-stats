from abc import ABC, abstractmethod

from model.Transaction import Transaction
from model.Block import Block


class NodeScrapper(ABC):
    @abstractmethod
    def get_block_number(self) -> int:
        pass

    @abstractmethod
    def get_block_by_number(self, block_number: int, fullTrx: bool = False) -> Block:
        pass

    @abstractmethod
    def get_blocks_by_numbers(self, block_numbers: list[int], fullTrx: bool = False) -> list[Block]:
        pass

    @abstractmethod
    def get_transaction_receipt(self, tx_hash: str) -> Transaction:
        pass

    @abstractmethod
    def get_transaction_receipts(self, tx_hash: list[str]) -> list[Transaction]:
        pass

