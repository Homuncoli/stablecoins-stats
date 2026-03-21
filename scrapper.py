from abc import ABC, abstractmethod

from model.Transaction import Transaction
from model.Block import Block


class NodeScrapper(ABC):
    ## Gets the current block number of the chain
    @abstractmethod
    def get_now_block(self) -> int:
        pass

    @abstractmethod
    def get_blocks_by_range(self, start: int, end: int) -> list[Block]:
        pass

    @abstractmethod
    def get_transactions_by_blocks(self, block_numbers: list[int]) -> list[Transaction]:
        pass