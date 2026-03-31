from abc import ABC, abstractmethod

from model.Transaction import Transaction
from model.Block import Block

class NodeScraper(ABC):
    def __init__(self):
        pass

    ## Gets the current block number of the chain
    @abstractmethod
    def get_now_block(self) -> int:
        pass

    @abstractmethod
    def handle_range(self, start: int, end: int):
        pass