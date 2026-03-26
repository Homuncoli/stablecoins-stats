from abc import ABC, abstractmethod

class NodeScraper(ABC):
    @abstractmethod
    def get_block_number(self) -> int:
        pass

    @abstractmethod
    def process_blocks(self, from_block: int, to_block: int):
        pass
