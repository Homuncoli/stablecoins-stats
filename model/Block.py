from dataclasses import dataclass

import psycopg
from constants import TRON_CHAIN_ID
from datetime import datetime, timezone

@dataclass
class Block:
    chain: str
    number: int
    ts: datetime
 
class TronBlock(Block):
    transaction_hashes: list[str]

    def __init__(self, number: int, ts: int, transaction_hashes: list[str]):
        super().__init__(TRON_CHAIN_ID, number, ts)
        self.transaction_hashes = transaction_hashes

def insert_blocks(conn: psycopg.Connection, blocks: list[Block]) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO blocks (chain, number, ts) VALUES (%s, %s, %s) ON CONFLICT (chain, number) DO NOTHING;",
            [(block.chain, block.number, block.ts) for block in blocks]
        )
    conn.commit()