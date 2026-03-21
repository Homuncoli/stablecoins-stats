from dataclasses import dataclass

import psycopg
from datetime import datetime

@dataclass
class Block:
    chain: str
    number: int
    ts: datetime

def insert_blocks(conn: psycopg.Connection, blocks: list[Block]) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO blocks (chain, number, ts) VALUES (%s, %s, %s) ON CONFLICT (chain, number) DO NOTHING;",
            [(block.chain, block.number, block.ts) for block in blocks]
        )
    conn.commit()