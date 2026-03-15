from dataclasses import dataclass

import psycopg
from constants import TRON_CHAIN_ID
from datetime import datetime, timezone

@dataclass
class Address:
    chain: str
    addr: bytearray

def insert_addresses(conn: psycopg.Connection, addresses: list[Address]) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO addresses (chain, addr) VALUES (%s, %s) ON CONFLICT (chain, addr) DO NOTHING;",
            [(chain, addr) for chain, addr in addresses]
        )
    conn.commit()