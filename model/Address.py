from dataclasses import dataclass

import psycopg
from constants import TRON_CHAIN_ID
from datetime import datetime, timezone

@dataclass
class Address:
    chain: str
    addr: bytearray
    first: int

def insert_addresses(conn: psycopg.Connection, addresses: list[Address]) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO addresses (chain, addr, first) VALUES (%s, %s, %s) ON CONFLICT (chain, addr) DO UPDATE SET first = LEAST(addresses.first, EXCLUDED.first)",
            [(addr.chain, addr.addr, addr.first) for addr in addresses]
        )
    conn.commit()