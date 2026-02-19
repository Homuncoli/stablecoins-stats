#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
import time
import shutil
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Set, Tuple

from dotenv import load_dotenv
from web3 import Web3
from web3.types import LogReceipt

import psycopg
from psycopg.rows import dict_row

load_dotenv()

ZERO_ADDR = "0x0000000000000000000000000000000000000000"
ZERO_ADDR_BYTES20 = bytes.fromhex(ZERO_ADDR[2:])

TRANSFER_TOPIC0 = Web3.keccak(text="Transfer(address,address,uint256)").hex()

BLACKLIST_SIGS = [
    ("BLACKLIST",   "Blacklisted(address)"),
    ("UNBLACKLIST", "UnBlacklisted(address)"),
    ("BLACKLIST",   "AddedBlackList(address)"),
    ("UNBLACKLIST", "RemovedBlackList(address)"),
    ("DESTROYEDBLACKFUNDS", "DestroyedBlackFunds(address,uint256)"),
]
EVENT_TYPE = {"BLACKLIST": 1, "UNBLACKLIST": 2}
BLACKLIST_TOPIC0_TO_TYPE: Dict[str, int] = {
    Web3.keccak(text=sig).hex(): EVENT_TYPE[name] for name, sig in BLACKLIST_SIGS
}


@dataclass(frozen=True)
class TokenConfig:
    symbol: str
    contract: str  # checksum address string
    decimals: int


def to_bytes20(addr: str) -> bytes:
    addr = Web3.to_checksum_address(addr)
    return bytes.fromhex(addr[2:])

def to_bytes32(hexstr: str) -> bytes:
    hs = hexstr[2:] if hexstr.startswith("0x") else hexstr
    return bytes.fromhex(hs.zfill(64))

def topic_to_addr_bytes20(topic_hex: str) -> Optional[bytes]:
    if not topic_hex or topic_hex == "0x":
        return None
    b = to_bytes32(topic_hex)
    return b[12:]  # last 20 bytes

def calldata_selector_bytes4(input_hex: str) -> Optional[bytes]:
    if not input_hex or input_hex == "0x":
        return None
    hs = input_hex[2:] if input_hex.startswith("0x") else input_hex
    if len(hs) < 8:
        return None
    return bytes.fromhex(hs[:8])


def ddl(no_constraints: bool) -> str:
    # Note: Ethereum addresses are 20 bytes (we store 20 bytes in BYTEA)
    # In no_constraints mode, we avoid PK/UNIQUE (and their indexes) on big tables for faster ingest.
    if no_constraints:
        return """
        CREATE TABLE IF NOT EXISTS address (
            id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            address     BYTEA NOT NULL
        );

        CREATE TABLE IF NOT EXISTS token (
            id       SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            contract BYTEA NOT NULL,
            decimals SMALLINT NOT NULL,
            symbol   TEXT
        );

        CREATE TABLE IF NOT EXISTS block (
            block_number INTEGER NOT NULL,
            timestamp    INTEGER NOT NULL,
            gas_limit    BIGINT NOT NULL,
            gas_used     BIGINT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS selector (
            id        INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            selector  BYTEA NOT NULL,  -- 4 bytes
            signature TEXT,
            source    TEXT
        );

        CREATE TABLE IF NOT EXISTS tx (
            tx_hash     BYTEA NOT NULL, -- 32 bytes
            block_number INTEGER NOT NULL,
            tx_index    SMALLINT NOT NULL,
            from_id     BIGINT NOT NULL,
            to_id       BIGINT,
            value       NUMERIC(78,0) NOT NULL,
            gas_limit   BIGINT NOT NULL,
            gas_used    BIGINT NOT NULL,
            aborted     BOOLEAN NOT NULL,
            selector_id INTEGER,
            input_len   INTEGER
        );

        CREATE TABLE IF NOT EXISTS token_transfer (
            block_number INTEGER NOT NULL,
            tx_index     SMALLINT NOT NULL,
            log_index    SMALLINT NOT NULL,
            from_id      BIGINT NOT NULL,
            to_id        BIGINT NOT NULL,
            token_id     SMALLINT NOT NULL,
            amount       NUMERIC(78,0) NOT NULL,
            transfer_type SMALLINT NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS token_event (
            block_number INTEGER NOT NULL,
            tx_index     SMALLINT NOT NULL,
            log_index    SMALLINT NOT NULL,
            token_id     SMALLINT NOT NULL,
            event_type   SMALLINT NOT NULL,
            subject_id   BIGINT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS event_sig (
            id       INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            topic0   BYTEA NOT NULL,
            signature TEXT
        );

        CREATE TABLE IF NOT EXISTS token_event_raw (
            block_number INTEGER NOT NULL,
            tx_index     SMALLINT NOT NULL,
            log_index    SMALLINT NOT NULL,
            token_id     SMALLINT NOT NULL,
            sig_id       INTEGER NOT NULL,
            topic1_id    BIGINT,
            topic2_id    BIGINT,
            topic3_id    BIGINT,
            data_len     INTEGER
        );

        CREATE TABLE IF NOT EXISTS ingest_state (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """
    else:
        return """
        CREATE TABLE IF NOT EXISTS address (
            id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            address     BYTEA UNIQUE NOT NULL   -- 20 bytes
        );

        CREATE TABLE IF NOT EXISTS token (
            id       SMALLINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            contract BYTEA UNIQUE NOT NULL,  -- 20 bytes
            decimals SMALLINT NOT NULL,
            symbol   TEXT
        );

        CREATE TABLE IF NOT EXISTS block (
            block_number INTEGER PRIMARY KEY,
            timestamp    INTEGER NOT NULL,
            gas_limit    BIGINT NOT NULL,
            gas_used     BIGINT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS selector (
            id        INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            selector  BYTEA UNIQUE NOT NULL,  -- 4 bytes
            signature TEXT,
            source    TEXT
        );

        CREATE TABLE IF NOT EXISTS tx (
            tx_hash     BYTEA PRIMARY KEY, -- 32 bytes
            block_number INTEGER NOT NULL REFERENCES block(block_number),
            tx_index    SMALLINT NOT NULL,
            from_id     BIGINT NOT NULL REFERENCES address(id),
            to_id       BIGINT REFERENCES address(id),
            value       NUMERIC(78,0) NOT NULL,
            gas_limit   BIGINT NOT NULL,
            gas_used    BIGINT NOT NULL,
            aborted     BOOLEAN NOT NULL,
            selector_id INTEGER REFERENCES selector(id),
            input_len   INTEGER
        );

        CREATE TABLE IF NOT EXISTS token_transfer (
            block_number INTEGER NOT NULL,
            tx_index     SMALLINT NOT NULL,
            log_index    SMALLINT NOT NULL,
            from_id      BIGINT NOT NULL REFERENCES address(id),
            to_id        BIGINT NOT NULL REFERENCES address(id),
            token_id     SMALLINT NOT NULL REFERENCES token(id),
            amount       NUMERIC(78,0) NOT NULL,
            transfer_type SMALLINT NOT NULL DEFAULT 0,
            PRIMARY KEY (block_number, tx_index, log_index)
        );

        CREATE TABLE IF NOT EXISTS token_event (
            block_number INTEGER NOT NULL,
            tx_index     SMALLINT NOT NULL,
            log_index    SMALLINT NOT NULL,
            token_id     SMALLINT NOT NULL REFERENCES token(id),
            event_type   SMALLINT NOT NULL,
            subject_id   BIGINT NOT NULL REFERENCES address(id),
            PRIMARY KEY (block_number, tx_index, log_index)
        );

        CREATE TABLE IF NOT EXISTS event_sig (
            id       INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            topic0   BYTEA UNIQUE NOT NULL, -- 32 bytes
            signature TEXT
        );

        CREATE TABLE IF NOT EXISTS token_event_raw (
            block_number INTEGER NOT NULL,
            tx_index     SMALLINT NOT NULL,
            log_index    SMALLINT NOT NULL,
            token_id     SMALLINT NOT NULL REFERENCES token(id),
            sig_id       INTEGER NOT NULL REFERENCES event_sig(id),
            topic1_id    BIGINT REFERENCES address(id),
            topic2_id    BIGINT REFERENCES address(id),
            topic3_id    BIGINT REFERENCES address(id),
            data_len     INTEGER,
            PRIMARY KEY (block_number, tx_index, log_index)
        );

        CREATE TABLE IF NOT EXISTS ingest_state (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """


class DB:
    def __init__(self, dsn: str):
        self.conn = psycopg.connect(dsn, row_factory=dict_row)
        self.conn.autocommit = False

    def close(self):
        self.conn.close()

    def init_schema(self, no_constraints: bool):
        with self.conn.cursor() as cur:
            cur.execute(ddl(no_constraints))
        self.conn.commit()

    def get_db_name(self) -> str:
        with self.conn.cursor() as cur:
            cur.execute("SELECT current_database() AS db;")
            return cur.fetchone()["db"]

    def get_state(self, key: str, default: str) -> str:
        with self.conn.cursor() as cur:
            cur.execute("SELECT value FROM ingest_state WHERE key=%s", (key,))
            row = cur.fetchone()
            return row["value"] if row else default

    def set_state(self, key: str, value: str):
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO ingest_state(key,value) VALUES (%s,%s) "
                "ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value",
                (key, value),
            )

    def get_db_size_bytes(self) -> int:
        with self.conn.cursor() as cur:
            cur.execute("SELECT pg_database_size(current_database()) AS size_bytes;")
            return int(cur.fetchone()["size_bytes"])

    def get_table_sizes(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT
                    relname AS table,
                    pg_total_relation_size(relid) AS total_bytes,
                    pg_relation_size(relid) AS table_bytes,
                    pg_indexes_size(relid) AS index_bytes
                FROM pg_catalog.pg_statio_user_tables
                ORDER BY total_bytes DESC;
            """)
            return cur.fetchall()

    def upsert_address_ids(self, addr_bytes: Sequence[bytes], no_constraints: bool) -> Dict[bytes, int]:
        uniq = list({a for a in addr_bytes if a is not None})
        if not uniq:
            return {}
        with self.conn.cursor() as cur:
            if no_constraints:
                # no UNIQUE, so we do a "best effort" de-dupe by checking existing first in batches.
                # For speed, we keep it simple; if you ingest without constraints, plan to dedupe at the end anyway.
                cur.execute("SELECT id, address FROM address WHERE address = ANY(%s)", (uniq,))
                rows = cur.fetchall()
                found = {r["address"]: int(r["id"]) for r in rows}
                missing = [a for a in uniq if a not in found]
                if missing:
                    cur.executemany("INSERT INTO address(address) VALUES (%s)", [(a,) for a in missing])
                    cur.execute("SELECT id, address FROM address WHERE address = ANY(%s)", (uniq,))
                    rows2 = cur.fetchall()
                    return {r["address"]: int(r["id"]) for r in rows2}
                return found
            else:
                cur.executemany(
                    "INSERT INTO address(address) VALUES (%s) ON CONFLICT (address) DO NOTHING",
                    [(a,) for a in uniq],
                )
                cur.execute("SELECT id, address FROM address WHERE address = ANY(%s)", (uniq,))
                rows = cur.fetchall()
                return {r["address"]: int(r["id"]) for r in rows}

    def upsert_token(self, t: TokenConfig, no_constraints: bool) -> int:
        with self.conn.cursor() as cur:
            if no_constraints:
                cur.execute(
                    "INSERT INTO token(contract, decimals, symbol) VALUES (%s,%s,%s) RETURNING id",
                    (to_bytes20(t.contract), t.decimals, t.symbol),
                )
                return int(cur.fetchone()["id"])
            else:
                cur.execute(
                    "INSERT INTO token(contract, decimals, symbol) VALUES (%s,%s,%s) "
                    "ON CONFLICT (contract) DO UPDATE SET decimals=EXCLUDED.decimals, symbol=EXCLUDED.symbol "
                    "RETURNING id",
                    (to_bytes20(t.contract), t.decimals, t.symbol),
                )
                return int(cur.fetchone()["id"])

    def upsert_block(self, block_number: int, timestamp: int, gas_limit: int, gas_used: int, no_constraints: bool):
        with self.conn.cursor() as cur:
            if no_constraints:
                cur.execute(
                    "INSERT INTO block(block_number,timestamp,gas_limit,gas_used) VALUES (%s,%s,%s,%s)",
                    (block_number, timestamp, gas_limit, gas_used),
                )
            else:
                cur.execute(
                    "INSERT INTO block(block_number,timestamp,gas_limit,gas_used) VALUES (%s,%s,%s,%s) "
                    "ON CONFLICT (block_number) DO UPDATE SET "
                    "timestamp=EXCLUDED.timestamp, gas_limit=EXCLUDED.gas_limit, gas_used=EXCLUDED.gas_used",
                    (block_number, timestamp, gas_limit, gas_used),
                )

    def upsert_selector_id(self, selector_bytes4: bytes, no_constraints: bool) -> Optional[int]:
        with self.conn.cursor() as cur:
            if no_constraints:
                cur.execute("INSERT INTO selector(selector) VALUES (%s) RETURNING id", (selector_bytes4,))
                return int(cur.fetchone()["id"])
            else:
                cur.execute(
                    "INSERT INTO selector(selector) VALUES (%s) "
                    "ON CONFLICT (selector) DO UPDATE SET selector=EXCLUDED.selector "
                    "RETURNING id",
                    (selector_bytes4,),
                )
                return int(cur.fetchone()["id"])

    def upsert_event_sig_id(self, topic0_bytes32: bytes, no_constraints: bool) -> int:
        with self.conn.cursor() as cur:
            if no_constraints:
                cur.execute("INSERT INTO event_sig(topic0) VALUES (%s) RETURNING id", (topic0_bytes32,))
                return int(cur.fetchone()["id"])
            else:
                cur.execute(
                    "INSERT INTO event_sig(topic0) VALUES (%s) "
                    "ON CONFLICT (topic0) DO UPDATE SET topic0=EXCLUDED.topic0 "
                    "RETURNING id",
                    (topic0_bytes32,),
                )
                return int(cur.fetchone()["id"])

    def insert_tx(self, row: Tuple, no_constraints: bool):
        with self.conn.cursor() as cur:
            if no_constraints:
                cur.execute(
                    "INSERT INTO tx(tx_hash, block_number, tx_index, from_id, to_id, value, gas_limit, gas_used, aborted, selector_id, input_len) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    row,
                )
            else:
                cur.execute(
                    "INSERT INTO tx(tx_hash, block_number, tx_index, from_id, to_id, value, gas_limit, gas_used, aborted, selector_id, input_len) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT (tx_hash) DO NOTHING",
                    row,
                )

    def insert_many(self, sql: str, rows: List[Tuple], no_constraints: bool, conflict_clause: str = ""):
        if not rows:
            return
        with self.conn.cursor() as cur:
            if no_constraints or not conflict_clause:
                cur.executemany(sql, rows)
            else:
                cur.executemany(sql + " " + conflict_clause, rows)


def disk_usage(path: str) -> Dict[str, float]:
    total, used, free = shutil.disk_usage(path)
    gb = 1024 ** 3
    return {"total_gb": total / gb, "used_gb": used / gb, "free_gb": free / gb}


def fetch_logs(w3: Web3, contract: str, from_block: int, to_block: int) -> List[LogReceipt]:
    return w3.eth.get_logs({
        "address": Web3.to_checksum_address(contract),
        "fromBlock": from_block,
        "toBlock": to_block,
    })


def ingest_range(
    w3: Web3,
    db: DB,
    token_cfgs: List[TokenConfig],
    from_block: int,
    to_block: int,
    #store_tx: bool,
    no_constraints: bool
):
    token_id_by_contract: Dict[str, int] = {}
    for t in token_cfgs:
        token_id_by_contract[Web3.to_checksum_address(t.contract)] = db.upsert_token(t, no_constraints)

    db.upsert_address_ids([ZERO_ADDR_BYTES20], no_constraints)

    block_cache = {}
    tx_cache = {}
    rcpt_cache = {}

    def get_block(bn: int):
        if bn not in block_cache:
            block_cache[bn] = w3.eth.get_block(bn)
        return block_cache[bn]

    def get_tx(txh):
        if txh not in tx_cache:
            tx_cache[txh] = w3.eth.get_transaction(txh)
        return tx_cache[txh]

    def get_rcpt(txh):
        if txh not in rcpt_cache:
            rcpt_cache[txh] = w3.eth.get_transaction_receipt(txh)
        return rcpt_cache[txh]

    for contract, token_id in token_id_by_contract.items():
        logs = fetch_logs(w3, contract, from_block, to_block)

        addr_bytes: List[bytes] = [ZERO_ADDR_BYTES20]
        tx_hashes_seen: Set[bytes] = set()

        for lg in logs:
            topics = [t.hex() for t in lg["topics"]]
            topic0 = topics[0] if topics else None

            if topic0 == TRANSFER_TOPIC0 and len(topics) >= 3:
                addr_bytes.append(topic_to_addr_bytes20(topics[1]))
                addr_bytes.append(topic_to_addr_bytes20(topics[2]))
            else:
                for i in range(1, min(4, len(topics))):
                    b20 = topic_to_addr_bytes20(topics[i])
                    if b20 is not None:
                        addr_bytes.append(b20)

            
            tx_hashes_seen.add(bytes(lg["transactionHash"]))

        addr_id = db.upsert_address_ids([a for a in addr_bytes if a is not None], no_constraints)

        if tx_hashes_seen:
            for txh in tx_hashes_seen:
                tx = get_tx(txh)
                rcpt = get_rcpt(txh)
                blk = get_block(tx["blockNumber"])

                db.upsert_block(
                    int(tx["blockNumber"]),
                    int(blk["timestamp"]),
                    int(blk["gasLimit"]),
                    int(blk["gasUsed"]),
                    no_constraints
                )

                from_b20 = to_bytes20(tx["from"])
                to_b20 = to_bytes20(tx["to"]) if tx.get("to") else None
                addr_id.update(db.upsert_address_ids([from_b20] + ([to_b20] if to_b20 else []), no_constraints))

                selector_id = None
                sel_b4 = calldata_selector_bytes4(tx.get("input", "0x"))
                if sel_b4:
                    selector_id = db.upsert_selector_id(sel_b4, no_constraints)

                input_len = 0
                if tx.get("input") and tx["input"] != "0x":
                    input_len = (len(tx["input"]) - 2) // 2

                row = (
                    txh,
                    int(tx["blockNumber"]),
                    int(tx["transactionIndex"]),
                    addr_id[from_b20],
                    addr_id.get(to_b20) if to_b20 else None,
                    int(tx["value"]),
                    int(tx["gas"]),
                    int(rcpt["gasUsed"]),
                    (rcpt.get("status", 1) == 0),
                    selector_id,
                    input_len,
                )
                db.insert_tx(row, no_constraints)

        transfer_rows: List[Tuple] = []
        event_rows: List[Tuple] = []
        raw_rows: List[Tuple] = []

        for lg in logs:
            bn = int(lg["blockNumber"])
            txi = int(lg["transactionIndex"])
            li = int(lg["logIndex"])

            topics = [t.hex() for t in lg["topics"]]
            topic0 = topics[0] if topics else None
            if not topic0:
                continue

            if topic0 == TRANSFER_TOPIC0 and len(topics) >= 3:
                fb = topic_to_addr_bytes20(topics[1])
                tb = topic_to_addr_bytes20(topics[2])
                amount = int(lg["data"], 16)

                if fb == ZERO_ADDR_BYTES20:
                    ttype = 1
                elif tb == ZERO_ADDR_BYTES20:
                    ttype = 2
                else:
                    ttype = 0

                transfer_rows.append((
                    bn, txi, li,
                    addr_id[fb],
                    addr_id[tb],
                    token_id,
                    amount,
                    ttype
                ))

            elif topic0 in BLACKLIST_TOPIC0_TO_TYPE and len(topics) >= 2:
                subj = topic_to_addr_bytes20(topics[1])
                if subj is not None:
                    event_rows.append((
                        bn, txi, li,
                        token_id,
                        BLACKLIST_TOPIC0_TO_TYPE[topic0],
                        addr_id[subj],
                    ))
                else:
                    sig_id = db.upsert_event_sig_id(to_bytes32(topic0), no_constraints)
                    raw_rows.append((bn, txi, li, token_id, sig_id, None, None, None,
                                     (len(lg["data"]) - 2)//2 if isinstance(lg["data"], str) else None))

            else:
                sig_id = db.upsert_event_sig_id(to_bytes32(topic0), no_constraints)

                t1 = topic_to_addr_bytes20(topics[1]) if len(topics) > 1 else None
                t2 = topic_to_addr_bytes20(topics[2]) if len(topics) > 2 else None
                t3 = topic_to_addr_bytes20(topics[3]) if len(topics) > 3 else None

                addr_id.update(db.upsert_address_ids([b for b in [t1, t2, t3] if b is not None], no_constraints))

                raw_rows.append((
                    bn, txi, li,
                    token_id,
                    sig_id,
                    addr_id.get(t1) if t1 else None,
                    addr_id.get(t2) if t2 else None,
                    addr_id.get(t3) if t3 else None,
                    (len(lg["data"]) - 2)//2 if isinstance(lg["data"], str) else None
                ))

        db.insert_many(
            "INSERT INTO token_transfer(block_number,tx_index,log_index,from_id,to_id,token_id,amount,transfer_type) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            transfer_rows,
            no_constraints,
            "ON CONFLICT (block_number,tx_index,log_index) DO NOTHING"
        )
        db.insert_many(
            "INSERT INTO token_event(block_number,tx_index,log_index,token_id,event_type,subject_id) VALUES (%s,%s,%s,%s,%s,%s)",
            event_rows,
            no_constraints,
            "ON CONFLICT (block_number,tx_index,log_index) DO NOTHING"
        )
        db.insert_many(
            "INSERT INTO token_event_raw(block_number,tx_index,log_index,token_id,sig_id,topic1_id,topic2_id,topic3_id,data_len) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            raw_rows,
            no_constraints,
            "ON CONFLICT (block_number,tx_index,log_index) DO NOTHING"
        )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rpc", default=os.getenv("ETH_RPC_URL"), help="RPC URL (HTTP)")
    ap.add_argument("--pg", default=os.getenv("PG_DSN"), help="Postgres DSN")
    ap.add_argument("--start", type=int, default=None, help="Start block (inclusive)")
    ap.add_argument("--end", type=int, default=None, help="End block (inclusive); default latest")
    ap.add_argument("--chunk", type=int, default=int(os.getenv("BLOCK_CHUNK", "5000")))
    #ap.add_argument("--store-tx", action="store_true", help="Store tx rows + selectors")
    ap.add_argument("--no-constraints", action="store_true", help="Create tables without PK/UNIQUE; faster ingest")
    ap.add_argument("--report-every", type=int, default=5, help="Print storage report every N chunks")
    ap.add_argument("--disk-path", default="/", help="Path to check disk usage for (e.g. /data)")
    args = ap.parse_args()

    if not args.rpc or not args.pg:
        print("Need --rpc and --pg (or ETH_RPC_URL and PG_DSN env vars).", file=sys.stderr)
        sys.exit(1)

    w3 = Web3(Web3.HTTPProvider(args.rpc, request_kwargs={"timeout": 120}))
    if not w3.is_connected():
        print("RPC not reachable.", file=sys.stderr)
        sys.exit(1)

    # Fill your token list here (Ethereum contracts). Remove placeholders.
    tokens = [
        TokenConfig("USDT",  Web3.to_checksum_address("0xdAC17F958D2ee523a2206206994597C13D831ec7"), 6),
        TokenConfig("USDC",  Web3.to_checksum_address("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"), 6),
        TokenConfig("DAI",   Web3.to_checksum_address("0x6B175474E89094C44Da98b954EedeAC495271d0F"), 18),
        TokenConfig("FRAX",  Web3.to_checksum_address("0x853d955aCEf822Db058eb8505911ED77F175b99e"), 18),
        TokenConfig("EURC",  Web3.to_checksum_address("0x1aBaEA1f7C830bD89Acc67eC4af516284b1bC33c"), 6),
        TokenConfig("EURT",  Web3.to_checksum_address("0xC581b735A1688071A1746c968e0798D642EDE491"), 6),
        TokenConfig("PYUSD", Web3.to_checksum_address("0x6c3ea9036406852006290770BEdFcAbA0e23A0e8"), 6),
        TokenConfig("USDS",  Web3.to_checksum_address("0xdC035D45d973E3EC169d2276DDab16f1e407384F"), 18),
        TokenConfig("USDe",  Web3.to_checksum_address("0x4c9EDD5852cd905f086C759E8383e09bff1E68B3"), 18),
        TokenConfig("EURCV", Web3.to_checksum_address("0x5F7827FDeb7c20b443265Fc2F40845B715385Ff2"), 18),
        TokenConfig("BRZ",   Web3.to_checksum_address("0x01d33FD36ec67c6Ada32cf36b31e88EE190B1839"), 18),
    ]

    db = DB(args.pg)
    db.init_schema(args.no_constraints)
    print(f"Connected to DB: {db.get_db_name()}  (no_constraints={args.no_constraints})")

    latest = w3.eth.block_number if args.end is None else args.end
    start = args.start
    if start is None:
        start = int(db.get_state("last_ingested_block", "0"))

    print(f"Latest block: {latest}")
    print(f"Starting from: {start}, chunk={args.chunk}")

    start_initial = start
    chunk_i = 0

    try:
        cur = start
        while cur <= latest:
            end = min(cur + args.chunk - 1, latest)
            t0 = time.time()

            try:
                ingest_range(
                    w3=w3,
                    db=db,
                    token_cfgs=tokens,
                    from_block=cur,
                    to_block=end,
                    #store_tx=args.store_tx,
                    no_constraints=args.no_constraints,
                )
                db.set_state("last_ingested_block", str(end + 1))
                db.conn.commit()

            except Exception as e:
                db.conn.rollback()
                print(f"[ERROR] blocks {cur}-{end}: {e}", file=sys.stderr)
                time.sleep(2)
                continue

            dt = time.time() - t0
            chunk_i += 1
            print(f"Ingested blocks {cur}-{end} in {dt:.1f}s; next={end+1}")

            if args.report_every > 0 and (chunk_i % args.report_every == 0):
                db_size = db.get_db_size_bytes()
                disk = disk_usage(args.disk_path)
                processed = (end - start_initial + 1)
                total = (latest - start_initial + 1)
                est_final = (db_size * (total / max(1, processed)))

                print("\n--- STORAGE / PROGRESS REPORT ---")
                print(f"Progress: {processed:,}/{total:,} blocks ({100*processed/total:.2f}%)")
                print(f"DB size now: {db_size/1024/1024:,.2f} MB  |  projected final: {est_final/1024/1024/1024:,.2f} GB")
                print(f"Disk {args.disk_path}: used {disk['used_gb']:.1f} / {disk['total_gb']:.1f} GB ({100*disk['used_gb']/disk['total_gb']:.1f}%), free {disk['free_gb']:.1f} GB")

                tables = db.get_table_sizes()
                print("Top tables:")
                for t in tables[:8]:
                    print(
                        f"  {t['table']:<20} {t['total_bytes']/1024/1024/1024:6.2f} GB "
                        f"(table {t['table_bytes']/1024/1024/1024:5.2f} GB, "
                        f"index {t['index_bytes']/1024/1024/1024:5.2f} GB)"
                    )
                print("---------------------------------\n")

            cur = end + 1

    finally:
        db.close()


if __name__ == "__main__":
    main()
