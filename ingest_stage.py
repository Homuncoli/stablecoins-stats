#!/usr/bin/env python3
from __future__ import annotations

"""
FAST staging ingest:
- ALL ERC-20 Transfer logs across all token contracts (global eth_getLogs topic0=Transfer)
- YAML-driven "special events" for selected issuer contracts (USDT/USDC/EURC/...):
    - mint/burn (Issue/Redeem or Mint/Burn events etc.)
    - blacklist/unblacklist/destroyed funds etc.

Key performance choices:
- NO indexes / PK / FK during ingest (create later)
- NO address/token id lookups during ingest
- Precompute YAML topic0 -> (bytes, key, type) dict once (no repeated per-chunk building)
- Optionally use COPY for much faster Postgres inserts than executemany

Dependencies:
  pip install requests psycopg[binary] eth-utils pyyaml

Env:
  export ETH_RPC_URL="http://127.0.0.1:8545"
  export PG_DSN="postgresql://user:pass@host:5432/dbname"
  export TOKEN_YAML="/path/to/tokens.yaml"

Run:
  python ingest_stage.py --latest 1000 --use-copy
  python ingest_stage.py --start 19000000 --end 19005000 --chunk 2000 --use-copy
"""

import argparse
import io
import os
import sys
import time
import shutil
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests
import psycopg
from psycopg.rows import dict_row
import yaml
from eth_utils import keccak, to_checksum_address
from dotenv import load_dotenv
load_dotenv()  # loads .env from current working directory


# ---------------- Constants ---------------- #

ZERO20 = b"\x00" * 20
TRANSFER_TOPIC0 = "0x" + keccak(text="Transfer(address,address,uint256)").hex()

# ---------------- Helpers ---------------- #

def strip_0x(h: str) -> str:
    return h[2:] if h.startswith("0x") else h

def hex_to_int(h: str) -> int:
    return int(h, 16)

def hex_to_bytes(h: str) -> bytes:
    hs = strip_0x(h)
    if hs == "":
        return b""
    return bytes.fromhex(hs)

def addr_hex_to_20(addr_hex: str) -> bytes:
    b = hex_to_bytes(addr_hex)
    if len(b) == 20:
        return b
    return b[-20:].rjust(20, b"\x00")

def topic_to_addr20(topic32_hex: str) -> Optional[bytes]:
    if not topic32_hex or topic32_hex == "0x":
        return None
    b = hex_to_bytes(topic32_hex)
    if len(b) != 32:
        b = b.rjust(32, b"\x00")
    return b[-20:]

def topic0_from_signature(sig: str) -> str:
    return "0x" + keccak(text=sig).hex()

def disk_usage(path: str) -> Dict[str, float]:
    total, used, free = shutil.disk_usage(path)
    gb = 1024 ** 3
    return {"total_gb": total / gb, "used_gb": used / gb, "free_gb": free / gb}

# Postgres COPY text format needs escaping for bytea if you use \x... strings.
def bytea_to_pg_hex(b: bytes) -> str:
    return "\\x" + b.hex()

# ---------------- JSON-RPC ---------------- #

class RpcError(RuntimeError):
    pass

class Rpc:
    def __init__(self, url: str, timeout: int = 120):
        self.url = url
        self.timeout = timeout
        self.sess = requests.Session()
        self._id = 1

    def call(self, method: str, params: Sequence[Any]) -> Any:
        payload = {"jsonrpc": "2.0", "id": self._id, "method": method, "params": list(params)}
        self._id += 1
        r = self.sess.post(self.url, json=payload, timeout=self.timeout)
        r.raise_for_status()
        j = r.json()
        if "error" in j and j["error"] is not None:
            raise RpcError(f"{method} error: {j['error']}")
        return j["result"]

    def block_number(self) -> int:
        return hex_to_int(self.call("eth_blockNumber", []))

    def get_logs(self, from_block: int, to_block: int, address: Optional[str], topics: List[Any]) -> List[Dict[str, Any]]:
        q: Dict[str, Any] = {
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
            "topics": topics,
        }
        if address is not None:
            q["address"] = address
        return self.call("eth_getLogs", [q])

def adaptive_get_logs(rpc: Rpc, from_block: int, to_block: int, address: Optional[str], topics: List[Any], max_splits: int = 20) -> List[Dict[str, Any]]:
    """
    eth_getLogs can fail on big ranges/high density. Recursively split until it works.
    """
    try:
        return rpc.get_logs(from_block, to_block, address, topics)
    except Exception:
        if from_block >= to_block or max_splits <= 0:
            raise
        mid = (from_block + to_block) // 2
        left = adaptive_get_logs(rpc, from_block, mid, address, topics, max_splits - 1)
        right = adaptive_get_logs(rpc, mid + 1, to_block, address, topics, max_splits - 1)
        return left + right

# ---------------- YAML config ---------------- #

@dataclass(frozen=True)
class TokenEventSpec:
    key: str
    signature: str
    type_label: str
    topic0: str

@dataclass(frozen=True)
class TokenCfg:
    name: str
    address: str  # checksum
    symbol: str
    decimals: int
    mint_burn_via_zero: bool
    events: List[TokenEventSpec]

def load_token_cfgs(yaml_path: str) -> List[TokenCfg]:
    with open(yaml_path, "r", encoding="utf-8") as f:
        doc = yaml.safe_load(f)

    stablecoins = doc.get("stablecoins", {}) or {}
    out: List[TokenCfg] = []

    for name, cfg in stablecoins.items():
        addr = to_checksum_address(cfg["address"])
        symbol = cfg.get("symbol", name.upper())
        decimals = int(cfg.get("decimals", 18))

        events_cfg = (cfg.get("events") or {})
        mbvz = bool((events_cfg.get("mint_burn_via_zero") or {}).get("enabled", False))

        events: List[TokenEventSpec] = []
        for key, e in events_cfg.items():
            if key == "mint_burn_via_zero":
                continue
            sig = e.get("signature")
            tlabel = e.get("type")
            if not sig or not tlabel:
                continue
            events.append(TokenEventSpec(key=key, signature=sig, type_label=tlabel, topic0=topic0_from_signature(sig)))

        out.append(TokenCfg(name=name, address=addr, symbol=symbol, decimals=decimals, mint_burn_via_zero=mbvz, events=events))

    return out

def build_token_event_maps(token_cfgs: List[TokenCfg]) -> Tuple[
    Dict[str, Dict[str, Tuple[bytes, str, str]]],
    Dict[str, List[Any]]
]:
    """
    Precompute once:
      token_event_map[contract][topic0_lower] = (topic0_bytes32, event_key, event_type)
      token_topics_or[contract] = [[topic0a, topic0b, ...]]   # OR list for eth_getLogs topics[0]
    """
    token_event_map: Dict[str, Dict[str, Tuple[bytes, str, str]]] = {}
    token_topics_or: Dict[str, List[Any]] = {}

    for t in token_cfgs:
        m: Dict[str, Tuple[bytes, str, str]] = {}
        topic0s: List[str] = []
        for e in t.events:
            t0 = e.topic0.lower()
            m[t0] = (hex_to_bytes(t0), e.key, e.type_label)
            topic0s.append(t0)

        token_event_map[t.address] = m
        token_topics_or[t.address] = [topic0s]

    return token_event_map, token_topics_or

# ---------------- Postgres staging schema ---------------- #

DDL = """
-- No PK/UNIQUE/index/FK while ingesting.

CREATE TABLE IF NOT EXISTS stage_erc20_transfer (
    block_number   INTEGER NOT NULL,
    tx_index       INTEGER NOT NULL,
    log_index      INTEGER NOT NULL,
    token_contract BYTEA   NOT NULL, -- 20 bytes
    from_addr      BYTEA   NOT NULL, -- 20 bytes
    to_addr        BYTEA   NOT NULL, -- 20 bytes
    amount         NUMERIC(78,0) NOT NULL,
    transfer_type  SMALLINT NOT NULL -- 0 normal, 1 mint, 2 burn
);

CREATE TABLE IF NOT EXISTS stage_token_event_typed (
    block_number   INTEGER NOT NULL,
    tx_index       INTEGER NOT NULL,
    log_index      INTEGER NOT NULL,
    token_contract BYTEA   NOT NULL, -- 20 bytes
    topic0         BYTEA   NOT NULL, -- 32 bytes
    event_key      TEXT    NOT NULL,
    event_type     TEXT    NOT NULL,
    subject_addr   BYTEA,
    data_len       INTEGER
);

CREATE TABLE IF NOT EXISTS stage_token_event_raw (
    block_number   INTEGER NOT NULL,
    tx_index       INTEGER NOT NULL,
    log_index      INTEGER NOT NULL,
    token_contract BYTEA   NOT NULL,
    topic0         BYTEA   NOT NULL, -- 32 bytes
    topic1_addr    BYTEA,
    topic2_addr    BYTEA,
    topic3_addr    BYTEA,
    data_len       INTEGER
);

CREATE TABLE IF NOT EXISTS ingest_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

class DB:
    def __init__(self, dsn: str, synchronous_commit_off: bool = False):
        self.conn = psycopg.connect(dsn, row_factory=dict_row)
        self.conn.autocommit = False
        if synchronous_commit_off:
            with self.conn.cursor() as cur:
                cur.execute("SET synchronous_commit = off;")
            self.conn.commit()

    def close(self):
        self.conn.close()

    def init_schema(self):
        with self.conn.cursor() as cur:
            cur.execute(DDL)
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

    # ---------- executemany writers ----------

    def insert_many(self, sql: str, rows: List[Tuple]):
        if not rows:
            return
        with self.conn.cursor() as cur:
            cur.executemany(sql, rows)

    # ---------- COPY writers (much faster) ----------

    def copy_stage_erc20_transfer(self, rows: List[Tuple]) -> None:
        """
        COPY is often 5-20x faster than executemany for large batches.
        We use COPY ... FROM STDIN with TEXT format.

        Expected row tuple:
          (block_number, tx_index, log_index, token20, from20, to20, amount_int, transfer_type)
        """
        if not rows:
            return
        buf = io.StringIO()
        for (bn, txi, li, token20, from20, to20, amount, ttype) in rows:
            # Tab-separated, newline-terminated; bytea as \x....
            buf.write(
                f"{bn}\t{txi}\t{li}\t{bytea_to_pg_hex(token20)}\t{bytea_to_pg_hex(from20)}\t{bytea_to_pg_hex(to20)}\t{amount}\t{ttype}\n"
            )
        buf.seek(0)
        with self.conn.cursor() as cur:
            with cur.copy(
                "COPY stage_erc20_transfer (block_number, tx_index, log_index, token_contract, from_addr, to_addr, amount, transfer_type) FROM STDIN"
            ) as cp:
                cp.write(buf.getvalue())

    def copy_stage_token_event_typed(self, rows: List[Tuple]) -> None:
        """
        Expected tuple:
          (bn, txi, li, token20, topic0bytes32, event_key, event_type, subject20_or_None, data_len_or_None)
        """
        if not rows:
            return
        buf = io.StringIO()
        for (bn, txi, li, token20, topic0b, event_key, event_type, subj, data_len) in rows:
            subj_s = "\\N" if subj is None else bytea_to_pg_hex(subj)
            dl_s = "\\N" if data_len is None else str(data_len)
            # text columns must be escaped for tabs/newlines/backslashes; event_key/type are from YAML so keep them simple
            ek = event_key.replace("\\", "\\\\").replace("\t", " ").replace("\n", " ")
            et = event_type.replace("\\", "\\\\").replace("\t", " ").replace("\n", " ")
            buf.write(
                f"{bn}\t{txi}\t{li}\t{bytea_to_pg_hex(token20)}\t{bytea_to_pg_hex(topic0b)}\t{ek}\t{et}\t{subj_s}\t{dl_s}\n"
            )
        buf.seek(0)
        with self.conn.cursor() as cur:
            with cur.copy(
                "COPY stage_token_event_typed (block_number, tx_index, log_index, token_contract, topic0, event_key, event_type, subject_addr, data_len) FROM STDIN"
            ) as cp:
                cp.write(buf.getvalue())

    def copy_stage_token_event_raw(self, rows: List[Tuple]) -> None:
        """
        Expected tuple:
          (bn, txi, li, token20, topic0b, t1, t2, t3, data_len)
        """
        if not rows:
            return
        buf = io.StringIO()
        for (bn, txi, li, token20, topic0b, t1, t2, t3, data_len) in rows:
            t1s = "\\N" if t1 is None else bytea_to_pg_hex(t1)
            t2s = "\\N" if t2 is None else bytea_to_pg_hex(t2)
            t3s = "\\N" if t3 is None else bytea_to_pg_hex(t3)
            dls = "\\N" if data_len is None else str(data_len)
            buf.write(
                f"{bn}\t{txi}\t{li}\t{bytea_to_pg_hex(token20)}\t{bytea_to_pg_hex(topic0b)}\t{t1s}\t{t2s}\t{t3s}\t{dls}\n"
            )
        buf.seek(0)
        with self.conn.cursor() as cur:
            with cur.copy(
                "COPY stage_token_event_raw (block_number, tx_index, log_index, token_contract, topic0, topic1_addr, topic2_addr, topic3_addr, data_len) FROM STDIN"
            ) as cp:
                cp.write(buf.getvalue())

# ---------------- Ingest functions ---------------- #

def ingest_chunk_all_transfers(
    rpc: Rpc,
    from_block: int,
    to_block: int,
) -> List[Tuple]:
    """
    Returns list of stage_erc20_transfer rows.
    """
    logs = adaptive_get_logs(rpc, from_block, to_block, address=None, topics=[TRANSFER_TOPIC0])
    rows: List[Tuple] = []

    for lg in logs:
        topics = lg.get("topics", []) or []
        if len(topics) < 3:
            continue

        token20 = addr_hex_to_20(lg["address"])
        from20 = topic_to_addr20(topics[1]) or ZERO20
        to20 = topic_to_addr20(topics[2]) or ZERO20
        amount = int(lg.get("data", "0x0"), 16)

        if from20 == ZERO20:
            ttype = 1
        elif to20 == ZERO20:
            ttype = 2
        else:
            ttype = 0

        bn = hex_to_int(lg["blockNumber"])
        txi = hex_to_int(lg["transactionIndex"])
        li = hex_to_int(lg["logIndex"])

        rows.append((bn, txi, li, token20, from20, to20, amount, ttype))

    return rows

def ingest_chunk_yaml_events(
    rpc: Rpc,
    token_cfgs: List[TokenCfg],
    token_event_map: Dict[str, Dict[str, Tuple[bytes, str, str]]],
    token_topics_or: Dict[str, List[Any]],
    from_block: int,
    to_block: int,
) -> Tuple[List[Tuple], List[Tuple]]:
    """
    Returns (typed_rows, raw_rows)
    typed tuple:
      (bn, txi, li, token20, topic0bytes32, event_key, event_type, subject20_or_None, data_len_or_None)
    raw tuple:
      (bn, txi, li, token20, topic0bytes32, t1, t2, t3, data_len)
    """
    typed_rows: List[Tuple] = []
    raw_rows: List[Tuple] = []

    for t in token_cfgs:
        m = token_event_map.get(t.address)
        topics_or = token_topics_or.get(t.address)
        if not m or not topics_or:
            continue

        logs = adaptive_get_logs(rpc, from_block, to_block, address=t.address, topics=topics_or)

        for lg in logs:
            topics = lg.get("topics", []) or []
            if not topics:
                continue

            spec = m.get(topics[0].lower())
            if spec is None:
                continue

            topic0_bytes, event_key, event_type = spec

            bn = hex_to_int(lg["blockNumber"])
            txi = hex_to_int(lg["transactionIndex"])
            li = hex_to_int(lg["logIndex"])
            token20 = addr_hex_to_20(lg["address"])

            t1 = topic_to_addr20(topics[1]) if len(topics) > 1 else None
            t2 = topic_to_addr20(topics[2]) if len(topics) > 2 else None
            t3 = topic_to_addr20(topics[3]) if len(topics) > 3 else None

            data = lg.get("data", "0x")
            data_len = (len(data) - 2) // 2 if isinstance(data, str) and data.startswith("0x") else None

            typed_rows.append((bn, txi, li, token20, topic0_bytes, event_key, event_type, t1, data_len))
            raw_rows.append((bn, txi, li, token20, topic0_bytes, t1, t2, t3, data_len))

    return typed_rows, raw_rows

# ---------------- Main ---------------- #

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rpc", default=os.getenv("ETH_RPC_URL"), help="RPC URL (HTTP)")
    ap.add_argument("--pg", default=os.getenv("PG_DSN"), help="Postgres DSN")
    ap.add_argument("--yaml", default=os.getenv("TOKEN_YAML"), help="Path to YAML config with token events")
    ap.add_argument("--start", type=int, default=None, help="Start block inclusive. Default resume from DB state.")
    ap.add_argument("--end", type=int, default=None, help="End block inclusive. Default latest head.")
    ap.add_argument("--latest", type=int, default=None, help="Convenience: scrape latest N blocks (overrides start/end).")
    ap.add_argument("--chunk", type=int, default=int(os.getenv("BLOCK_CHUNK", "2000")), help="Blocks per step (auto-splits on errors).")
    ap.add_argument("--report-every", type=int, default=5, help="Storage report every N chunks.")
    ap.add_argument("--disk-path", default="/", help="Disk path (e.g. /data)")
    ap.add_argument("--disable-yaml-events", action="store_true", help="Only ingest global ERC-20 transfers.")
    ap.add_argument("--use-copy", action="store_true", help="Use COPY for inserts (faster).")
    ap.add_argument("--synchronous-commit-off", action="store_true", help="SET synchronous_commit=off (faster; risk on crash).")
    args = ap.parse_args()

    if not args.rpc or not args.pg:
        print("Need --rpc and --pg (or ETH_RPC_URL and PG_DSN).", file=sys.stderr)
        sys.exit(1)

    rpc = Rpc(args.rpc)

    # Determine range
    head = rpc.block_number()
    if args.latest is not None:
        end_block = head
        start_block = max(0, head - args.latest + 1)
    else:
        end_block = head if args.end is None else args.end
        # start can resume from state if not provided
        start_block = args.start

    # YAML load + precompute maps
    token_cfgs: List[TokenCfg] = []
    token_event_map: Dict[str, Dict[str, Tuple[bytes, str, str]]] = {}
    token_topics_or: Dict[str, List[Any]] = {}

    if not args.disable_yaml_events:
        if not args.yaml or not os.path.exists(args.yaml):
            print("YAML not found. Either set --yaml/TOKEN_YAML or use --disable-yaml-events.", file=sys.stderr)
            sys.exit(1)
        token_cfgs = load_token_cfgs(args.yaml)
        token_event_map, token_topics_or = build_token_event_maps(token_cfgs)
        print(f"Loaded {len(token_cfgs)} token configs from YAML.")

    db = DB(args.pg, synchronous_commit_off=args.synchronous_commit_off)
    db.init_schema()
    print(f"Connected to DB: {db.get_db_name()}")

    if start_block is None:
        start_block = int(db.get_state("last_ingested_block", "0"))

    print(f"Head block: {head}")
    print(f"Scraping blocks {start_block}..{end_block} (chunk={args.chunk}) | use_copy={args.use_copy} yaml_events={not args.disable_yaml_events}")

    start_initial = start_block
    chunk_i = 0
    total_transfers = 0
    total_yaml_events = 0

    try:
        cur = start_block
        while cur <= end_block:
            end = min(cur + args.chunk - 1, end_block)
            t0 = time.time()

            try:
                transfer_rows = ingest_chunk_all_transfers(rpc, cur, end)

                typed_rows: List[Tuple] = []
                raw_rows: List[Tuple] = []
                if token_cfgs:
                    typed_rows, raw_rows = ingest_chunk_yaml_events(
                        rpc, token_cfgs, token_event_map, token_topics_or, cur, end
                    )

                # Write
                if args.use_copy:
                    db.copy_stage_erc20_transfer(transfer_rows)
                    if typed_rows:
                        db.copy_stage_token_event_typed(typed_rows)
                    if raw_rows:
                        db.copy_stage_token_event_raw(raw_rows)
                else:
                    db.insert_many(
                        "INSERT INTO stage_erc20_transfer(block_number,tx_index,log_index,token_contract,from_addr,to_addr,amount,transfer_type) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                        transfer_rows,
                    )
                    if typed_rows:
                        db.insert_many(
                            "INSERT INTO stage_token_event_typed(block_number,tx_index,log_index,token_contract,topic0,event_key,event_type,subject_addr,data_len) "
                            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                            typed_rows,
                        )
                    if raw_rows:
                        db.insert_many(
                            "INSERT INTO stage_token_event_raw(block_number,tx_index,log_index,token_contract,topic0,topic1_addr,topic2_addr,topic3_addr,data_len) "
                            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                            raw_rows,
                        )

                total_transfers += len(transfer_rows)
                total_yaml_events += len(typed_rows)

                db.set_state("last_ingested_block", str(end + 1))
                db.conn.commit()

            except Exception as e:
                db.conn.rollback()
                print(f"[ERROR] blocks {cur}-{end}: {e}", file=sys.stderr)
                time.sleep(2)
                if args.chunk > 200:
                    args.chunk = max(200, args.chunk // 2)
                    print(f"Reducing chunk size to {args.chunk} and retrying...", file=sys.stderr)
                continue

            dt = time.time() - t0
            chunk_i += 1
            print(
                f"Ingested blocks {cur}-{end} in {dt:.1f}s | "
                f"transfers={len(transfer_rows):,} yaml_events={len(typed_rows):,} | next={end+1}"
            )

            if args.report_every > 0 and (chunk_i % args.report_every == 0):
                db_size = db.get_db_size_bytes()
                disk = disk_usage(args.disk_path)
                processed = (end - start_initial + 1)
                total = (end_block - start_initial + 1)
                est_final = (db_size * (total / max(1, processed)))

                print("\n--- STORAGE / PROGRESS REPORT ---")
                print(f"Progress: {processed:,}/{total:,} blocks ({100*processed/total:.2f}%)")
                print(f"Rows so far: transfers={total_transfers:,} yaml_events={total_yaml_events:,}")
                print(f"DB size now: {db_size/1024/1024:,.2f} MB  |  projected final: {est_final/1024/1024/1024:,.2f} GB")
                print(
                    f"Disk {args.disk_path}: used {disk['used_gb']:.1f}/{disk['total_gb']:.1f} GB "
                    f"({100*disk['used_gb']/disk['total_gb']:.1f}%), free {disk['free_gb']:.1f} GB"
                )

                tables = db.get_table_sizes()
                print("Top tables:")
                for t in tables[:8]:
                    print(
                        f"  {t['table']:<28} {t['total_bytes']/1024/1024/1024:6.2f} GB "
                        f"(table {t['table_bytes']/1024/1024/1024:5.2f} GB, index {t['index_bytes']/1024/1024/1024:5.2f} GB)"
                    )
                print("---------------------------------\n")

            cur = end + 1

    finally:
        db.close()

if __name__ == "__main__":
    main()
