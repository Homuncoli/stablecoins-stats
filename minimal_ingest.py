"""
Install:
  pip install psycopg[binary] requests eth-utils pyyaml

Env:
  ETH_RPC_URL
  PG_DSN
  BLOCK_CHUNK    default 5000   # block range for eth_getLogs
  BLOCK_BATCH    default 200    # how many eth_getBlockByNumber calls per HTTP batch
  RPC_TIMEOUT    default 60

Usage:
  python ingest_all.py --start 18000000 --end 18005000
  python ingest_all.py --start 18000000 --end 18005000 --chunk 1000 --block-batch 100
  python ingest_all.py --start 18000000 --yaml config/stablecoins_detailed.yaml   # only to ADD signatures (still no filtering)
"""

import os
import time
import argparse
from typing import Any, Dict, List, Optional, Tuple

import requests
import psycopg
import yaml
from eth_utils import keccak


# ---------------- RPC CLIENT (raw JSON-RPC) ---------------- #

class RpcClient:
    def __init__(self, url: str, timeout: int = 60, max_retries: int = 6):
        self.url = url
        self.session = requests.Session()
        self.timeout = timeout
        self.max_retries = max_retries
        self._id = 0

    def _next_id(self) -> int:
        self._id += 1
        return self._id

    def _post(self, payload: Any) -> Any:
        last = None
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.post(self.url, json=payload, timeout=self.timeout)
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                last = e
                time.sleep(min(2 ** (attempt - 1), 20))
        raise RuntimeError(f"RPC request failed after retries: {last}") from last

    def call(self, method: str, params: list) -> Any:
        payload = {"jsonrpc": "2.0", "id": self._next_id(), "method": method, "params": params}
        data = self._post(payload)
        if isinstance(data, dict) and "error" in data:
            raise RuntimeError(f"RPC error: {data['error']}")
        return data["result"]

    def batch(self, calls: List[Tuple[str, list]]) -> List[Any]:
        """
        calls: [(method, params), ...]
        returns results in the same order
        """
        payload = []
        ids: List[int] = []
        for method, params in calls:
            cid = self._next_id()
            ids.append(cid)
            payload.append({"jsonrpc": "2.0", "id": cid, "method": method, "params": params})

        data = self._post(payload)
        if not isinstance(data, list):
            raise RuntimeError(f"Expected batch response list, got {type(data)}")

        by_id = {item["id"]: item for item in data}
        out: List[Any] = []
        for cid in ids:
            item = by_id.get(cid)
            if item is None:
                raise RuntimeError(f"Missing batch response for id={cid}")
            if "error" in item:
                raise RuntimeError(f"RPC error for id={cid}: {item['error']}")
            out.append(item.get("result"))
        return out

    # Convenience wrappers
    def eth_block_number(self) -> int:
        return int(self.call("eth_blockNumber", []), 16)

    def eth_get_logs(self, from_block: int, to_block: int, topic0_or: List[str]) -> List[dict]:
        params = [{
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
            "topics": [topic0_or],  # OR on topics[0]
        }]
        return self.call("eth_getLogs", params)

    def eth_get_block_by_number_batch(self, block_numbers: List[int], full_tx: bool = True) -> List[dict]:
        calls = [("eth_getBlockByNumber", [hex(bn), full_tx]) for bn in block_numbers]
        return self.batch(calls)


# ---------------- helpers (hex parsing) ---------------- #

def h2i(x: Any) -> int:
    return int(x, 16) if isinstance(x, str) and x.startswith("0x") else int(x)

def hex_to_bytes20(addr_hex: Optional[str]) -> Optional[bytes]:
    if addr_hex is None:
        return None
    h = addr_hex[2:] if addr_hex.startswith("0x") else addr_hex
    b = bytes.fromhex(h)
    if len(b) != 20:
        raise ValueError(f"expected 20-byte address, got {len(b)} bytes: {addr_hex}")
    return b

def topic_to_addr(topic_hex: str) -> bytes:
    # last 20 bytes of the 32-byte topic
    h = topic_hex[2:] if topic_hex.startswith("0x") else topic_hex
    t = bytes.fromhex(h)
    if len(t) != 32:
        raise ValueError("topic not 32 bytes")
    return t[-20:]

def method_id_from_input(input_hex: Optional[str], to_addr: Optional[bytes]) -> Optional[bytes]:
    if to_addr is None:
        return None
    if not input_hex or input_hex == "0x":
        return None
    h = input_hex[2:] if input_hex.startswith("0x") else input_hex
    if len(h) < 8:
        return None
    return bytes.fromhex(h[:8])

def uint256_from_data(data_hex: str, word_index: int = 0) -> int:
    h = data_hex[2:] if data_hex.startswith("0x") else data_hex
    if not h:
        return 0
    start = word_index * 64
    end = start + 64
    if len(h) < end:
        raise ValueError(f"data too short for uint256 word {word_index}")
    return int(h[start:end], 16)

def topic0(sig: str) -> str:
    return "0x" + keccak(text=sig).hex()


def safe_int_hex(x):
    if x is None:
        return None
    return int(x, 16) if isinstance(x, str) and x.startswith("0x") else int(x)



# ---------------- event signatures & mapping (inspired by your working parser) ---------------- #

TRANSFER_SIG = "Transfer(address,address,uint256)"
TRANSFER_TOPIC0 = topic0(TRANSFER_SIG)

# Compact event_type codes (smallint) for token_event
EV_ISSUE = 2
EV_REDEEM = 3
EV_ADDED_BLACKLIST = 4
EV_REMOVED_BLACKLIST = 5
EV_DESTROYED_BLACK_FUNDS = 6

# Default special signatures (extendable)
DEFAULT_SPECIAL_SIGS = [
    # blacklist
    "Blacklisted(address)",
    "UnBlacklisted(address)",
    "AddedBlackList(address)",
    "RemovedBlackList(address)",
    "DestroyedBlackFunds(address,uint256)",
    # issuance/mint/burn
    "Issue(uint256)",
    "Redeem(uint256)",
    "Mint(address,address,uint256)",
    "Burn(address,uint256)",
    "FRAXMinted(address,address,uint256)",
    "FRAXBurned(address,address,uint256)",
]

SIG_TO_EVENTTYPE = {
    # issuance
    "Issue(uint256)": EV_ISSUE,
    "Mint(address,address,uint256)": EV_ISSUE,
    "FRAXMinted(address,address,uint256)": EV_ISSUE,
    # redemption
    "Redeem(uint256)": EV_REDEEM,
    "Burn(address,uint256)": EV_REDEEM,
    "FRAXBurned(address,address,uint256)": EV_REDEEM,
    # blacklist add/remove
    "AddedBlackList(address)": EV_ADDED_BLACKLIST,
    "Blacklisted(address)": EV_ADDED_BLACKLIST,
    "RemovedBlackList(address)": EV_REMOVED_BLACKLIST,
    "UnBlacklisted(address)": EV_REMOVED_BLACKLIST,
    # destroyed
    "DestroyedBlackFunds(address,uint256)": EV_DESTROYED_BLACK_FUNDS,
}

def load_sigs_from_yaml(path: Optional[str]) -> List[str]:
    """
    Uses your stablecoins YAML only as a source of signatures.
    Still NO address filtering.
    """
    if not path:
        return []
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    sigs: List[str] = []
    for _, cfg in (raw.get("stablecoins", {}) or {}).items():
        events = (cfg.get("events", {}) or {})
        for ev_name, ev_cfg in events.items():
            if ev_name in ("transfer", "mint_burn_via_zero"):
                continue
            sig = ev_cfg.get("signature")
            if sig and sig not in sigs:
                sigs.append(sig)
    return sigs


# ---------------- DB schema ---------------- #

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS eth_block (
  block_number   INTEGER PRIMARY KEY,
  ts             TIMESTAMPTZ NOT NULL
);

CREATE TABLE address (
    id      SERIAL PRIMARY KEY,
    addr    BYTEA UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS eth_tx (
  block_number   INTEGER NOT NULL,
  tx_index       INTEGER NOT NULL,
  from_addr      BYTEA NOT NULL,
  to_addr        BYTEA,
  method_id      BYTEA,
  gas_price_wei  NUMERIC(78,0),   -- legacy txs
  max_fee_per_gas_wei NUMERIC(78,0),          -- EIP-1559
  max_priority_fee_per_gas_wei NUMERIC(78,0), -- EIP-1559
  PRIMARY KEY (block_number, tx_index),
  FOREIGN KEY (block_number) REFERENCES eth_block(block_number)
);

CREATE TABLE IF NOT EXISTS erc20_transfer (
  block_number   INTEGER NOT NULL,
  tx_index       INTEGER NOT NULL,
  log_index      INTEGER NOT NULL,
  token_addr     BYTEA NOT NULL,
  from_addr      BYTEA NOT NULL,
  to_addr        BYTEA NOT NULL,
  amount         NUMERIC(78,0) NOT NULL,
  PRIMARY KEY (block_number, tx_index, log_index),
  FOREIGN KEY (block_number, tx_index) REFERENCES eth_tx(block_number, tx_index)
);

CREATE TABLE IF NOT EXISTS token_event (
  block_number   INTEGER NOT NULL,
  tx_index       INTEGER NOT NULL,
  log_index      INTEGER NOT NULL,
  token_addr     BYTEA NOT NULL,
  event_type     SMALLINT NOT NULL,
  a0             BYTEA,
  a1             BYTEA,
  value          NUMERIC(78,0),
  PRIMARY KEY (block_number, tx_index, log_index),
  FOREIGN KEY (block_number, tx_index) REFERENCES eth_tx(block_number, tx_index)
);
"""

def ensure_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()


# ---------------- inserts ---------------- #

def insert_blocks_and_txs(conn, blocks):
    block_rows = []
    tx_rows = []

    for b in blocks:
        if not b:
            continue
        bn = h2i(b["number"])
        ts = h2i(b["timestamp"])
        block_rows.append((bn, ts))

        for tx in b.get("transactions", []):
            tx_index = h2i(tx["transactionIndex"])
            from_addr = hex_to_bytes20(tx["from"])
            to_addr = hex_to_bytes20(tx["to"]) if tx.get("to") else None
            method_id = method_id_from_input(tx.get("input"), to_addr)

            gas_price = safe_int_hex(tx.get("gasPrice"))
            max_fee = safe_int_hex(tx.get("maxFeePerGas"))
            max_prio = safe_int_hex(tx.get("maxPriorityFeePerGas"))

            tx_rows.append((bn, tx_index, from_addr, to_addr, method_id, gas_price, max_fee, max_prio))

    with conn.cursor() as cur:
        if block_rows:
            cur.executemany(
                """
                INSERT INTO eth_block(block_number, ts)
                VALUES (%s, to_timestamp(%s))
                ON CONFLICT (block_number) DO NOTHING
                """,
                block_rows,
            )
        if tx_rows:
            cur.executemany(
                """
                INSERT INTO eth_tx(
                  block_number, tx_index, from_addr, to_addr, method_id,
                  gas_price_wei, max_fee_per_gas_wei, max_priority_fee_per_gas_wei
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (block_number, tx_index) DO NOTHING
                """,
                tx_rows,
            )
    conn.commit()

def insert_transfer_logs(conn: psycopg.Connection, logs: List[dict]) -> None:
    rows: List[Tuple[int, int, int, bytes, bytes, bytes, int]] = []
    for lg in logs:
        bn = h2i(lg["blockNumber"])
        txi = h2i(lg["transactionIndex"])
        logi = h2i(lg["logIndex"])
        token = hex_to_bytes20(lg["address"])

        topics = lg["topics"]
        # Transfer indexed params are always topics[1], topics[2]
        from_addr = topic_to_addr(topics[1])
        to_addr = topic_to_addr(topics[2])

        amount = int(lg.get("data") or "0x0", 16)
        rows.append((bn, txi, logi, token, from_addr, to_addr, amount))

    if not rows:
        return

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO erc20_transfer
              (block_number, tx_index, log_index, token_addr, from_addr, to_addr, amount)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (block_number, tx_index, log_index) DO NOTHING
            """,
            rows,
        )
    conn.commit()

def parse_special_event_like_yours(lg: dict, topic0_to_sig: Dict[str, str]) -> Optional[Tuple[int,int,int,bytes,int,Optional[bytes],Optional[bytes],Optional[int]]]:
    """
    Inspired by your working parsing logic:
    - Blacklist events: indexed addr in topics[1] OR address in data (fallback)
    - DestroyedBlackFunds: address + uint256
    - Issue/Redeem: single uint256
    - Mint/FRAXMinted: 2 indexed addrs + uint256
    - Burn: indexed addr + uint256
    - FRAXBurned: 2 indexed addrs + uint256
    """
    topics = lg.get("topics") or []
    if not topics:
        return None

    t0 = topics[0].lower()
    sig = topic0_to_sig.get(t0)
    if not sig:
        return None

    event_type = SIG_TO_EVENTTYPE.get(sig)
    if not event_type:
        return None

    bn = h2i(lg["blockNumber"])
    txi = h2i(lg["transactionIndex"])
    logi = h2i(lg["logIndex"])
    token = hex_to_bytes20(lg["address"])
    data_hex = lg.get("data") or "0x"

    a0: Optional[bytes] = None
    a1: Optional[bytes] = None
    value: Optional[int] = None

    # --- Issue/Redeem (single uint256) ---
    if sig in ("Issue(uint256)", "Redeem(uint256)"):
        value = uint256_from_data(data_hex, 0)
        return (bn, txi, logi, token, event_type, a0, a1, value)

    # --- DestroyedBlackFunds(address,uint256) ---
    if sig == "DestroyedBlackFunds(address,uint256)":
        # In your older code you parsed address from data; but in many contracts it's indexed in topics[1].
        # We'll support BOTH:
        if len(topics) >= 2 and topics[1] and len(topics[1]) >= 66:
            a0 = topic_to_addr(topics[1])
            value = uint256_from_data(data_hex, 0)
        else:
            # fallback: address in first word of data, amount in second
            a0 = bytes.fromhex((data_hex[2:] if data_hex.startswith("0x") else data_hex)[:64][-40:])
            value = uint256_from_data(data_hex, 1)
        return (bn, txi, logi, token, event_type, a0, a1, value)

    # --- Blacklist address events (address) ---
    if sig in ("Blacklisted(address)", "UnBlacklisted(address)", "AddedBlackList(address)", "RemovedBlackList(address)"):
        # Case 1: indexed address in topics[1]
        if len(topics) >= 2 and topics[1] and len(topics[1]) >= 66:
            a0 = topic_to_addr(topics[1])
            return (bn, txi, logi, token, event_type, a0, a1, value)

        # Case 2: non-indexed address in data (your previous fallback)
        h = data_hex[2:] if data_hex.startswith("0x") else data_hex
        if len(h) >= 64:
            addr_word = h[:64]
            a0 = bytes.fromhex(addr_word[-40:])
            return (bn, txi, logi, token, event_type, a0, a1, value)

        return None

    # --- Mint(address,address,uint256) / FRAXMinted(address,address,uint256) ---
    if sig in ("Mint(address,address,uint256)", "FRAXMinted(address,address,uint256)"):
        if len(topics) >= 3:
            a0 = topic_to_addr(topics[1])
            a1 = topic_to_addr(topics[2])
        value = uint256_from_data(data_hex, 0)
        return (bn, txi, logi, token, event_type, a0, a1, value)

    # --- Burn(address,uint256) ---
    if sig == "Burn(address,uint256)":
        if len(topics) >= 2:
            a0 = topic_to_addr(topics[1])
        value = uint256_from_data(data_hex, 0)
        return (bn, txi, logi, token, event_type, a0, a1, value)

    # --- FRAXBurned(address,address,uint256) ---
    if sig == "FRAXBurned(address,address,uint256)":
        if len(topics) >= 3:
            a0 = topic_to_addr(topics[1])
            a1 = topic_to_addr(topics[2])
        value = uint256_from_data(data_hex, 0)
        return (bn, txi, logi, token, event_type, a0, a1, value)

    # Unknown signature parsing (skip)
    return None

def insert_token_events(conn: psycopg.Connection, rows: List[Tuple[int,int,int,bytes,int,Optional[bytes],Optional[bytes],Optional[int]]]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO token_event
              (block_number, tx_index, log_index, token_addr, event_type, a0, a1, value)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (block_number, tx_index, log_index) DO NOTHING
            """,
            rows,
        )
    conn.commit()


# ---------------- main ---------------- #

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rpc", default=os.getenv("ETH_RPC_URL"), help="RPC URL (HTTP)")
    ap.add_argument("--pg", default=os.getenv("PG_DSN"), help="Postgres DSN")
    ap.add_argument("--start", type=int, required=True, help="Start block (inclusive)")
    ap.add_argument("--end", type=int, default=None, help="End block (inclusive); default latest")
    ap.add_argument("--chunk", type=int, default=int(os.getenv("BLOCK_CHUNK", "5000")), help="Block range size for eth_getLogs")
    ap.add_argument("--block-batch", type=int, default=int(os.getenv("BLOCK_BATCH", "200")), help="How many blocks per JSON-RPC batch request")
    ap.add_argument("--timeout", type=int, default=int(os.getenv("RPC_TIMEOUT", "60")))
    ap.add_argument("--yaml", default=None, help="Optional: stablecoins YAML to ADD more event signatures (NO filtering)")
    args = ap.parse_args()

    if not args.rpc:
        raise SystemExit("Missing --rpc or ETH_RPC_URL")
    if not args.pg:
        raise SystemExit("Missing --pg or PG_DSN")

    rpc = RpcClient(args.rpc, timeout=args.timeout)

    latest = rpc.eth_block_number()
    end = args.end if args.end is not None else latest
    if args.start > end:
        raise SystemExit(f"--start {args.start} > --end {end}")

    # Special signatures = defaults + any discovered in YAML
    sigs = list(DEFAULT_SPECIAL_SIGS)
    for s in load_sigs_from_yaml(args.yaml):
        if s not in sigs:
            sigs.append(s)

    # topic0 -> signature mapping (computed exactly like your old code)
    topic0_to_sig: Dict[str, str] = {topic0(sig).lower(): sig for sig in sigs}
    special_topic0s = sorted(topic0_to_sig.keys())

    with psycopg.connect(args.pg) as conn:
        ensure_schema(conn)

        for chunk_start in range(args.start, end + 1, args.chunk):
            chunk_end = min(chunk_start + args.chunk - 1, end)
            print(f"Processing blocks {chunk_start} - {chunk_end}")

            # 1) Blocks + TXs (batched eth_getBlockByNumber with full tx objects)
            block_nums = list(range(chunk_start, chunk_end + 1))
            for i in range(0, len(block_nums), args.block_batch):
                batch_nums = block_nums[i:i + args.block_batch]
                blocks = rpc.eth_get_block_by_number_batch(batch_nums, full_tx=True)
                insert_blocks_and_txs(conn, blocks)

            # 2) ALL ERC20 Transfer logs in range
            try:
                transfer_logs = rpc.eth_get_logs(chunk_start, chunk_end, [TRANSFER_TOPIC0])
            except Exception as e:
                print(f"eth_getLogs (Transfer) error {chunk_start}-{chunk_end}: {e}")
                print("Tip: reduce --chunk (e.g. 1000, 500, 200) if your node returns 'too large' responses.")
                raise
            insert_transfer_logs(conn, transfer_logs)

            # 3) Special logs in range (mint/burn/blacklist)
            if special_topic0s:
                try:
                    special_logs = rpc.eth_get_logs(chunk_start, chunk_end, special_topic0s)
                except Exception as e:
                    print(f"eth_getLogs (special) error {chunk_start}-{chunk_end}: {e}")
                    print("Tip: reduce --chunk if your node returns 'too large' responses.")
                    raise

                rows: List[Tuple[int,int,int,bytes,int,Optional[bytes],Optional[bytes],Optional[int]]] = []
                for lg in special_logs:
                    row = parse_special_event_like_yours(lg, topic0_to_sig)
                    if row is not None:
                        rows.append(row)
                insert_token_events(conn, rows)


if __name__ == "__main__":
    main()