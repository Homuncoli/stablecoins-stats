#!/usr/bin/env python3
import csv
import json
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

import requests
import yaml
from eth_utils import keccak, to_checksum_address


# ---------------- RPC CLIENT (raw JSON-RPC) ---------------- #

class RpcClient:
    def __init__(self, url: str):
        self.url = url
        self.session = requests.Session()
        self._id = 0

    def _call(self, method: str, params):
        self._id += 1
        payload = {"jsonrpc": "2.0", "id": self._id, "method": method, "params": params}
        resp = self.session.post(self.url, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise RuntimeError(f"RPC error {data['error']}")
        return data["result"]

    def eth_get_logs(self, from_block: int, to_block: int, addresses):
        params = [{
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
            "address": addresses,
        }]
        return self._call("eth_getLogs", params)

    def eth_get_block(self, block_number: int):
        return self._call("eth_getBlockByNumber", [hex(block_number), False])

    def eth_get_code(self, address: str):
        return self._call("eth_getCode", [address, "latest"])


# ---------------- ADDRESS CACHE (SQLite) ---------------- #

class AddressTypeCache:
    """
    SQLite-backed cache for address types: "eoa" / "contract".
    Avoids huge JSON files and avoids loading everything into RAM.
    """

    def __init__(self, db_path: str = "address_types.sqlite"):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self._init_db()

    def _init_db(self):
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS address_types (
                address TEXT PRIMARY KEY,
                addr_type TEXT NOT NULL
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_addr_type ON address_types(addr_type)")
        self.conn.commit()

    def get_many(self, addresses):
        """
        Returns dict: checksum_address -> addr_type for all found in DB.
        """
        if not addresses:
            return {}
        # SQLite has a parameter limit, chunk the query
        out = {}
        addr_list = list(addresses)
        CHUNK = 800  # safe
        cur = self.conn.cursor()
        for i in range(0, len(addr_list), CHUNK):
            chunk = addr_list[i:i + CHUNK]
            placeholders = ",".join(["?"] * len(chunk))
            cur.execute(f"SELECT address, addr_type FROM address_types WHERE address IN ({placeholders})", chunk)
            out.update({row[0]: row[1] for row in cur.fetchall()})
        return out

    def set_many(self, mapping: dict):
        """
        mapping: checksum_address -> "eoa"/"contract"
        """
        if not mapping:
            return
        cur = self.conn.cursor()
        cur.executemany(
            "INSERT OR REPLACE INTO address_types(address, addr_type) VALUES (?, ?)",
            [(a, t) for a, t in mapping.items()]
        )
        self.conn.commit()

    def close(self):
        try:
            self.conn.commit()
        finally:
            self.conn.close()


# ---------------- ADDRESS CLASSIFIER (EOA vs CONTRACT) ---------------- #

class AddressClassifier:
    def __init__(self, rpc: RpcClient, cache_db_path: str = "address_types.sqlite"):
        self.rpc = rpc
        self.cache_db = AddressTypeCache(cache_db_path)

    def close(self):
        self.cache_db.close()

    def classify_many(self, addresses, batch_size: int = 250):
        """
        Classify addresses via eth_getCode using JSON-RPC batch calls,
        split into smaller batches to avoid HTTP 413 (request too large).

        Steps:
        1) Normalize & checksum addresses
        2) Query SQLite cache for known ones
        3) Batch-call eth_getCode only for unknown ones
        4) Write results back to SQLite
        """
        # Normalize + checksum + dedupe
        normalized = []
        seen = set()
        for a in addresses:
            if not a:
                continue
            try:
                addr = to_checksum_address(a)
            except Exception:
                continue
            if addr in seen:
                continue
            seen.add(addr)
            normalized.append(addr)

        if not normalized:
            return

        # Check cache first
        cached = self.cache_db.get_many(normalized)
        to_query = [a for a in normalized if a not in cached]
        if not to_query:
            return

        newly_classified = {}

        # Send multiple smaller batches to avoid 413
        for start in range(0, len(to_query), batch_size):
            chunk = to_query[start:start + batch_size]

            payload = [
                {"jsonrpc": "2.0", "id": i, "method": "eth_getCode", "params": [addr, "latest"]}
                for i, addr in enumerate(chunk)
            ]

            resp = self.rpc.session.post(self.rpc.url, json=payload, timeout=60)
            # If still too big, reduce batch_size
            if resp.status_code == 413:
                raise RuntimeError(
                    f"HTTP 413 (Request Entity Too Large) even with batch_size={batch_size}. "
                    f"Try batch_size=150 or 100."
                )
            resp.raise_for_status()
            results = resp.json()

            code_by_id = {}
            for item in results:
                if "error" in item:
                    code_by_id[item["id"]] = "0x"
                else:
                    code_by_id[item["id"]] = item.get("result", "0x")

            for i, addr in enumerate(chunk):
                code = code_by_id.get(i, "0x")
                newly_classified[addr] = "contract" if code and code != "0x" else "eoa"

        # Persist to SQLite
        self.cache_db.set_many(newly_classified)


# ---------------- CONFIG / EVENT MAPPING ---------------- #

@dataclass
class EventConfig:
    name: str
    type: str
    topic0: str
    signature: str


ZERO_ADDR = "0x0000000000000000000000000000000000000000"


def load_env_config(coin_keys):
    load_dotenv()

    cfg = {}
    cfg["rpc_url"] = os.getenv("RPC_URL", "http://localhost:8545")
    cfg["start_block"] = int(os.getenv("START_BLOCK"))
    cfg["end_block"] = int(os.getenv("END_BLOCK"))
    cfg["chunk_size"] = int(os.getenv("CHUNK_SIZE", 5000))

    coin_env = os.getenv("COINS", "")
    cfg["coin_keys"] = [c.strip() for c in coin_env.split(",")] if coin_env else coin_keys

    event_env = os.getenv("EVENTS", "")
    cfg["categories"] = {e.strip() for e in event_env.split(",")} if event_env else {"transfers", "issuance", "blacklist"}

    cfg["out_root"] = os.getenv("OUT_ROOT", "out")
    cfg["code_batch_size"] = int(os.getenv("CODE_BATCH_SIZE", "250"))  # new knob

    return cfg


def load_stablecoins_config(path: str):
    with open(path) as f:
        cfg = yaml.safe_load(f)["stablecoins"]

    stablecoins_by_addr = {}
    coins_meta = {}

    for key, info in cfg.items():
        addr = to_checksum_address(info["address"])
        decimals = info["decimals"]
        symbol = info["symbol"]

        events_cfg = info.get("events", {})
        events_by_topic = {}
        mint_burn_via_zero = False

        for ev_name, ev_info in events_cfg.items():
            if ev_name == "mint_burn_via_zero":
                mint_burn_via_zero = ev_info.get("enabled", False)
                continue

            sig = ev_info["signature"]
            topic0 = "0x" + keccak(text=sig).hex()
            events_by_topic[topic0.lower()] = EventConfig(
                name=ev_name,
                type=ev_info["type"],
                topic0=topic0.lower(),
                signature=sig,
            )

        stablecoins_by_addr[addr] = {
            "key": key,
            "symbol": symbol,
            "decimals": decimals,
            "events": events_by_topic,
            "mint_burn_via_zero": mint_burn_via_zero,
        }
        coins_meta[key] = {"address": addr, "symbol": symbol, "decimals": decimals}

    return stablecoins_by_addr, coins_meta


# ---------------- LOG PARSING HELPERS ---------------- #

def parse_transfer_log(log, coin_cfg):
    topics = log["topics"]
    data = log["data"]

    from_addr = "0x" + topics[1][-40:]
    to_addr = "0x" + topics[2][-40:]

    value_raw = int(data, 16)
    amount = value_raw / (10 ** coin_cfg["decimals"])

    if coin_cfg["mint_burn_via_zero"]:
        if from_addr.lower() == ZERO_ADDR:
            event_type = "mint"
        elif to_addr.lower() == ZERO_ADDR:
            event_type = "burn"
        else:
            event_type = "transfer"
    else:
        event_type = "transfer"

    return {"event_type": event_type, "from": from_addr, "to": to_addr, "amount": amount}


def parse_single_uint_event(log, coin_cfg, event_type: str):
    value_raw = int(log["data"], 16)
    amount = value_raw / (10 ** coin_cfg["decimals"])
    return {"event_type": event_type, "amount": amount}


def parse_blacklist_event(log, event_type: str):
    data = log["data"]
    if data.startswith("0x"):
        data = data[2:]
    if len(data) < 64:
        raise ValueError(f"Blacklist event data too short: {len(data)} hex chars")
    addr_word = data[:64]
    addr = "0x" + addr_word[-40:]
    return {"event_type": event_type, "address": addr}


def parse_destroyed_black_funds(log, coin_cfg):
    data = log["data"]
    if data.startswith("0x"):
        data = data[2:]
    if len(data) < 128:
        raise ValueError(f"DestroyedBlackFunds data too short: {len(data)} hex chars")

    addr_word = data[:64]
    amount_word = data[64:128]

    addr = "0x" + addr_word[-40:]
    value_raw = int(amount_word, 16)
    amount = value_raw / (10 ** coin_cfg["decimals"])

    return {"event_type": "destroyedBlackFunds", "address": addr, "amount": amount}


def event_category(event_type: str) -> str:
    if event_type == "transfer":
        return "transfers"
    if event_type in ("mint", "burn", "issue", "redeem"):
        return "issuance"
    if event_type in ("addedToBlacklist", "removedFromBlacklist", "destroyedBlackFunds"):
        return "blacklist"
    return "other"


# ---------------- MAIN SCRAPER ---------------- #



def main():
    
    stablecoins_by_addr, coins_meta = load_stablecoins_config("config/stablecoins_detailed.yaml")
    coin_keys = list(coins_meta.keys())
    env = load_env_config(coin_keys)

    rpc_url = env["rpc_url"]
    start_block = env["start_block"]
    end_block = env["end_block"]
    chunk_size = env["chunk_size"]
    selected_coin_keys = env["coin_keys"]
    selected_categories = env["categories"]
    out_root = Path(env["out_root"])
    code_batch_size = env["code_batch_size"]

    print("=== Loaded .env configuration ===")
    print(f"RPC URL: {rpc_url}")
    print(f"Blocks: {start_block} - {end_block}")
    print(f"Chunk size: {chunk_size}")
    print(f"Stablecoins: {selected_coin_keys}")
    print(f"Event categories: {selected_categories}")
    print(f"Output root: {out_root.resolve()}")
    print(f"CODE_BATCH_SIZE: {code_batch_size}")

    rpc = RpcClient(rpc_url)
    classifier = AddressClassifier(rpc, cache_db_path="address_types.sqlite")

    # Build mapping for selected coins
    addr_to_coin = {}
    selected_addrs = []
    for key in selected_coin_keys:
        meta = coins_meta[key]
        addr = meta["address"]
        coin_cfg = stablecoins_by_addr[addr]
        addr_to_coin[addr] = coin_cfg
        selected_addrs.append(addr)

    # Output CSVs: out/<symbol>/transfers.csv etc.
    TRANSFER_FIELDS = ["block_number", "timestamp", "tx_hash", "event_type", "from", "to", "from_type", "to_type", "amount"]
    ISSUANCE_FIELDS = ["block_number", "timestamp", "tx_hash", "event_type", "amount"]
    BLACKLIST_FIELDS = ["block_number", "timestamp", "tx_hash", "event_type", "address", "amount"]

    def _open_writer(path: Path, fieldnames):
        path.parent.mkdir(parents=True, exist_ok=True)
        f = path.open("w", newline="")
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        return f, w

    out_files = {}
    writers = {}
    for key in selected_coin_keys:
        symbol = coins_meta[key]["symbol"]
        coin_dir = out_root / symbol.lower()
        out_files[key] = {}
        writers[key] = {}

        f, w = _open_writer(coin_dir / "transfers.csv", TRANSFER_FIELDS)
        out_files[key]["transfers"] = f
        writers[key]["transfers"] = w

        f, w = _open_writer(coin_dir / "issuance.csv", ISSUANCE_FIELDS)
        out_files[key]["issuance"] = f
        writers[key]["issuance"] = w

        f, w = _open_writer(coin_dir / "blacklist.csv", BLACKLIST_FIELDS)
        out_files[key]["blacklist"] = f
        writers[key]["blacklist"] = w

    block_ts_cache = {}

    try:
        for chunk_start in range(start_block, end_block + 1, chunk_size):
            chunk_end = min(chunk_start + chunk_size - 1, end_block)
            print(f"Processing blocks {chunk_start}–{chunk_end}…")

            try:
                logs = rpc.eth_get_logs(chunk_start, chunk_end, selected_addrs)
            except Exception as e:
                print(f"eth_getLogs error {chunk_start}-{chunk_end}: {e}")
                continue

            print(f"  Retrieved {len(logs)} logs")

            for log in logs:
                log["address"] = to_checksum_address(log["address"])
                log["topics"] = [t.lower() for t in log["topics"]]

            addresses_in_chunk = set()
            rows_by_coin_key = {k: [] for k in selected_coin_keys}

            for log in logs:
                addr = log["address"]
                coin_cfg = addr_to_coin.get(addr)
                if not coin_cfg:
                    continue

                topic0 = log["topics"][0].lower()
                ev_cfg = coin_cfg["events"].get(topic0)
                if not ev_cfg:
                    continue

                block_number = int(log["blockNumber"], 16)
                if block_number in block_ts_cache:
                    ts = block_ts_cache[block_number]
                else:
                    block = rpc.eth_get_block(block_number)
                    ts = int(block["timestamp"], 16)
                    block_ts_cache[block_number] = ts

                tx_hash = log["transactionHash"]

                base = {
                    "block_number": block_number,
                    "timestamp": ts,
                    "tx_hash": tx_hash,
                    "event_type": None,
                    "from": None,
                    "to": None,
                    "from_type": None,
                    "to_type": None,
                    "address": None,
                    "amount": None,
                }

                if ev_cfg.type == "transfer":
                    parsed = parse_transfer_log(log, coin_cfg)
                    base.update(parsed)
                    if parsed.get("from"):
                        addresses_in_chunk.add(parsed["from"])
                    if parsed.get("to"):
                        addresses_in_chunk.add(parsed["to"])

                elif ev_cfg.type in ("issue", "redeem", "mint", "burn"):
                    _toggle = parse_single_uint_event(log, coin_cfg, ev_cfg.type)
                    base.update(_toggle)

                elif ev_cfg.type in ("addedToBlacklist", "removedFromBlacklist"):
                    base.update(parse_blacklist_event(log, ev_cfg.type))

                elif ev_cfg.type == "destroyedBlackFunds":
                    base.update(parse_destroyed_black_funds(log, coin_cfg))

                else:
                    continue

                cat = event_category(base["event_type"])
                if cat not in selected_categories:
                    continue

                rows_by_coin_key[coin_cfg["key"]].append(base)

            # Batch classify transfer endpoints (chunked to avoid 413)
            try:
                classifier.classify_many(addresses_in_chunk, batch_size=code_batch_size)
            except RuntimeError as e:
                print(f"[classify_many] {e}")
                print("Tip: set CODE_BATCH_SIZE=150 (or 100) in your .env and rerun.")
                raise

            # Write rows to per-category files
            for coin_key, rows in rows_by_coin_key.items():
                for r in rows:
                    cat = event_category(r["event_type"])
                    if cat == "transfers":
                        # Lookup types from SQLite cache (should now be present)
                        if r.get("from"):
                            try:
                                r["from_type"] = classifier.cache_db.get_many([to_checksum_address(r["from"])]).get(
                                    to_checksum_address(r["from"])
                                )
                            except Exception:
                                r["from_type"] = None
                        if r.get("to"):
                            try:
                                r["to_type"] = classifier.cache_db.get_many([to_checksum_address(r["to"])]).get(
                                    to_checksum_address(r["to"])
                                )
                            except Exception:
                                r["to_type"] = None

                        writers[coin_key]["transfers"].writerow({k: r.get(k) for k in TRANSFER_FIELDS})

                    elif cat == "issuance":
                        writers[coin_key]["issuance"].writerow({k: r.get(k) for k in ISSUANCE_FIELDS})

                    elif cat == "blacklist":
                        writers[coin_key]["blacklist"].writerow({k: r.get(k) for k in BLACKLIST_FIELDS})

    finally:
        for coin_key in out_files:
            for f in out_files[coin_key].values():
                f.close()
        classifier.close()


if __name__ == "__main__":
    main()
