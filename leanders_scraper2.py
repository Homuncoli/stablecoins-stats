#!/usr/bin/env python3
import csv
import json
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv
import os

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
        payload = {
            "jsonrpc": "2.0",
            "id": self._id,
            "method": method,
            "params": params,
        }
        resp = self.session.post(self.url, json=payload)
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


# ---------------- ADDRESS CLASSIFIER (EOA vs CONTRACT) ---------------- #

class AddressClassifier:
    def __init__(self, rpc: RpcClient, cache_path: str = "address_types.json"):
        self.rpc = rpc
        self.cache_path = Path(cache_path)
        if self.cache_path.exists():
            with self.cache_path.open() as f:
                self.cache = json.load(f)
        else:
            self.cache = {}  # addr -> "eoa" / "contract"

    def classify(self, address: str) -> str:
        """Single-address classification (fallback / rarely used)."""
        if not address:
            return "unknown"
        addr = to_checksum_address(address)
        if addr in self.cache:
            return self.cache[addr]
        code = self.rpc.eth_get_code(addr)
        addr_type = "contract" if code and code != "0x" else "eoa"
        self.cache[addr] = addr_type
        return addr_type

    def classify_many(self, addresses) -> None:
        """
        Classify a set/list of addresses using a single JSON-RPC batch call.
        Populates self.cache in-place. Addresses already in cache are skipped.
        """
        normalized = []
        for a in addresses:
            if not a:
                continue
            try:
                normalized.append(to_checksum_address(a))
            except Exception:
                continue

        to_query = [a for a in normalized if a not in self.cache]
        if not to_query:
            return

        payload = []
        for i, addr in enumerate(to_query):
            payload.append({
                "jsonrpc": "2.0",
                "id": i,
                "method": "eth_getCode",
                "params": [addr, "latest"],
            })

        resp = self.rpc.session.post(self.rpc.url, json=payload)
        resp.raise_for_status()
        results = resp.json()

        code_by_id = {}
        for item in results:
            if "error" in item:
                code_by_id[item["id"]] = "0x"
            else:
                code_by_id[item["id"]] = item.get("result", "0x")

        for i, addr in enumerate(to_query):
            code = code_by_id.get(i, "0x")
            addr_type = "contract" if code and code != "0x" else "eoa"
            self.cache[addr] = addr_type

    def save(self):
        with self.cache_path.open("w") as f:
            json.dump(self.cache, f)


# ---------------- CONFIG / EVENT MAPPING ---------------- #

@dataclass
class EventConfig:
    name: str        # yaml key: "transfer", "mint", ...
    type: str        # semantic type: "transfer", "issue", "redeem", ...
    topic0: str      # keccak hash of signature
    signature: str


ZERO_ADDR = "0x0000000000000000000000000000000000000000"


def load_env_config(coin_keys):
    """
    Load scraper configuration from .env file.
    """
    load_dotenv()

    cfg = {}
    cfg["rpc_url"] = os.getenv("RPC_URL", "http://localhost:8545")
    cfg["start_block"] = int(os.getenv("START_BLOCK"))
    cfg["end_block"] = int(os.getenv("END_BLOCK"))
    cfg["chunk_size"] = int(os.getenv("CHUNK_SIZE", 5000))

    # COINS=usdt,dai,usdc
    coin_env = os.getenv("COINS", "")
    if coin_env:
        cfg["coin_keys"] = [c.strip() for c in coin_env.split(",")]
    else:
        cfg["coin_keys"] = coin_keys

    # EVENTS=transfers,issuance,blacklist
    event_env = os.getenv("EVENTS", "")
    if event_env:
        cfg["categories"] = {e.strip() for e in event_env.split(",")}
    else:
        cfg["categories"] = {"transfers", "issuance", "blacklist"}

    # Optional: output root folder
    cfg["out_root"] = os.getenv("OUT_ROOT", "out")

    return cfg


def load_stablecoins_config(path: str):
    with open(path) as f:
        cfg = yaml.safe_load(f)["stablecoins"]

    stablecoins_by_addr = {}   # checksum address -> coin_cfg
    coins_meta = {}            # key -> meta info (symbol, etc.)

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
        coins_meta[key] = {
            "address": addr,
            "symbol": symbol,
            "decimals": decimals,
        }

    return stablecoins_by_addr, coins_meta


# ---------------- LOG PARSING HELPERS ---------------- #

def parse_transfer_log(log, coin_cfg):
    topics = log["topics"]
    data = log["data"]

    from_addr = "0x" + topics[1][-40:]
    to_addr   = "0x" + topics[2][-40:]

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

    return {
        "event_type": event_type,
        "from": from_addr,
        "to": to_addr,
        "amount": amount,
    }


def parse_single_uint_event(log, coin_cfg, event_type: str):
    data = log["data"]
    value_raw = int(data, 16)
    amount = value_raw / (10 ** coin_cfg["decimals"])
    return {
        "event_type": event_type,
        "amount": amount,
    }


def parse_blacklist_event(log, event_type: str):
    data = log["data"]
    if data.startswith("0x"):
        data = data[2:]

    if len(data) < 64:
        raise ValueError(f"Blacklist event data too short: {len(data)} hex chars")

    addr_word = data[:64]
    addr = "0x" + addr_word[-40:]

    return {
        "event_type": event_type,
        "address": addr,
    }


def parse_destroyed_black_funds(log, coin_cfg):
    data = log["data"]
    if data.startswith("0x"):
        data = data[2:]

    if len(data) < 128:
        raise ValueError(f"DestroyedBlackFunds data too short: {len(data)} hex chars")

    addr_word   = data[:64]
    amount_word = data[64:128]

    addr = "0x" + addr_word[-40:]
    value_raw = int(amount_word, 16)
    amount = value_raw / (10 ** coin_cfg["decimals"])

    return {
        "event_type": "destroyedBlackFunds",
        "address": addr,
        "amount": amount,
    }


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

    print("=== Loaded .env configuration ===")
    print(f"RPC URL: {rpc_url}")
    print(f"Blocks: {start_block} → {end_block}")
    print(f"Chunk size: {chunk_size}")
    print(f"Stablecoins: {selected_coin_keys}")
    print(f"Event categories: {selected_categories}")
    print(f"Output root: {out_root.resolve()}")

    rpc = RpcClient(rpc_url)
    classifier = AddressClassifier(rpc)

    # Build mapping for selected coins
    addr_to_coin = {}
    selected_addrs = []
    for key in selected_coin_keys:
        meta = coins_meta[key]
        addr = meta["address"]
        coin_cfg = stablecoins_by_addr[addr]
        addr_to_coin[addr] = coin_cfg
        selected_addrs.append(addr)

    # ---------------- OUTPUT: per-coin dir + per-category CSVs ---------------- #

    TRANSFER_FIELDS = [
        "block_number", "timestamp", "tx_hash",
        "event_type",
        "from", "to", "from_type", "to_type",
        "amount",
    ]

    ISSUANCE_FIELDS = [
        "block_number", "timestamp", "tx_hash",
        "event_type",
        "amount",
    ]

    BLACKLIST_FIELDS = [
        "block_number", "timestamp", "tx_hash",
        "event_type",
        "address",
        "amount",   # only for destroyedBlackFunds; blank otherwise
    ]

    def _open_writer(path: Path, fieldnames):
        path.parent.mkdir(parents=True, exist_ok=True)
        f = path.open("w", newline="")
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        return f, w

    out_files = {}   # coin_key -> {category -> filehandle}
    writers = {}     # coin_key -> {category -> DictWriter}

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

    # Cache block timestamps to reduce RPC calls
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
                    parsed = parse_single_uint_event(log, coin_cfg, ev_cfg.type)
                    base.update(parsed)

                elif ev_cfg.type in ("addedToBlacklist", "removedFromBlacklist"):
                    parsed = parse_blacklist_event(log, ev_cfg.type)
                    base.update(parsed)

                elif ev_cfg.type == "destroyedBlackFunds":
                    parsed = parse_destroyed_black_funds(log, coin_cfg)
                    base.update(parsed)

                else:
                    continue

                cat = event_category(base["event_type"])
                if cat not in selected_categories:
                    continue

                coin_key = coin_cfg["key"]
                rows_by_coin_key[coin_key].append(base)

            # Batch classify only transfer endpoints
            classifier.classify_many(addresses_in_chunk)

            # Write rows into the correct per-coin / per-category CSV
            for coin_key, rows in rows_by_coin_key.items():
                for r in rows:
                    cat = event_category(r["event_type"])
                    if cat not in selected_categories:
                        continue

                    if cat == "transfers":
                        if r.get("from"):
                            try:
                                r["from_type"] = classifier.cache.get(to_checksum_address(r["from"]))
                            except Exception:
                                r["from_type"] = None
                        if r.get("to"):
                            try:
                                r["to_type"] = classifier.cache.get(to_checksum_address(r["to"]))
                            except Exception:
                                r["to_type"] = None

                        writers[coin_key]["transfers"].writerow({k: r.get(k) for k in TRANSFER_FIELDS})

                    elif cat == "issuance":
                        writers[coin_key]["issuance"].writerow({k: r.get(k) for k in ISSUANCE_FIELDS})

                    elif cat == "blacklist":
                        writers[coin_key]["blacklist"].writerow({k: r.get(k) for k in BLACKLIST_FIELDS})

            classifier.save()

    finally:
        for coin_key in out_files:
            for f in out_files[coin_key].values():
                f.close()
        classifier.save()


if __name__ == "__main__":
    main()
