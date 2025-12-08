import asyncio
import aiohttp
import csv
import argparse
from datetime import datetime
from collections import defaultdict
from tqdm import tqdm
import config_env
import yaml
from web3 import Web3
import os


RPC_URL = config_env.get_RPC_URL()
START_BLOCK = config_env.get_START_BLOCK()
END_BLOCK = config_env.get_END_BLOCK()
BLOCK_BATCH_SIZE = config_env.get_BLOCK_BATCH_SIZE()  # number of blocks to query concurrently
CHUNK_SIZE = config_env.get_CHUNK_SIZE()  # number of blocks to process before writing to CSV


with open("config/stablecoins.yaml", "r") as f:
    STABLECOINS_CONFIG = yaml.safe_load(f)

USDT_CONFIG = STABLECOINS_CONFIG["stablecoins"]["usdt"]
USDT_ADDRESS = USDT_CONFIG["address"].lower()
USDT_DECIMALS = USDT_CONFIG["decimals"]

ZERO_ADDRESS = "0x" + "0" * 40

# Tether-specific events (topics are keccak256 of the event signatures)

TRANSFER_TOPIC = "0x" + Web3.keccak(text="Transfer(address,address,uint256)").hex()
ISSUE_EVENT_TOPIC = "0x" + Web3.keccak(text="Issue(uint256)").hex()
REDEEM_EVENT_TOPIC = "0x" + Web3.keccak(text="Redeem(uint256)").hex()
ADDED_BLACKLIST_TOPIC = "0x" + Web3.keccak(text="AddedBlackList(address)").hex()
REMOVED_BLACKLIST_TOPIC = "0x" + Web3.keccak(text="RemovedBlackList(address)").hex()
DESTROYED_BLACKFUNDS_TOPIC = "0x" + Web3.keccak(text="DestroyedBlackFunds(address,uint256)").hex()

async def rpc_call(session, method, params=None):
    if params is None:
        params = []
    async with session.post(
        RPC_URL,
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        },
    ) as resp:
        result = await resp.json()
        return result["result"]


# --------------------------------------------------------------------
# CSV helpers
# --------------------------------------------------------------------

def write_csv_chunk(filename, fieldnames, rows, is_first_chunk=False):
    if not rows:
        return

    mode = "w" if is_first_chunk else "a"
    with open(filename, mode, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if is_first_chunk:
            writer.writeheader()
        writer.writerows(rows)


def write_daily_stats(daily_stats, filename):
    fieldnames = [
        "date",
        "issued_event",          # Issue(uint256)
        "redeemed_event",        # Redeem(uint256)
        "destroyed_blackfunds",  # DestroyedBlackFunds(address,uint256)
        "from_zero_transfer",    # volume of transfers with from == 0x0
        "to_zero_transfer",      # volume of transfers with to   == 0x0
    ]
    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for date in sorted(daily_stats.keys()):
            stats = daily_stats[date]
            writer.writerow(
                {
                    "date": date,
                    "issued_event": f"{stats['issued_event']:.2f}",
                    "redeemed_event": f"{stats['redeemed_event']:.2f}",
                    "destroyed_blackfunds": f"{stats['destroyed_blackfunds']:.2f}",
                    "from_zero_transfer": f"{stats['from_zero_transfer']:.2f}",
                    "to_zero_transfer": f"{stats['to_zero_transfer']:.2f}",
                }
            )

def write_csv_chunk(filename, fieldnames, rows, is_first_chunk=False):
    if not rows:
        return

    mode = "w" if is_first_chunk else "a"
    with open(filename, mode, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if is_first_chunk:
            writer.writeheader()
        writer.writerows(rows)


def write_daily_stats(daily_stats, filename):
    """
    Daily aggregates based ONLY on:
      - Issue(uint256)
      - Redeem(uint256)
      - DestroyedBlackFunds(address,uint256)
    Transfers are NOT used here (you'll analyse them later yourself).
    """
    fieldnames = [
        "date",
        "issued_event",
        "redeemed_event",
        "destroyed_blackfunds",
    ]
    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for date in sorted(daily_stats.keys()):
            stats = daily_stats[date]
            writer.writerow(
                {
                    "date": date,
                    "issued_event": f"{stats['issued_event']:.2f}",
                    "redeemed_event": f"{stats['redeemed_event']:.2f}",
                    "destroyed_blackfunds": f"{stats['destroyed_blackfunds']:.2f}",
                }
            )


# --------------------------------------------------------------------
# Log parsers
# --------------------------------------------------------------------

def _decode_address_from_topic(topic_hex: str) -> str:
    # topics are 32 bytes, last 20 bytes = address
    return "0x" + topic_hex[-40:]


def _decode_amount_from_data(data_hex: str) -> float:
    if data_hex == "0x" or data_hex == "0x0":
        return 0.0
    amount_int = int(data_hex, 16)
    return amount_int / (10 ** USDT_DECIMALS)


def _timestamp_to_date_str(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")


def parse_usdt_transfer_log(log, tx_hash, block_number, timestamp):
    """
    
    """
    topics = log.get("topics", [])
    if len(topics) < 3:
        return None

    from_address = _decode_address_from_topic(topics[1])
    to_address = _decode_address_from_topic(topics[2])
    amount_tokens = _decode_amount_from_data(log["data"])

    return {
        "block_number": block_number,
        "timestamp": timestamp,
        "date": _timestamp_to_date_str(timestamp),
        "tx_hash": tx_hash,
        "from": from_address,
        "to": to_address,
        "amount": amount_tokens,
    }


def parse_usdt_issue_redeem_log(topic0, log, tx_hash, block_number, timestamp):
    if topic0 == ISSUE_EVENT_TOPIC:
        event_type = "issue"
    elif topic0 == REDEEM_EVENT_TOPIC:
        event_type = "redeem"
    else:
        return None

    amount_tokens = _decode_amount_from_data(log["data"])

    return {
        "block_number": block_number,
        "timestamp": timestamp,
        "date": _timestamp_to_date_str(timestamp),
        "tx_hash": tx_hash,
        "event_type": event_type,
        "amount": amount_tokens,
    }


def _decode_destroyed_blackfunds(data_hex: str):
    """
    DestroyedBlackFunds(address _blackListedUser, uint256 _balance)

    Both params are NON-indexed, layout:

       [ 32 bytes address ][ 32 bytes uint balance ]
    """
    if data_hex in ("0x", "0x0", None):
        return None, 0.0

    data = data_hex[2:]
    # pad to at least 2 words
    data = data.rjust(64 * 2, "0")

    # first word: address
    addr_word = data[:64]
    addr = "0x" + addr_word[-40:]

    # second word: uint balance
    bal_word = data[64:128]
    amount_int = int(bal_word, 16)
    amount_tokens = amount_int / (10 ** USDT_DECIMALS)

    return addr, amount_tokens


def _decode_single_address_from_data(data_hex: str):
    """
    Decode a single address from event data where the ABI is:

        event Something(address _user);

    i.e., one non-indexed address parameter -> first 32-byte word in `data`.
    """
    if data_hex in ("0x", "0x0", None):
        return None

    # strip '0x'
    data = data_hex[2:]
    # left-pad to at least one 32-byte word
    data = data.rjust(64, "0")

    # first word = 32 bytes = 64 hex chars
    addr_word = data[:64]
    # address is last 20 bytes of that word
    return "0x" + addr_word[-40:]


def parse_usdt_blacklist_log(topic0, log, tx_hash, block_number, timestamp):
    data_hex = log.get("data", "0x")

    # Defaults
    user_address = None
    amount_tokens = 0.0

    if topic0 == ADDED_BLACKLIST_TOPIC:
        # event AddedBlackList(address _user)
        event_type = "addedToBlacklist"  # use the label you like
        user_address = _decode_single_address_from_data(data_hex)

    elif topic0 == REMOVED_BLACKLIST_TOPIC:
        # event RemovedBlackList(address _user)
        event_type = "removedFromBlacklist"
        user_address = _decode_single_address_from_data(data_hex)

    elif topic0 == DESTROYED_BLACKFUNDS_TOPIC:
        # event DestroyedBlackFunds(address _blackListedUser, uint _balance)
        event_type = "destroyedBlackFunds"
        user_address, amount_tokens = _decode_destroyed_blackfunds(data_hex)

    else:
        return None

    return {
        "block_number": block_number,
        "timestamp": timestamp,
        "date": _timestamp_to_date_str(timestamp),
        "tx_hash": tx_hash,
        "event_type": event_type,
        "address": user_address,
        "amount": amount_tokens,
    }


# --------------------------------------------------------------------
# Block processing
# --------------------------------------------------------------------

async def process_block(session, block_number):
    """
    Process a single block and extract all USDT-related events:
    - All Transfers
    - All Issue / Redeem events
    - All blacklist events
    """
    block = await rpc_call(
        session,
        "eth_getBlockByNumber",
        [hex(block_number), True],
    )
    if block is None:
        return [], [], []

    timestamp = int(block["timestamp"], 16)

    receipts = await rpc_call(
        session,
        "eth_getBlockReceipts",
        [hex(block_number)],
    )
    if receipts is None:
        return [], [], []

    transfers = []
    issue_redeem_events = []
    blacklist_events = []

    for receipt in receipts:
        if receipt is None or "logs" not in receipt:
            continue

        tx_hash = receipt["transactionHash"]

        for log in receipt["logs"]:
            log_address = log.get("address", "").lower()
            if log_address != USDT_ADDRESS:
                continue

            topics = log.get("topics", [])
            if not topics:
                continue

            topic0 = topics[0].lower()

            # All Transfers
            if topic0 == TRANSFER_TOPIC:
                transfer = parse_usdt_transfer_log(
                    log, tx_hash, block_number, timestamp
                )
                if transfer:
                    transfers.append(transfer)
                continue

            # Issue / Redeem
            if topic0 in (ISSUE_EVENT_TOPIC, REDEEM_EVENT_TOPIC):
                evt = parse_usdt_issue_redeem_log(
                    topic0, log, tx_hash, block_number, timestamp
                )
                if evt:
                    issue_redeem_events.append(evt)
                continue

            # Blacklist-related
            if topic0 in (
                ADDED_BLACKLIST_TOPIC,
                REMOVED_BLACKLIST_TOPIC,
                DESTROYED_BLACKFUNDS_TOPIC,
            ):
                evt = parse_usdt_blacklist_log(
                    topic0, log, tx_hash, block_number, timestamp
                )
                if evt:
                    blacklist_events.append(evt)
                continue

    return transfers, issue_redeem_events, blacklist_events


# --------------------------------------------------------------------
# Main
# --------------------------------------------------------------------

async def main():
    print("=" * 70)
    print("USDT DETAIL ANALYSIS (Ethereum)")
    print("=" * 70)
    print(f"RPC URL:        {RPC_URL}")
    print(f"USDT address:   {USDT_ADDRESS}")
    print(f"Block range:    {START_BLOCK} – {END_BLOCK}")
    print(f"Batch size:     {BLOCK_BATCH_SIZE}")
    print(f"Chunk size:     {CHUNK_SIZE}")
    print("=" * 70)

    transfers_file = "usdt_transfers_detailed.csv"
    blacklist_file = "usdt_blacklist_events.csv"
    issue_redeem_file = "usdt_issue_redeem_events.csv"
    daily_stats_file = "usdt_daily_issuance_redemption.csv"

    # Remove old files if present
    for fname in [
        transfers_file,
        blacklist_file,
        issue_redeem_file,
        daily_stats_file,
    ]:
        if os.path.exists(fname):
            os.remove(fname)

    # Track whether we've written headers yet
    first_transfers_chunk = True
    first_blacklist_chunk = True
    first_issue_redeem_chunk = True

    # Daily aggregates FROM EVENTS ONLY
    daily_stats = defaultdict(
        lambda: {
            "issued_event": 0.0,          # from Issue(uint256)
            "redeemed_event": 0.0,        # from Redeem(uint256)
            "destroyed_blackfunds": 0.0,  # from DestroyedBlackFunds
        }
    )

    total_blocks_processed = 0
    total_transfers = 0
    total_issues = 0
    total_redeems = 0
    total_blacklist_events = 0

    transfers_fields = [
        "block_number",
        "timestamp",
        "date",
        "tx_hash",
        "from",
        "to",
        "amount",
    ]
    blacklist_fields = [
        "block_number",
        "timestamp",
        "date",
        "tx_hash",
        "event_type",
        "address",
        "amount",
    ]
    issue_redeem_fields = [
        "block_number",
        "timestamp",
        "date",
        "tx_hash",
        "event_type",
        "amount",
    ]

    async with aiohttp.ClientSession() as session:
        chunk_count = 0

        for chunk_start in range(START_BLOCK, END_BLOCK + 1, CHUNK_SIZE):
            chunk_end = min(chunk_start + CHUNK_SIZE - 1, END_BLOCK)
            chunk_count += 1

            print(f"\nProcessing chunk {chunk_count}: blocks {chunk_start}-{chunk_end}")

            chunk_transfers = []
            chunk_blacklist = []
            chunk_issue_redeem = []

            for batch_start in range(chunk_start, chunk_end + 1, BLOCK_BATCH_SIZE):
                batch_end = min(batch_start + BLOCK_BATCH_SIZE - 1, chunk_end)

                tasks = [
                    process_block(session, b)
                    for b in range(batch_start, batch_end + 1)
                ]
                results = await asyncio.gather(*tasks)
                total_blocks_processed += len(results)

                for transfers, issue_redeem, blacklist in tqdm(
                    results,
                    total=len(results),
                    desc=f"Batch {batch_start}-{batch_end}",
                ):
                    # Aggregate detailed rows
                    chunk_transfers.extend(transfers)
                    chunk_issue_redeem.extend(issue_redeem)
                    chunk_blacklist.extend(blacklist)

                    # Daily stats ONLY from Issue/Redeem/DestroyedBlackFunds
                    for e in issue_redeem:
                        date = e["date"]
                        if e["event_type"] == "issue":
                            daily_stats[date]["issued_event"] += e["amount"]
                        elif e["event_type"] == "redeem":
                            daily_stats[date]["redeemed_event"] += e["amount"]

                    for b_evt in blacklist:
                        date = b_evt["date"]
                        if b_evt["event_type"] == "destroyed":
                            daily_stats[date]["destroyed_blackfunds"] += b_evt["amount"]

            # Write chunk CSVs
            if chunk_transfers:
                write_csv_chunk(
                    transfers_file,
                    transfers_fields,
                    chunk_transfers,
                    is_first_chunk=first_transfers_chunk,
                )
                first_transfers_chunk = False
                total_transfers += len(chunk_transfers)
                print(f"  ✓ Transfers: {len(chunk_transfers)} rows")

            if chunk_blacklist:
                write_csv_chunk(
                    blacklist_file,
                    blacklist_fields,
                    chunk_blacklist,
                    is_first_chunk=first_blacklist_chunk,
                )
                first_blacklist_chunk = False
                total_blacklist_events += len(chunk_blacklist)
                print(f"  ✓ Blacklist events: {len(chunk_blacklist)} rows")

            if chunk_issue_redeem:
                write_csv_chunk(
                    issue_redeem_file,
                    issue_redeem_fields,
                    chunk_issue_redeem,
                    is_first_chunk=first_issue_redeem_chunk,
                )
                first_issue_redeem_chunk = False
                for e in chunk_issue_redeem:
                    if e["event_type"] == "issue":
                        total_issues += 1
                    elif e["event_type"] == "redeem":
                        total_redeems += 1
                print(f"  ✓ Issue/Redeem events: {len(chunk_issue_redeem)} rows")

            print(f"✓ Chunk {chunk_count} completed.")

    # Write daily stats (from events only)
    print("\nWriting daily issuance/redemption stats (from events only)…")
    write_daily_stats(daily_stats, daily_stats_file)
    print(f"  ✓ Daily stats written to {daily_stats_file}")

    # Final summary
    print("\n" + "=" * 70)
    print("USDT DETAIL SUMMARY")
    print("=" * 70)
    print(f"Blocks processed:         {total_blocks_processed}")
    print(f"Total USDT transfers:     {total_transfers}")
    print(f"Issue events (Issue):     {total_issues}")
    print(f"Redeem events (Redeem):   {total_redeems}")
    print(f"Blacklist events (all):   {total_blacklist_events}")
    print()
    print(f"Transfers CSV:            {transfers_file}")
    print(f"Blacklist events CSV:     {blacklist_file}")
    print(f"Issue/Redeem CSV:         {issue_redeem_file}")
    print(f"Daily stats CSV:          {daily_stats_file}")
    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())