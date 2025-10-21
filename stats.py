import asyncio
import aiohttp
import csv
from datetime import datetime
from collections import defaultdict
from tqdm import tqdm

# --- Configuration ---
RPC_URL = "http://10.9.0.35:8545"  # ethereum2
START_BLOCK = 20322000   # July 16th 2025
END_BLOCK = 20344500  # July 20th 2025
BLOCK_BATCH_SIZE = 100  # number of blocks to query concurrently

# USDC contract address (Ethereum mainnet)
USDC_ADDRESS = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".lower()

# ERC20 Transfer event signature:
# Transfer(address indexed from, address indexed to, uint256 value)
TRANSFER_TOPIC = (
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)

# Output files
OUTPUT_FILE = "usdc_transfers.csv"
DAILY_STATS_FILE = "usdc_daily_stats.csv"


async def rpc_call(session, method, params=None):
    if params is None:
        params = []
    async with session.post(RPC_URL, json={
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params
    }) as resp:
        result = await resp.json()
        return result["result"]


def parse_transfer_log(log, tx_hash, block_number, timestamp):
    """Parse a USDC Transfer event log"""
    # topics[0] is the event signature
    # topics[1] is the 'from' address
    # topics[2] is the 'to' address
    # data contains the amount

    if len(log["topics"]) < 3:
        return None

    from_address = "0x" + log["topics"][1][-40:]  # last 40 chars
    to_address = "0x" + log["topics"][2][-40:]
    amount_hex = log["data"]
    amount = int(amount_hex, 16) if amount_hex != "0x" else 0

    # USDC has 6 decimals
    amount_usdc = amount / 1e6

    return {
        "block_number": block_number,
        "timestamp": timestamp,
        "tx_hash": tx_hash,
        "from": from_address,
        "to": to_address,
        "amount": amount_usdc
    }


async def process_block(session, block_number):
    """Process a single block and extract USDC transfers"""
    block = await rpc_call(
        session, "eth_getBlockByNumber", [hex(block_number), True]
    )

    if block is None:
        return []

    timestamp = int(block["timestamp"], 16)

    # fetch all receipts for the block in one call
    receipts = await rpc_call(
        session, "eth_getBlockReceipts", [hex(block_number)]
    )

    if receipts is None:
        return []

    transfers = []

    for receipt in receipts:
        if receipt is None or "logs" not in receipt:
            continue

        tx_hash = receipt["transactionHash"]

        # Check each log in the receipt
        for log in receipt["logs"]:
            # Check if this is a USDC Transfer event
            if (log.get("address", "").lower() == USDC_ADDRESS and
                    len(log.get("topics", [])) > 0 and
                    log["topics"][0] == TRANSFER_TOPIC):

                transfer = parse_transfer_log(
                    log, tx_hash, block_number, timestamp
                )
                if transfer:
                    transfers.append(transfer)

    return transfers


async def main():
    all_transfers = []

    async with aiohttp.ClientSession() as session:
        for batch_start in range(
            START_BLOCK, END_BLOCK + 1, BLOCK_BATCH_SIZE
        ):
            batch_end = min(batch_start + BLOCK_BATCH_SIZE - 1, END_BLOCK)
            tasks = [
                process_block(session, b)
                for b in range(batch_start, batch_end + 1)
            ]

            results = await asyncio.gather(*tasks)

            for transfers in tqdm(
                results,
                total=len(results),
                desc=f"Blocks {batch_start}-{batch_end}"
            ):
                all_transfers.extend(transfers)

    # Write results to CSV
    if all_transfers:
        with open(OUTPUT_FILE, 'w', newline='') as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "block_number", "timestamp", "tx_hash",
                    "from", "to", "amount"
                ]
            )
            writer.writeheader()
            writer.writerows(all_transfers)

        print(f"\n✓ Extracted {len(all_transfers)} USDC transfers")
        print(f"✓ Results written to {OUTPUT_FILE}")

        # Calculate overall stats
        total_volume = sum(t["amount"] for t in all_transfers)
        unique_txs = len(set(t["tx_hash"] for t in all_transfers))

        # Calculate unique addresses (both from and to)
        unique_addresses = set()
        for t in all_transfers:
            unique_addresses.add(t["from"].lower())
            unique_addresses.add(t["to"].lower())

        # Calculate daily stats
        daily_data = defaultdict(lambda: {
            "volume": 0,
            "tx_hashes": set(),
            "transfer_count": 0
        })

        for t in all_transfers:
            date = datetime.fromtimestamp(t["timestamp"]).strftime("%Y-%m-%d")
            daily_data[date]["volume"] += t["amount"]
            daily_data[date]["tx_hashes"].add(t["tx_hash"])
            daily_data[date]["transfer_count"] += 1

        # Write daily stats to CSV
        with open(DAILY_STATS_FILE, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                "date", "total_volume_usdc", "transaction_count",
                "transfer_count", "avg_transfer_size_usdc"
            ])
            for date in sorted(daily_data.keys()):
                avg_transfer = (
                    daily_data[date]["volume"] /
                    daily_data[date]["transfer_count"]
                )
                writer.writerow([
                    date,
                    f"{daily_data[date]['volume']:.2f}",
                    len(daily_data[date]["tx_hashes"]),
                    daily_data[date]["transfer_count"],
                    f"{avg_transfer:.2f}"
                ])

        print(f"✓ Daily statistics written to {DAILY_STATS_FILE}")

        # Print overall stats
        print("\n" + "="*60)
        print("OVERALL STATISTICS")
        print("="*60)
        print(f"  Blocks processed: {START_BLOCK} to {END_BLOCK}")
        print(f"  Total USDC transfers: {len(all_transfers):,}")
        print(f"  Unique transactions: {unique_txs:,}")
        print(f"  Unique addresses: {len(unique_addresses):,}")
        print(f"  Total volume: ${total_volume:,.2f} USDC")

        # Print daily stats summary
        print("\n" + "="*75)
        print("DAILY STATISTICS")
        print("="*75)
        print(f"{'Date':<12} {'Volume (USDC)':>18} {'Txs':>8} "
              f"{'Transfers':>10} {'Avg Size':>15}")
        print("-"*75)
        for date in sorted(daily_data.keys()):
            volume = daily_data[date]["volume"]
            tx_count = len(daily_data[date]["tx_hashes"])
            transfer_count = daily_data[date]["transfer_count"]
            avg_transfer = volume / transfer_count
            print(f"{date:<12} ${volume:>16,.2f} {tx_count:>8,} "
                  f"{transfer_count:>10,} ${avg_transfer:>13,.2f}")
    else:
        print(
            f"\nNo USDC transfers found in blocks "
            f"{START_BLOCK} to {END_BLOCK}"
        )


if __name__ == "__main__":
    asyncio.run(main())
