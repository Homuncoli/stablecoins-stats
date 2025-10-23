import asyncio
import aiohttp
import csv
import argparse
from datetime import datetime
from collections import defaultdict
from tqdm import tqdm

# --- Configuration ---
RPC_URL = "http://10.9.0.35:8545"  # ethereum2
START_BLOCK = 20328000   # July 17th 2025
END_BLOCK = 20340000  # July 19th 2025
BLOCK_BATCH_SIZE = 100  # number of blocks to query concurrently

# Stablecoin configurations
STABLECOINS = {
    "usdc": {
        "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".lower(),
        "decimals": 6,
        "symbol": "USDC"
    },
    "usdt": {
        "address": "0xdAC17F958D2ee523a2206206994597C13D831ec7".lower(),
        "decimals": 6,
        "symbol": "USDT"
    },
    "pyusd": {
        "address": "0x6c3ea9036406852006290770BEdFcAbA0e23A0e8".lower(),
        "decimals": 6,
        "symbol": "PYUSD"
    },
    "busd": {
        "address": "0x4Fabb145d64652a948d72533023f6E7A623C7C53".lower(),
        "decimals": 18,
        "symbol": "BUSD"
    },
    "dai": {
        "address": "0x6B175474E89094C44Da98b954EedeAC495271d0F".lower(),
        "decimals": 18,
        "symbol": "DAI"
    },
    "eurc": {
        "address": "0x1aBaEA1f7C830cD89Eff2d4bB2882FbC54A9Ec56".lower(),
        "decimals": 6,
        "symbol": "EURC"
    }
}

# ERC20 Transfer event signature:
# Transfer(address indexed from, address indexed to, uint256 value)
TRANSFER_TOPIC = (
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)


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


def parse_transfer_log(log, tx_hash, block_number, timestamp,
                       stablecoin_config):
    """Parse a stablecoin Transfer event log"""
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

    # Convert based on stablecoin decimals
    amount_tokens = amount / (10 ** stablecoin_config["decimals"])

    return {
        "block_number": block_number,
        "timestamp": timestamp,
        "tx_hash": tx_hash,
        "from": from_address,
        "to": to_address,
        "amount": amount_tokens
    }


async def process_block(session, block_number, selected_stablecoins):
    """Process a single block and extract transfers for all selected
    stablecoins"""
    block = await rpc_call(
        session, "eth_getBlockByNumber", [hex(block_number), True]
    )

    if block is None:
        return {}

    timestamp = int(block["timestamp"], 16)

    # fetch all receipts for the block in one call
    receipts = await rpc_call(
        session, "eth_getBlockReceipts", [hex(block_number)]
    )

    if receipts is None:
        return {}

    # Initialize transfers dict for each stablecoin
    all_transfers = {name: [] for name in selected_stablecoins}

    for receipt in receipts:
        if receipt is None or "logs" not in receipt:
            continue

        tx_hash = receipt["transactionHash"]

        # Check each log in the receipt
        for log in receipt["logs"]:
            # Check if this is a Transfer event for any selected stablecoin
            if (len(log.get("topics", [])) > 0 and
                    log["topics"][0] == TRANSFER_TOPIC):
                log_address = log.get("address", "").lower()
                # Find which stablecoin this transfer belongs to
                for stablecoin_name in selected_stablecoins:
                    stablecoin_config = STABLECOINS[stablecoin_name]
                    if log_address == stablecoin_config["address"]:
                        transfer = parse_transfer_log(
                            log, tx_hash, block_number, timestamp,
                            stablecoin_config
                        )
                        if transfer:
                            all_transfers[stablecoin_name].append(transfer)
                        break  # Found matching stablecoin, stop checking

    return all_transfers


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Analyze stablecoin transfers on Ethereum",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python stats.py --usdc          # Analyze USDC only
  python stats.py --usdt --dai    # Analyze USDT and DAI
  python stats.py --all            # Analyze all stablecoins
        """
    )

    # Add flags for each stablecoin
    parser.add_argument("--usdc", action="store_true", help="Analyze USDC")
    parser.add_argument("--usdt", action="store_true", help="Analyze USDT")
    parser.add_argument("--pyusd", action="store_true", help="Analyze PYUSD")
    parser.add_argument("--busd", action="store_true", help="Analyze BUSD")
    parser.add_argument("--dai", action="store_true", help="Analyze DAI")
    parser.add_argument("--eurc", action="store_true", help="Analyze EURC")
    parser.add_argument("--all", action="store_true",
                        help="Analyze all stablecoins")

    args = parser.parse_args()

    # Determine which stablecoins to analyze
    selected_stablecoins = []

    if args.all:
        selected_stablecoins = list(STABLECOINS.keys())
    else:
        for stablecoin in STABLECOINS.keys():
            if getattr(args, stablecoin, False):
                selected_stablecoins.append(stablecoin)

    # Default to all stablecoins if no flags provided
    if not selected_stablecoins:
        selected_stablecoins = list(STABLECOINS.keys())

    return selected_stablecoins


def analyze_stablecoin_data(stablecoin_name, stablecoin_config, all_transfers):
    """Analyze and save data for a specific stablecoin"""
    print(f"\n{'='*60}")
    print(f"ANALYZING {stablecoin_config['symbol']}")
    print(f"{'='*60}")

    # Generate output filenames
    output_file = f"{stablecoin_name}_transfers.csv"
    daily_stats_file = f"{stablecoin_name}_daily_stats.csv"

    # Write results to CSV
    if all_transfers:
        with open(output_file, 'w', newline='') as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "block_number", "timestamp", "tx_hash",
                    "from", "to", "amount"
                ]
            )
            writer.writeheader()
            writer.writerows(all_transfers)

        print(f"\n✓ Extracted {len(all_transfers)} "
              f"{stablecoin_config['symbol']} transfers")
        print(f"✓ Results written to {output_file}")

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
        with open(daily_stats_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                "date", f"total_volume_{stablecoin_name}", "transaction_count",
                "transfer_count", f"avg_transfer_size_{stablecoin_name}"
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

        print(f"✓ Daily statistics written to {daily_stats_file}")

        # Print overall stats
        print("\n" + "="*60)
        print("OVERALL STATISTICS")
        print("="*60)
        print(f"  Blocks processed: {START_BLOCK} to {END_BLOCK}")
        print(f"  Total {stablecoin_config['symbol']} transfers: "
              f"{len(all_transfers):,}")
        print(f"  Unique transactions: {unique_txs:,}")
        print(f"  Unique addresses: {len(unique_addresses):,}")

        # Use appropriate currency symbol
        currency_symbol = "€" if stablecoin_config['symbol'] == "EURC" else "$"
        print(f"  Total volume: {currency_symbol}{total_volume:,.2f} "
              f"{stablecoin_config['symbol']}")

        # Print daily stats summary
        print("\n" + "="*75)
        print("DAILY STATISTICS")
        print("="*75)
        volume_header = f"Volume ({stablecoin_config['symbol']})"
        print(f"{'Date':<12} {volume_header:>18} {'Txs':>8} "
              f"{'Transfers':>10} {'Avg Size':>15}")
        print("-"*75)
        for date in sorted(daily_data.keys()):
            volume = daily_data[date]["volume"]
            tx_count = len(daily_data[date]["tx_hashes"])
            transfer_count = daily_data[date]["transfer_count"]
            avg_transfer = volume / transfer_count
            print(f"{date:<12} {currency_symbol}{volume:>15,.2f} "
                  f"{tx_count:>8,} {transfer_count:>10,} "
                  f"{currency_symbol}{avg_transfer:>12,.2f}")
    else:
        print(
            f"\nNo {stablecoin_config['symbol']} transfers found in blocks "
            f"{START_BLOCK} to {END_BLOCK}"
        )


async def main():
    selected_stablecoins = parse_arguments()

    symbols = [STABLECOINS[s]['symbol'] for s in selected_stablecoins]
    print(f"Analyzing stablecoins: {', '.join(symbols)}")
    print(f"Processing blocks {START_BLOCK} to {END_BLOCK}")

    # Initialize aggregated transfers for each stablecoin
    aggregated_transfers = {name: [] for name in selected_stablecoins}

    async with aiohttp.ClientSession() as session:
        # Process blocks in batches
        for batch_start in range(
            START_BLOCK, END_BLOCK + 1, BLOCK_BATCH_SIZE
        ):
            batch_end = min(batch_start + BLOCK_BATCH_SIZE - 1, END_BLOCK)
            tasks = [
                process_block(session, b, selected_stablecoins)
                for b in range(batch_start, batch_end + 1)
            ]

            results = await asyncio.gather(*tasks)

            # Process results and aggregate transfers
            for block_transfers in tqdm(
                results,
                total=len(results),
                desc=f"Blocks {batch_start}-{batch_end}"
            ):
                for stablecoin_name in selected_stablecoins:
                    aggregated_transfers[stablecoin_name].extend(
                        block_transfers.get(stablecoin_name, [])
                    )

    # Analyze each stablecoin's data
    for stablecoin_name in selected_stablecoins:
        stablecoin_config = STABLECOINS[stablecoin_name]
        analyze_stablecoin_data(stablecoin_name, stablecoin_config,
                                aggregated_transfers[stablecoin_name])


if __name__ == "__main__":
    asyncio.run(main())
