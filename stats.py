import asyncio
import aiohttp
import csv
import argparse
from datetime import datetime
from collections import defaultdict
from tqdm import tqdm

# --- Configuration ---
# -- VM --
#RPC_URL = "http://localhost:8545"  # ethereum2
#START_BLOCK = 22699600   # June 14th 2025
#END_BLOCK = 23300300  # Sept 5th 2025
# -- local --
RPC_URL = "http://10.9.0.35:8545"  # ethereum2
START_BLOCK = 22700000   
END_BLOCK = 22720000  
BLOCK_BATCH_SIZE = 200  # number of blocks to query concurrently
CHUNK_SIZE = 10000  # number of blocks to process before writing to CSV

# DeFi Protocol addresses (main contracts)
DEFI_PROTOCOLS = {
    "aave": [
        "0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9",  # Aave V2 Pool
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",  # Aave V3 Pool
    ],
    "compound": [
        "0x39AA39c021dfbaE8faC545936693aC917d5E7563",  # Compound Comptroller
        # "0xc00e94cb662c3520282e6f5717214004a7f26888",  # Compound Token
    ],
    "uniswap": [ # https://docs.uniswap.org/contracts/v3/reference/deployments/ethereum-deployments
        #  https://docs.uniswap.org/contracts/v2/reference/smart-contracts/v2-deployments
        # "0x1f98431c8ad98523631ae4a59f267346ea31f984",  # Uniswap V3 Factory
        "0xE592427A0AEce92De3Edee1F18E0157C05861564",  # Uniswap V3 Router
        "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D",  # Uniswap V2 Router
    ],
    "lido": [
        "0xAE7ab96520DE3A18E5e111B5EaAb095312d7FE84",  # Lido stETH
        "0xdc24316b9ae028f1497c275eb9192a3ea0f67022",  # Lido stETH Curve Pool
    ],
    "curve": [
        "0xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7",  # Curve 3pool
        "0xa2b47e3d5c8c5c5c5c5c5c5c5c5c5c5c5c5c5c5c",  # Curve Registry
    ],
    "dydx": [
        "0x1e0447b19bb6ecfdae1e4ae1694b0c3659614e4e",  # dYdX Solo Margin
        # "0x4ec4ba6e9bb1e416b70419c1a96c319c12f98234",  # dYdX Perpetual
    ]
    #"morpho": [
    #    "0x58D97B57BB95320F9a05dC918Aef65434969c2B2",  # Morpho Protocol
    #]
}

# ROLLUP Protocol addresses
ROLLUP_PROTOCOLS = {
    "arbitrum": [  # https://docs.arbitrum.io/build-decentralized-apps/reference/contract-addresses
        "0x8315177aB297bA92A06054cE80a67Ed4DBd7ed3a",  # Arbitrum One Bridge
        "0xC1Ebd02f738644983b6C4B2d440b8e77DdE276Bd",  # Arbitrum Nova Bridge
        "0x0B9857ae2D4A3DBe74ffE1d7DF045bb7F96E4840",  # Arbitrum One Outbox
        "0xD4B80C3D7240325D18E645B49e6535A3Bf95cc58",  # Arbitrum Nova Outbox
        "0x912CE59144191C1204E64559FE8253a0e49E6548",  # Arbitrum One Delayed Inbox
        "0xc4448b71118c9071Bcb9734A0EAc55D18A153949",  # Arbitrum Nova Delayed Inbox
    ],
    "base": [  #https://docs.base.org/base-chain/network-information/base-contracts#ethereum-mainnet
        "0x3154Cf16ccdb4C6d922629664174b904d80F2C35",  # Base Bridge
    ],
    "optimism": [  # https://docs.optimism.io/reference/addresses
        "0x99C9fc46f92E8a1c0deC1b1747d010903E884bE1",  # Optimism Bridge Proxy
    ],
    #"polygon": [
    #    "0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0",  # Polygon Bridge
    #],
    "unichain": [  #https://docs.unichain.org/docs/technical-information/contract-addresses
        "0x81014f44b0a345033bb2b3b21c7a1a308b35feea",  # Unichain Bridge
    ]
}

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
    # "dai": {
    #     "address": "0x6B175474E89094C44Da98b954EedeAC495271d0F".lower(),
    #     "decimals": 18,
    #     "symbol": "DAI"
    # },
    "eurc": {
        "address": "0x1aBaEA1f7C830bD89Acc67eC4af516284b1bC33c".lower(),
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


def write_transfers_chunk(transfers, output_file, is_first_chunk=False):
    """Write a chunk of transfers to CSV file"""
    if not transfers:
        return
    
    mode = 'w' if is_first_chunk else 'a'
    with open(output_file, mode, newline='') as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "block_number", "timestamp", "tx_hash",
                "from", "to", "amount", "is_defi_transaction", "is_rollup_transaction"
            ]
        )
        if is_first_chunk:
            writer.writeheader()
        writer.writerows(transfers)


def write_daily_stats_chunk(daily_data, daily_stats_file, is_first_chunk=False):
    """Write daily stats chunk to CSV file"""
    if not daily_data:
        return
    
    mode = 'w' if is_first_chunk else 'a'
    with open(daily_stats_file, mode, newline='') as f:
        writer = csv.writer(f)
        if is_first_chunk:
            # Extract stablecoin name from filename
            stablecoin_name = daily_stats_file.replace("_daily_stats.csv", "")
            writer.writerow([
                "date", f"total_volume_{stablecoin_name}", "transaction_count",
                "transfer_count", f"avg_transfer_size_{stablecoin_name}",
                "different_addresses", "defi_transaction_count", "rollup_transaction_count"
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
                f"{avg_transfer:.2f}",
                len(daily_data[date]["different_addresses"]),
                len(daily_data[date]["defi_tx_hashes"]),
                len(daily_data[date]["rollup_tx_hashes"])
            ])


def is_defi_protocol(address):
    """Check if an address belongs to any DeFi protocol"""
    address_lower = address.lower()
    
    # Check DeFi protocols
    for protocol, addresses in DEFI_PROTOCOLS.items():
        for protocol_address in addresses:
            if address_lower == protocol_address.lower():
                return True
    
    return False


def is_rollup_protocol(address):
    """Check if an address belongs to any ROLLUP protocol"""
    address_lower = address.lower()
    
    # Check ROLLUP protocols
    for protocol, addresses in ROLLUP_PROTOCOLS.items():
        for protocol_address in addresses:
            if address_lower == protocol_address.lower():
                return True
    
    return False


def is_defi_or_rollup_protocol(address):
    """Check if an address belongs to any DeFi or ROLLUP protocol"""
    return is_defi_protocol(address) or is_rollup_protocol(address)


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
        "amount": amount_tokens,
        "is_defi_transaction": (is_defi_protocol(from_address) or
                                is_defi_protocol(to_address)),
        "is_rollup_transaction": (is_rollup_protocol(from_address) or
                                  is_rollup_protocol(to_address))
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
  # python stats.py --usdt --dai    # Analyze USDT and DAI
  python stats.py --all            # Analyze all stablecoins
        """
    )

    # Add flags for each stablecoin
    parser.add_argument("--usdc", action="store_true", help="Analyze USDC")
    parser.add_argument("--usdt", action="store_true", help="Analyze USDT")
    parser.add_argument("--pyusd", action="store_true", help="Analyze PYUSD")
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
                    "from", "to", "amount", "is_defi_transaction"
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
        defi_txs = len(set(t["tx_hash"] for t in all_transfers
                           if t["is_defi_transaction"]))
        rollup_txs = len(set(t["tx_hash"] for t in all_transfers
                             if t["is_rollup_transaction"]))

        # Calculate different addresses (both from and to)
        different_addresses = set()
        for t in all_transfers:
            different_addresses.add(t["from"].lower())
            different_addresses.add(t["to"].lower())

        # Calculate daily stats
        daily_data = defaultdict(lambda: {
            "volume": 0,
            "tx_hashes": set(),
            "transfer_count": 0,
            "different_addresses": set(),
            "defi_tx_hashes": set(),
            "rollup_tx_hashes": set()
        })

        for t in all_transfers:
            date = datetime.fromtimestamp(t["timestamp"]).strftime("%Y-%m-%d")
            daily_data[date]["volume"] += t["amount"]
            daily_data[date]["tx_hashes"].add(t["tx_hash"])
            daily_data[date]["transfer_count"] += 1
            daily_data[date]["different_addresses"].add(t["from"].lower())
            daily_data[date]["different_addresses"].add(t["to"].lower())

            # Track DeFi and rollup transactions separately
            if t["is_defi_transaction"]:
                daily_data[date]["defi_tx_hashes"].add(t["tx_hash"])
            if t["is_rollup_transaction"]:
                daily_data[date]["rollup_tx_hashes"].add(t["tx_hash"])

        # Write daily stats to CSV
        with open(daily_stats_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                "date", f"total_volume_{stablecoin_name}", "transaction_count",
                "transfer_count", f"avg_transfer_size_{stablecoin_name}",
                "different_addresses", "defi_transaction_count", "rollup_transaction_count"
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
                    f"{avg_transfer:.2f}",
                    len(daily_data[date]["different_addresses"]),
                    len(daily_data[date]["defi_tx_hashes"]),
                    len(daily_data[date]["rollup_tx_hashes"])
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
        print(f"  DeFi transactions: {defi_txs:,}")
        print(f"  Rollup transactions: {rollup_txs:,}")
        print(f"  Different addresses: {len(different_addresses):,}")

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
    print(f"Using chunk size: {CHUNK_SIZE} blocks per chunk")

    # Initialize output files for each stablecoin
    for stablecoin_name in selected_stablecoins:
        output_file = f"{stablecoin_name}_transfers.csv"
        daily_stats_file = f"{stablecoin_name}_daily_stats.csv"
        
        # Clear existing files
        import os
        if os.path.exists(output_file):
            os.remove(output_file)
        if os.path.exists(daily_stats_file):
            os.remove(daily_stats_file)

    # Process blocks in chunks to reduce memory usage
    chunk_count = 0
    total_blocks_processed = 0
    
    # Initialize global daily stats aggregation
    global_daily_stats = {
        name: defaultdict(lambda: {
            "volume": 0,
            "tx_hashes": set(),
            "transfer_count": 0,
            "different_addresses": set(),
            "defi_tx_hashes": set(),
            "rollup_tx_hashes": set()
        }) for name in selected_stablecoins
    }
    
    async with aiohttp.ClientSession() as session:
        for chunk_start in range(START_BLOCK, END_BLOCK + 1, CHUNK_SIZE):
            chunk_end = min(chunk_start + CHUNK_SIZE - 1, END_BLOCK)
            chunk_count += 1
            
            print(f"\nProcessing chunk {chunk_count}: blocks {chunk_start}-{chunk_end}")
            
            # Initialize chunk data for each stablecoin
            chunk_transfers = {name: [] for name in selected_stablecoins}
            chunk_daily_stats = {
                name: defaultdict(lambda: {
                    "volume": 0,
                    "tx_hashes": set(),
                    "transfer_count": 0,
                    "different_addresses": set(),
                    "defi_tx_hashes": set(),
                    "rollup_tx_hashes": set()
                }) for name in selected_stablecoins
            }
            
            # Process blocks in this chunk
            for batch_start in range(chunk_start, chunk_end + 1, BLOCK_BATCH_SIZE):
                batch_end = min(batch_start + BLOCK_BATCH_SIZE - 1, chunk_end)
                tasks = [
                    process_block(session, b, selected_stablecoins)
                    for b in range(batch_start, batch_end + 1)
                ]

                results = await asyncio.gather(*tasks)
                total_blocks_processed += len(results)

                # Process results and aggregate transfers for this chunk
                for block_transfers in tqdm(
                    results,
                    total=len(results),
                    desc=f"Batch {batch_start}-{batch_end}"
                ):
                    for stablecoin_name in selected_stablecoins:
                        transfers = block_transfers.get(stablecoin_name, [])
                        chunk_transfers[stablecoin_name].extend(transfers)
                        
                        # Update global daily stats (aggregate across all chunks)
                        for transfer in transfers:
                            date = datetime.fromtimestamp(transfer["timestamp"]).strftime("%Y-%m-%d")
                            global_daily_stats[stablecoin_name][date]["volume"] += transfer["amount"]
                            global_daily_stats[stablecoin_name][date]["tx_hashes"].add(transfer["tx_hash"])
                            global_daily_stats[stablecoin_name][date]["transfer_count"] += 1
                            global_daily_stats[stablecoin_name][date]["different_addresses"].add(transfer["from"].lower())
                            global_daily_stats[stablecoin_name][date]["different_addresses"].add(transfer["to"].lower())
                            
                            if transfer["is_defi_transaction"]:
                                global_daily_stats[stablecoin_name][date]["defi_tx_hashes"].add(transfer["tx_hash"])
                            if transfer["is_rollup_transaction"]:
                                global_daily_stats[stablecoin_name][date]["rollup_tx_hashes"].add(transfer["tx_hash"])
            
            # Write transfers chunk data to CSV files
            for stablecoin_name in selected_stablecoins:
                if chunk_transfers[stablecoin_name]:
                    output_file = f"{stablecoin_name}_transfers.csv"
                    
                    # Write transfers chunk
                    write_transfers_chunk(chunk_transfers[stablecoin_name], output_file, 
                                        is_first_chunk=(chunk_count == 1))
                    
                    print(f"  ✓ {stablecoin_name.upper()}: {len(chunk_transfers[stablecoin_name])} transfers")
            
            # Clear chunk transfers data to free memory (keep global daily stats)
            chunk_transfers.clear()
            
            print(f"✓ Chunk {chunk_count} completed. Total blocks processed: {total_blocks_processed}")

    # Write final aggregated daily stats to CSV files
    print(f"\nWriting aggregated daily statistics...")
    for stablecoin_name in selected_stablecoins:
        daily_stats_file = f"{stablecoin_name}_daily_stats.csv"
        write_daily_stats_chunk(global_daily_stats[stablecoin_name], daily_stats_file, is_first_chunk=True)
        print(f"  ✓ {stablecoin_name.upper()}: Daily stats written")

    # Generate final summary statistics
    print(f"\n{'='*60}")
    print("FINAL SUMMARY")
    print(f"{'='*60}")
    
    for stablecoin_name in selected_stablecoins:
        stablecoin_config = STABLECOINS[stablecoin_name]
        output_file = f"{stablecoin_name}_transfers.csv"
        
        # Count total transfers from file
        total_transfers = 0
        if os.path.exists(output_file):
            with open(output_file, 'r') as f:
                total_transfers = sum(1 for line in f) - 1  # Subtract header
        
        print(f"{stablecoin_config['symbol']}: {total_transfers:,} total transfers")
    
    print(f"\nAnalysis complete! Processed {total_blocks_processed} blocks in {chunk_count} chunks.")


if __name__ == "__main__":
    asyncio.run(main())
