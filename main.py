import argparse
import time
import os
from unittest import case

from dotenv import load_dotenv
import psycopg
from db_schema import ensure_schema
from model.Address import Address, insert_addresses
from model.Block import insert_blocks
from model.Transaction import insert_transactions
from tron.TronJsonRpc import TronRpcScrapper

load_dotenv()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--chain", default=os.getenv("CHAIN", "eth"), help="Chain name")
    ap.add_argument("--rpc", default=os.getenv("RPC_URL"), help="RPC URL (HTTP)")
    ap.add_argument("--pg", default=os.getenv("PG_DSN"), help="Postgres DSN")
    ap.add_argument("--start", type=int, required=True, help="Start block (inclusive)")
    ap.add_argument("--end", type=int, default=None, help="End block (inclusive); default latest")
    ap.add_argument("--chunk-size", type=int, default=int(os.getenv("BLOCK_CHUNK_SIZE", "1000")), help="Blocks per outer processing chunk")
    ap.add_argument("--block-batch-size", type=int, default=int(os.getenv("BLOCK_BATCH_SIZE", "200")), help="How many blocks per JSON-RPC batch request")
    ap.add_argument("--timeout", type=int, default=int(os.getenv("RPC_TIMEOUT", "60")))
    ap.add_argument("--retries", type=int, default=int(os.getenv("RPC_MAX_RETRIES", "3")), help="Max JSON-RPC request retries")
    ap.add_argument("--yaml", default=None, help="Optional: stablecoins YAML to ADD more event signatures (NO filtering)")
    args = ap.parse_args()

    if not args.rpc:
        raise SystemExit("Missing --rpc or RPC_URL")
    if not args.pg:
        raise SystemExit("Missing --pg or PG_DSN")
    
    scraperFactory = None
    match args.chain.lower():
        case "tron":
                scraperFactory = lambda: TronRpcScrapper(args.rpc, block_batch_size=args.block_batch_size, max_retries=args.retries, timeout=args.timeout)
        case _:
            raise SystemExit(f"Unsupported chain: {args.chain}")

    with psycopg.connect(args.pg) as conn:
        ensure_schema(conn)

        with scraperFactory() as scraper:
            start = args.start
            end = min(args.end, scraper.get_block_number()) if args.end else scraper.get_block_number()
            
            chunk = 1
            print(f"Processing {end - start + 1} blocks (from {start} to {end}):")

            for chunk_start in range(start, end + 1, args.chunk_size):
                time_start = time.time()

                chunk_end = min(chunk_start + args.chunk_size - 1, end)

                ## RPC calls
                blocks = scraper.get_blocks_by_numbers(list(range(chunk_start, chunk_end + 1)), fullTrx=False)
                transactions = []
                for block in blocks:
                    transactions.extend(block.transaction_hashes)
                transactions = scraper.get_transaction_receipts([tx for tx in transactions])

                addresses = list()
                for tx in transactions:
                    addresses.append(Address(tx.chain, bytes(tx.from_id), tx.block_number))
                    addresses.append(Address(tx.chain, bytes(tx.to_id), tx.block_number))

                ## DB insertions
                insert_blocks(conn, blocks)
                insert_addresses(conn, addresses)
                insert_transactions(conn, transactions)

                time_end = time.time()
                print(f"\tChunk {chunk}: Time taken: {time_end - time_start:.2f} seconds => {((args.chunk_size) / (time_end - time_start)):.2f} blocks/s - estimated time remaining: {((end - chunk_end) / (args.chunk_size)) * (time_end - time_start) / 60:.2f} minutes")
                chunk += 1