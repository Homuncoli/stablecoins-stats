import argparse
import time
import os

from dotenv import load_dotenv
import psycopg
import psycopg_pool
from db_schema import ensure_schema
from concurrent.futures import ThreadPoolExecutor, as_completed

from tron.TronJsonRpc import TronRpcScraper
from scraper import NodeScraper

load_dotenv()

def process_block_range(scraper: NodeScraper, conn: psycopg.Connection, start: int, end: int):
    print(f"Processing blocks {start} to {end}...")
    time.sleep(1)  # Simulate work

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--chain", default=os.getenv("CHAIN", "eth"), help="Chain name")
    ap.add_argument("--rpc", default=os.getenv("RPC_URL"), help="RPC URL (HTTP)")
    ap.add_argument("--pg", default=os.getenv("PG_DSN"), help="Postgres DSN")
    ap.add_argument("--start", type=int, required=True, help="Start block (inclusive)")
    ap.add_argument("--end", type=int, default=None, help="End block (inclusive); default latest")
    ap.add_argument("--chunk-size", type=int, default=int(os.getenv("BLOCK_CHUNK_SIZE", "1000")), help="Blocks per Chunk")
    ap.add_argument("--workers", type=int, default=int(os.getenv("WORKERS", "10")), help="Number of worker threads")
    ap.add_argument("--timeout", type=int, default=int(os.getenv("RPC_TIMEOUT", "60")))
    ap.add_argument("--retries", type=int, default=int(os.getenv("RPC_MAX_RETRIES", "3")), help="Max request retries")
    ap.add_argument("--yaml", default=None, help="Optional: stablecoins YAML to ADD more event signatures (NO filtering)")
    args = ap.parse_args()

    if not args.rpc:
        raise SystemExit("Missing --rpc or RPC_URL")
    if not args.pg:
        raise SystemExit("Missing --pg or PG_DSN")
    
    scraperFactory = None
    match args.chain.lower():
        case "tron":
                scraperFactory = lambda: TronRpcScraper(args.rpc, max_retries=args.retries, timeout=args.timeout)
        case _:
            raise SystemExit(f"Unsupported chain: {args.chain}")

    with psycopg_pool.ConnectionPool(args.pg, min_size=args.workers, max_size=args.workers) as pool:
        pool.wait()
        start = args.start
        with pool.connection() as conn:
            ensure_schema(conn)
        
        with scraperFactory() as scraper:
            current_block = scraper.get_block_number()
            end = min(args.end, current_block) if args.end else current_block

        print(f"Processing blocks {start} to {end} on {args.chain} from {args.rpc} with {args.workers} workers...")
        print(f"\tChunk size: {args.chunk_size} blocks, {start} to {end} => {(end - start) // args.chunk_size + 1} chunks")

        ranges = [(i, min(i + args.chunk_size - 1, end)) for i in range(start, end + 1, args.chunk_size)]

        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = [executor.submit(process_block_range, scraperFactory(), conn, start, end) for start, end in ranges]

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Error processing block range: {e}")
