import argparse
import sys
import time
import os
from unittest import case

from dotenv import load_dotenv
import psycopg
from psycopg_pool import ConnectionPool
from db_schema import ensure_schema

from concurrent.futures import ThreadPoolExecutor, as_completed

import grpc

from tron.TrongRpc import TronGRpcScraper

sys.path.insert(0, os.path.abspath('./tron/generated'))
sys.path.insert(0, os.path.abspath('./'))

import api.api_pb2 as api
import api.api_pb2_grpc as tron_api
from core.Tron_pb2 import Block as gRpcBlock

load_dotenv()

def run_scraper(scraperFactory, pool, start, end):
    with pool.connection() as conn:
        scraper = scraperFactory(conn)
        scraper.handle_range(start, end)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--chain", default=os.getenv("CHAIN", "eth"), help="Chain name")
    ap.add_argument("--rpc", default=os.getenv("RPC_URL"), help="RPC URL (HTTP)")
    ap.add_argument("--pg", default=os.getenv("PG_DSN"), help="Postgres DSN")
    ap.add_argument("--start", type=int, required=True, help="Start block (inclusive)")
    ap.add_argument("--end", type=int, default=None, help="End block (inclusive); default latest")
    ap.add_argument("--workers", type=int, default=int(os.getenv("WORKERS", "10")), help="Number of workers for parallel processing")
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
                channel = grpc.insecure_channel(args.rpc, options=[('grpc.max_send_message_length', 100 * 1024 * 1024), ('grpc.max_receive_message_length', 100 * 1024 * 1024)])
                stub = tron_api.WalletStub(channel)
                scraperFactory = lambda conn: TronGRpcScraper(stub, conn)
        case _:
            raise SystemExit(f"Unsupported chain: {args.chain}")

    with psycopg.connect(args.pg) as conn:
        ensure_schema(conn)

        scraper = scraperFactory(conn)
        start = args.start
        end = min(args.end, scraper.get_now_block()) if args.end else scraper.get_now_block()
        chunks = [ (start + i, min(start + i + args.chunk_size - 1, end)) for i in range(0, end - start + 1, args.chunk_size) ]

    start_time = time.time()
    with ConnectionPool(args.pg, min_size=args.workers, max_size=args.workers) as pool:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = [ executor.submit(run_scraper, scraperFactory, pool, range[0], range[1]) for i, range in enumerate(chunks) ]

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Error processing chunk: {e}")
                    raise e
    end_time = time.time()
    print(f"Finished processing blocks {start} to {end} in {end_time - start_time:.2f} seconds => {(end - start + 1) / (end_time - start_time):.2f} blocks/sec")