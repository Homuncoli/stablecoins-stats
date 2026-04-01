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

import logging
import threading

import grpc
from perf_timing import TIMING

from tron.TrongRpc import TronGRpcScraper, transaction_consumer, transaction_queue

sys.path.insert(0, os.path.abspath('./tron/generated'))
sys.path.insert(0, os.path.abspath('./'))

import api.api_pb2 as api
import api.api_pb2_grpc as tron_api
from core.Tron_pb2 import Block as gRpcBlock

load_dotenv()

def scrape(factory, pool, start, end, logger):
    with pool.connection() as conn:
        scraper = factory(conn)
        scraper.handle_range(start, end, logger)

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
    ap.add_argument("--debug", type=str, default="INFO", help="Enable debug logging")
    ap.add_argument("--profile-timing", action="store_true", help="Collect and print section timings across all threads")
    args = ap.parse_args()

    logging.basicConfig(level=getattr(logging, args.debug.upper()), format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    if args.profile_timing:
        TIMING.clear()
        TIMING.enable()

    if not args.rpc:
        raise SystemExit("Missing --rpc or RPC_URL")
    if not args.pg:
        raise SystemExit("Missing --pg or PG_DSN")
    
    scraperFactory = None
    close = None
    match args.chain.lower():
        case "tron":
                channel = grpc.insecure_channel(args.rpc, options=[('grpc.max_send_message_length', 100 * 1024 * 1024), ('grpc.max_receive_message_length', 100 * 1024 * 1024)])
                stub = tron_api.WalletStub(channel)
                scraperFactory = lambda conn: TronGRpcScraper(stub, conn)
                close = channel.close
        case _:
            raise SystemExit(f"Unsupported chain: {args.chain}")

    with psycopg.connect(args.pg) as conn:
        ensure_schema(conn)

        scraper = scraperFactory(conn)
        start = args.start
        end = min(args.end, scraper.get_now_block()) if args.end else scraper.get_now_block()
        chunks = [ (start + i, min(start + i + args.chunk_size - 1, end)) for i in range(0, end - start + 1, args.chunk_size) ]
        logging.info(f"Processing blocks from {start} to {end} in {len(chunks)} chunks of up to {args.chunk_size} blocks each with {args.workers} workers...")

    start_time = time.time()

    failed_chunks = []
    consumer_stop_event = threading.Event()
    consumer_thread = threading.Thread(
        target=transaction_consumer,
        kwargs={"output_csv": "TRANSACTIONS.csv", "stop_event": consumer_stop_event},
        name="transaction-consumer",
        daemon=True,
    )
    consumer_thread.start()

    with ConnectionPool(args.pg, min_size=args.workers, max_size=args.workers) as pool:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = [ executor.submit(scrape, scraperFactory, pool, chunk_start, chunk_end, logging.getLogger(f"Chunk {i+1}({chunk_start}-{chunk_end})")) for i, (chunk_start, chunk_end) in enumerate(chunks) ]

            for i, future in enumerate(as_completed(futures)):
                try:
                    future.result()
                except Exception as e:
                    failed_chunks.append((args.start + args.chunk_size * i, args.start + args.chunk_size * (i + 1) - 1))
                    logging.error(f"Error processing chunk: {e}")

    # Drain queued transactions before shutdown.
    transaction_queue.join()
    consumer_stop_event.set()
    transaction_queue.put(None)
    consumer_thread.join(timeout=30)
    close()
    with open(".failed_chunks.txt", "w") as f:
        for chunk_start, chunk_end in failed_chunks:
            f.write(f"{chunk_start}-{chunk_end}\n")

    end_time = time.time()
    logging.info(f"Finished processing blocks {start} to {end} in {end_time - start_time:.2f} seconds => {(end - start + 1) / (end_time - start_time):.2f} blocks/sec")
    if args.profile_timing:
        for line in TIMING.report_lines():
            logging.info(line)