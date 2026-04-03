import argparse
from concurrent.futures import ThreadPoolExecutor
import signal
import sys
import time
import os

from dotenv import load_dotenv
import psycopg
from psycopg_pool import ConnectionPool
from db_schema import ensure_schema

import logging
import threading
from concurrent.futures import wait, FIRST_COMPLETED

import grpc
from metrics import TIMING
from tron import scrape as tron
from tron.tx_consumer import tx_consumer
from tron.tf_consumer import tf_consumer

sys.path.insert(0, os.path.abspath('./tron/generated'))
sys.path.insert(0, os.path.abspath('./'))

import api.api_pb2 as api
import api.api_pb2_grpc as tron_api
from core.Tron_pb2 import Block as gRpcBlock

load_dotenv()

TIMEOUT = 1.0

def get_now_block(STUB) -> int:
    return STUB.GetNowBlock(api.EmptyMessage()).block_header.raw_data.number

def get_chunks(STUB, start: int, end: int, chunk_size: int) -> list[tuple[int, int]]:
    try: 
        now_block = get_now_block(STUB)
        end = min(end, now_block)
        chunks = [ (start + i, min(start + i + chunk_size - 1, end)) for i in range(0, end - start + 1, chunk_size) ]
        logging.info("%d chunks from block %d to %d with chunk size %d", len(chunks), start, end, chunk_size)
        return chunks
    except grpc.RpcError as e:
        logging.fatal("Failed to get now block for chunking: %s", e)
        raise

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rpc", default=os.getenv("RPC_URL"), help="RPC URL (HTTP)")
    ap.add_argument("--pg", default=os.getenv("PG_DSN"), help="Postgres DSN")
    ap.add_argument("--start", type=int, required=True, help="Start block (inclusive)")
    ap.add_argument("--end", type=int, default=None, help="End block (inclusive); default latest")
    ap.add_argument("--debug", default="INFO", help="Debug level")

    ap.add_argument("--rpc-workers", type=int, default=10, help="Number of concurrent RPC workers to use for scraping")
    ap.add_argument("--chunk-size", type=int, default=1000, help="Number of blocks to scrape per chunk")
    
    ap.add_argument("--tx-consumers", type=int, default=5, help="Number of transaction consumer threads")
    ap.add_argument("--tx-copy-batch-size", type=int, default=1000, help="Number of transactions to batch in each COPY from scraper to staging table")
    ap.add_argument("--tx-merge-batch-size", type=int, default=1000, help="Number of transactions to batch in each merge from staging to final table")
    
    ap.add_argument("--tf-consumers", type=int, default=5, help="Number of transfer consumer threads")
    ap.add_argument("--tf-copy-batch-size", type=int, default=1000, help="Number of transfers to batch in each COPY from scraper to staging table")
    ap.add_argument("--tf-merge-batch-size", type=int, default=1000, help="Number of transfers to batch in each merge from staging to final table")

    ap.add_argument("--profile-timing", action="store_true", help="Enable detailed timing of scraping and database operations")
    ap.add_argument("--metrics", type=int, default=60, help="Interval in seconds to log scraping metrics (blocks/sec, queue sizes, etc.)")

    args = ap.parse_args()

    logging.basicConfig(level=getattr(logging, args.debug.upper()), format='%(asctime)s - %(name)s - %(levelname)s: %(message)s')

    if args.profile_timing:
        TIMING.clear()
        TIMING.enable()

    if not args.rpc:
        raise SystemExit("Missing --rpc or RPC_URL")
    if not args.pg:
        raise SystemExit("Missing --pg or PG_DSN")
    
    CHANNEL = grpc.insecure_channel(args.rpc, options=[('grpc.max_send_message_length', 100 * 1024 * 1024), ('grpc.max_receive_message_length', 100 * 1024 * 1024)])
    STUB = tron_api.WalletStub(CHANNEL)

    with psycopg.connect(args.pg) as conn:
        ensure_schema(conn)

    chunks = get_chunks(STUB, args.start, args.end, args.chunk_size)

    rpc_futures = []
    rpc_stop = threading.Event()

    tx_consumer_stop_event = threading.Event()
    tx_threads = []

    tf_consumer_stop_event = threading.Event()
    tf_threads = []

    start_time = time.time()

    with ConnectionPool(args.pg, max_size=max(args.tx_consumers, 4), name="tx_pool") as tx_pool:
        for i in range(args.tx_consumers):
            thread = threading.Thread(
                target=tx_consumer,
                kwargs={
                    "pool": tx_pool,
                    "timeout": TIMEOUT,
                    "consumer_id": i,
                    "stop_event": tx_consumer_stop_event,
                    "batch_size": args.tx_copy_batch_size,
                    "metrics": args.metrics
                },
                name=f"tx-consumer-{i}"
            )
            thread.start()
            tx_threads.append(thread)

        with ConnectionPool(args.pg, max_size=max(args.tf_consumers, 4), name="tf_pool") as tf_pool:
            for i in range(args.tf_consumers):
                thread = threading.Thread(
                    target=tf_consumer,
                    kwargs={
                        "pool": tf_pool,
                        "timeout": TIMEOUT,
                        "consumer_id": i,
                        "stop_event": tf_consumer_stop_event,
                        "batch_size": args.tf_copy_batch_size,
                        "metrics": args.metrics
                    },
                    name=f"tf-consumer-{i}"
                )
                thread.start()
                tf_threads.append(thread)

            rpc_executor = ThreadPoolExecutor(max_workers=args.rpc_workers, thread_name_prefix="rpc-scraper")
            rpc_futures = [
                rpc_executor.submit(
                    tron.scrape,
                    STUB,
                    chunk_id=i,
                    chunk_start=chunk_start,
                    chunk_end=chunk_end,
                    stop_event=rpc_stop
                )
                for i, (chunk_start, chunk_end) in enumerate(chunks)
            ]

            try:
                pending = set(rpc_futures)
                while pending:
                    done, pending = wait(pending, timeout=1.0, return_when=FIRST_COMPLETED)
                    for future in done:
                        exception = future.exception()
                        if exception is not None:
                            raise exception
            except KeyboardInterrupt:
                logging.info("Keyboard interrupt received, stopping...")
                rpc_stop.set()
                rpc_executor.shutdown(wait=False, cancel_futures=True)
                pending = set(rpc_futures)
                while pending:
                    done, pending = wait(pending, timeout=1.0, return_when=FIRST_COMPLETED)
                    for future in done:
                        exception = future.exception()
                        if exception is not None:
                            raise exception
            except Exception as e:
                logging.fatal("Fatal error in RPC scraping threads: %s", e)
                rpc_stop.set()
                rpc_executor.shutdown(wait=False, cancel_futures=True)
                pending = set(rpc_futures)
                while pending:
                    done, pending = wait(pending, timeout=1.0, return_when=FIRST_COMPLETED)
                    for future in done:
                        exception = future.exception()
                        if exception is not None:
                            logging.fatal("RPC worker failed: %s", exception)
            finally:
                rpc_executor.shutdown(wait=False, cancel_futures=True)

            tx_consumer_stop_event.set()
            try:
                for thread in tx_threads:
                    thread.join()
            except Exception as e:
                logging.fatal("Fatal error in transaction consumer threads: %s", e)

            tf_consumer_stop_event.set()
            try:
                for thread in tf_threads:
                    thread.join()
            except Exception as e:
                logging.fatal("Fatal error in transfer consumer threads: %s", e)

    if args.profile_timing:
        for line in TIMING.report_lines():
            logging.info(line)

    end_time = time.time()
    logging.info("Scraped blocks %d in %d seconds => %f blocks/s", args.end - args.start + 1, end_time - start_time, (args.end - args.start) / (end_time - start_time) if end_time - start_time > 0 else 0)