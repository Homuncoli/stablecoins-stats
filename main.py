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

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

import logging
import threading
from concurrent.futures import wait, FIRST_COMPLETED

import grpc
from metrics import TIMING, log_timings
from model.Tron import TRON_QUEUE
from tron import scrape as tron
from tron.db_consumer import db_consumer

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
        chunks = [ (start + i, min(start + i + chunk_size - 1, end)) for i in range(0, end - start + 1, chunk_size) ]
        return chunks
    except grpc.RpcError as e:
        logging.fatal("Failed to get now block for chunking: %s", e)
        raise

def monitor_metrics(stop_event: threading.Event, args, rpc_futures: list, interval: int):
    logger = logging.getLogger("metrics-monitor")
    with logging_redirect_tqdm():
        with tqdm(total=args.chunk_size * len(rpc_futures), unit="blocks", desc="Scraped") as pbar:
            last_done = 0
            while not stop_event.is_set():
                done = sum(1 for future in rpc_futures if future.done()) * args.chunk_size
                
                pbar.update(done - last_done)
                pbar.set_postfix({
                    "queue": TRON_QUEUE.qsize()
                })

                last_done = done
                if done == args.chunk_size * len(rpc_futures):
                    break
                time.sleep(interval)

            done = sum(1 for future in rpc_futures if future.done()) * args.chunk_size
                
            pbar.update(done - last_done)
            pbar.set_postfix({
                "queue": TRON_QUEUE.qsize()
            })
        
        with tqdm(total=TRON_QUEUE.qsize(), unit="transactions", desc="Backlog") as pbar:
            last_queue_size = TRON_QUEUE.qsize()
            while not stop_event.is_set() or not TRON_QUEUE.empty():
                queue_size = TRON_QUEUE.qsize()

                pbar.update(last_queue_size - queue_size)
                pbar.set_postfix({
                    "pending": queue_size
                })

                last_queue_size = queue_size
                time.sleep(interval)
            pbar.update(last_queue_size - TRON_QUEUE.qsize())
            pbar.set_postfix({
                "pending": TRON_QUEUE.qsize()
            })

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rpc", default=os.getenv("RPC_URL"), help="RPC URL (HTTP)")
    ap.add_argument("--pg", default=os.getenv("PG_DSN"), help="Postgres DSN")
    ap.add_argument("--start", type=int, required=True, help="Start block (inclusive)")
    ap.add_argument("--end", type=int, default=None, help="End block (inclusive); default latest")
    ap.add_argument("--debug", default="INFO", help="Debug level")

    ap.add_argument("--rpc-workers", type=int, default=None, help="Number of concurrent RPC workers to use for scraping")
    ap.add_argument("--chunk-size", type=int, default=None, help="Number of blocks to scrape per chunk")
    
    ap.add_argument("--db-consumers", type=int, default=5, help="Number of database consumer threads")
    ap.add_argument("--commit-size", type=int, default=1000, help="Number of transactions to batch in each copy from buffer to staging table")
    ap.add_argument("--merge-size", type=int, default=1000, help="Number of transactions to batch in each merge from staging table to final tables")
    
    ap.add_argument("--profile-timing", action="store_true", help="Enable detailed timing of scraping and database operations")
    ap.add_argument("--metrics", type=int, default=5, help="Interval in seconds to log scraping metrics (blocks/sec, queue sizes, etc.)")
    args = ap.parse_args()

    logging.basicConfig(level=getattr(logging, args.debug.upper()), format='%(asctime)s - %(name)s - %(levelname)s: %(message)s')

    if args.profile_timing:
        TIMING_ENABLED = True

    if not args.rpc:
        raise SystemExit("Missing --rpc or RPC_URL")
    if not args.pg:
        raise SystemExit("Missing --pg or PG_DSN")
    
    CHANNEL = grpc.insecure_channel(args.rpc, options=[('grpc.max_send_message_length', 100 * 1024 * 1024), ('grpc.max_receive_message_length', 100 * 1024 * 1024)])
    STUB = tron_api.WalletStub(CHANNEL)

    with psycopg.connect(args.pg) as conn:
        ensure_schema(conn)

    args.end = args.end if args.end is not None else get_now_block(STUB)

    if args.rpc_workers is None and args.chunk_size is None:
        logging.fatal("At least one of --rpc-workers or --chunk-size must be specified")
        raise SystemExit(1)
    if args.rpc_workers is None:
        args.rpc_workers = (args.end - args.start) // args.chunk_size + 1
    if args.chunk_size is None:
        args.chunk_size = (args.end - args.start) // args.rpc_workers + 1

    logging.info("Scraping %d blocks (from %d to %d) in %d chunks with chunk size %d => %d RPC workers, %d DB consumers", args.end - args.start + 1, args.start, args.end, (args.end - args.start + 1) // args.chunk_size, args.chunk_size, args.rpc_workers, args.db_consumers)

    chunks = get_chunks(STUB, args.start, args.end, args.chunk_size)

    rpc_futures = []
    rpc_stop = threading.Event()
    db_threads = []
    db_stop_event = threading.Event()

    start_time = time.time()

    with ConnectionPool(args.pg, max_size=max(args.db_consumers, 4), name="db_pool") as db_pool:
        for i in range(args.db_consumers):
            thread = threading.Thread(
                target=db_consumer,
                kwargs={
                    "pool": db_pool,
                    "timeout": TIMEOUT,
                    "consumer_id": i,
                    "stop_event": db_stop_event,
                    "commit_size": args.commit_size,
                    "merge_size": args.merge_size,
                    "metrics": args.metrics
                },
                name=f"db-consumer-{i}"
            )
            thread.start()
            db_threads.append(thread)

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

        if args.metrics > 0:
            metrics_thread = threading.Thread(
                target=monitor_metrics,
                kwargs={
                    "stop_event": db_stop_event,
                    "args": args,
                    "rpc_futures": rpc_futures,
                    "interval": args.metrics
                },
                name="metrics-monitor",
                daemon=True
            )
            metrics_thread.start()

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

        logging.info("RPC scraping completed, waiting for database consumers to finish processing remaining items in queue...")
        db_stop_event.set()
        try:
            for thread in db_threads:
                thread.join()
        except Exception as e:
            logging.fatal("Fatal error in database consumer threads: %s", e)

    CHANNEL.close()

    log_timings()

    end_time = time.time()
    logging.info("Scraped blocks %d in %d seconds => %f blocks/s", args.end - args.start + 1, end_time - start_time, (args.end - args.start) / (end_time - start_time) if end_time - start_time > 0 else 0)