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
from collections import defaultdict

import logging
import threading

import grpc
from perf_timing import TIMING

from tron.TrongRpc import (
    ChunkRetryError,
    TronGRpcScraper,
    transaction_consumer,
    transaction_queue,
    transfer_consumer,
    transfer_queue,
)

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
    ap.add_argument("--tx-consumers", type=int, default=int(os.getenv("TX_CONSUMERS", "2")), help="Number of Postgres transaction writer threads")
    ap.add_argument("--tx-copy-batch-size", type=int, default=int(os.getenv("TX_COPY_BATCH_SIZE", "50000")), help="Rows to buffer before COPY into staging")
    ap.add_argument("--tx-merge-batch-size", type=int, default=int(os.getenv("TX_MERGE_BATCH_SIZE", "500000")), help="Rows in staging before merging into transactions")
    ap.add_argument("--tx-stage-commit-batch-size", type=int, default=int(os.getenv("TX_STAGE_COMMIT_BATCH_SIZE", "2000000")), help="Rows copied to staging before commit")
    ap.add_argument("--tx-merge-strategy", choices=["on_conflict", "anti_join"], default=os.getenv("TX_MERGE_STRATEGY", "on_conflict"), help="Merge strategy from staging to transactions")
    ap.add_argument("--tx-merge-on-shutdown-only", action="store_true", help="Only merge staging into transactions at shutdown")
    ap.add_argument("--tx-partition-span", type=int, default=int(os.getenv("TX_PARTITION_SPAN", "1000000")), help="Block span per transactions partition")
    ap.add_argument("--tx-queue-timeout", type=int, default=int(os.getenv("TX_QUEUE_TIMEOUT", "2")), help="Queue poll timeout (seconds) for transaction consumers")
    ap.add_argument("--tx-sync-commit", action="store_true", help="Enable synchronous_commit for transaction consumers")
    ap.add_argument("--tx-metrics-interval", type=int, default=int(os.getenv("TX_METRICS_INTERVAL", "30")), help="Seconds between transaction consumer metrics logs")
    ap.add_argument("--transfer-consumers", type=int, default=int(os.getenv("TRANSFER_CONSUMERS", "1")), help="Number of Postgres transfer writer threads")
    ap.add_argument("--transfer-copy-batch-size", type=int, default=int(os.getenv("TRANSFER_COPY_BATCH_SIZE", "100000")), help="Rows to buffer before COPY into transfer staging")
    ap.add_argument("--transfer-merge-batch-size", type=int, default=int(os.getenv("TRANSFER_MERGE_BATCH_SIZE", "1000000")), help="Rows in transfer staging before merging into transfers")
    ap.add_argument("--transfer-stage-commit-batch-size", type=int, default=int(os.getenv("TRANSFER_STAGE_COMMIT_BATCH_SIZE", "2000000")), help="Rows copied to transfer staging before commit")
    ap.add_argument("--transfer-merge-on-shutdown-only", action="store_true", help="Only merge transfer staging at shutdown")
    ap.add_argument("--transfer-queue-timeout", type=int, default=int(os.getenv("TRANSFER_QUEUE_TIMEOUT", "2")), help="Queue poll timeout (seconds) for transfer consumers")
    ap.add_argument("--transfer-sync-commit", action="store_true", help="Enable synchronous_commit for transfer consumers")
    ap.add_argument("--transfer-metrics-interval", type=int, default=int(os.getenv("TRANSFER_METRICS_INTERVAL", "30")), help="Seconds between transfer consumer metrics logs")
    ap.add_argument("--chunk-max-retries", type=int, default=int(os.getenv("CHUNK_MAX_RETRIES", "3")), help="How many times to retry a chunk on transient RPC failures")
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
    retried_chunks = []
    chunk_attempts = defaultdict(int)
    tx_consumer_stop_event = threading.Event()
    transfer_consumer_stop_event = threading.Event()

    with ThreadPoolExecutor(max_workers=args.tx_consumers, thread_name_prefix="transaction-consumer") as tx_consumer_executor:
        tx_consumer_futures = [
            tx_consumer_executor.submit(
                transaction_consumer,
                pg_dsn=args.pg,
                consumer_id=i,
                stop_event=tx_consumer_stop_event,
                batch_size=args.tx_copy_batch_size,
                merge_batch_size=args.tx_merge_batch_size,
                merge_on_shutdown_only=args.tx_merge_on_shutdown_only,
                merge_strategy=args.tx_merge_strategy,
                stage_commit_batch_size=args.tx_stage_commit_batch_size,
                queue_timeout=args.tx_queue_timeout,
                sync_commit=args.tx_sync_commit,
                metrics_interval_s=args.tx_metrics_interval,
            )
            for i in range(args.tx_consumers)
        ]

        with ThreadPoolExecutor(max_workers=args.transfer_consumers, thread_name_prefix="transfer-consumer") as transfer_consumer_executor:
            transfer_consumer_futures = [
                transfer_consumer_executor.submit(
                    transfer_consumer,
                    pg_dsn=args.pg,
                    consumer_id=i,
                    stop_event=transfer_consumer_stop_event,
                    batch_size=args.transfer_copy_batch_size,
                    merge_batch_size=args.transfer_merge_batch_size,
                    merge_on_shutdown_only=args.transfer_merge_on_shutdown_only,
                    stage_commit_batch_size=args.transfer_stage_commit_batch_size,
                    queue_timeout=args.transfer_queue_timeout,
                    sync_commit=args.transfer_sync_commit,
                    metrics_interval_s=args.transfer_metrics_interval,
                )
                for i in range(args.transfer_consumers)
            ]

            with ConnectionPool(args.pg, min_size=args.workers, max_size=args.workers) as pool:
                pending_chunks = list(chunks)
                while pending_chunks:
                    retry_queue = []
                    with ThreadPoolExecutor(max_workers=args.workers) as executor:
                        future_to_chunk = {
                            executor.submit(
                                scrape,
                                scraperFactory,
                                pool,
                                chunk_start,
                                chunk_end,
                                logging.getLogger(f"Chunk({chunk_start}-{chunk_end})"),
                            ): (chunk_start, chunk_end)
                            for (chunk_start, chunk_end) in pending_chunks
                        }

                        for future in as_completed(future_to_chunk):
                            chunk_start, chunk_end = future_to_chunk[future]
                            try:
                                future.result()
                            except ChunkRetryError as e:
                                chunk_attempts[(chunk_start, chunk_end)] += 1
                                attempt = chunk_attempts[(chunk_start, chunk_end)]
                                retried_chunks.append((chunk_start, chunk_end, attempt, str(e)))
                                if attempt <= args.chunk_max_retries:
                                    retry_queue.append((chunk_start, chunk_end))
                                    logging.warning(
                                        "Retrying chunk %s-%s (attempt %s/%s) due to transient RPC error: %s",
                                        chunk_start,
                                        chunk_end,
                                        attempt,
                                        args.chunk_max_retries,
                                        e,
                                    )
                                else:
                                    failed_chunks.append((chunk_start, chunk_end, str(e)))
                                    logging.error(
                                        "Chunk %s-%s exceeded retry limit (%s)",
                                        chunk_start,
                                        chunk_end,
                                        args.chunk_max_retries,
                                    )
                            except Exception as e:
                                failed_chunks.append((chunk_start, chunk_end, str(e)))
                                logging.error("Chunk %s-%s failed permanently: %s", chunk_start, chunk_end, e)

                    pending_chunks = retry_queue

            # Ensure all transactions are durably merged before transfers finalize.
            transaction_queue.join()
            tx_consumer_stop_event.set()
            for _ in range(args.tx_consumers):
                transaction_queue.put(None)

            for i, consumer_future in enumerate(tx_consumer_futures):
                try:
                    consumer_future.result(timeout=60)
                except Exception:
                    logging.exception("Transaction consumer %d failed", i)
                    raise

            # Transfers can now resolve FK references to transactions deterministically.
            transfer_queue.join()
            transfer_consumer_stop_event.set()
            for _ in range(args.transfer_consumers):
                transfer_queue.put(None)

            for i, consumer_future in enumerate(transfer_consumer_futures):
                try:
                    consumer_future.result(timeout=60)
                except Exception:
                    logging.exception("Transfer consumer %d failed", i)
                    raise
    close()
    with open(".failed_chunks.txt", "w") as f:
        for chunk_start, chunk_end, err in failed_chunks:
            f.write(f"{chunk_start}-{chunk_end}\t{err}\n")

    with open(".retried_chunks.txt", "w") as f:
        for chunk_start, chunk_end, attempt, err in retried_chunks:
            f.write(f"{chunk_start}-{chunk_end}\tattempt={attempt}\t{err}\n")

    end_time = time.time()
    logging.info(f"Finished processing blocks {start} to {end} in {end_time - start_time:.2f} seconds => {(end - start + 1) / (end_time - start_time):.2f} blocks/sec")
    if args.profile_timing:
        for line in TIMING.report_lines():
            logging.info(line)