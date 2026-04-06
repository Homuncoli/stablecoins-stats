import argparse
import csv
import itertools
import logging
import os
import subprocess
import sys
import time
from dataclasses import dataclass

from dotenv import load_dotenv
import psycopg

load_dotenv()

@dataclass(frozen=True)
class BenchmarkConfig:
    rpc_workers: int
    db_consumers: int
    commit_size: int
    merge_size: int


@dataclass
class BenchmarkResult:
    config: BenchmarkConfig
    run_index: int
    seconds: float
    blocks_per_second: float
    return_code: int
    stdout_tail: str
    stderr_tail: str


def parse_int_list(raw: str) -> list[int]:
    values = []
    for part in raw.split(","):
        value = part.strip()
        if not value:
            continue
        values.append(int(value))
    if not values:
        raise ValueError("Expected at least one integer value")
    return values


def cleanup_block_range(pg_dsn: str, start_block: int, end_block: int) -> None:
    tx_min = start_block * 1000
    tx_max = end_block * 1000 + 999

    with psycopg.connect(pg_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM transfers WHERE transaction BETWEEN %s AND %s", (tx_min, tx_max))
            cur.execute("DELETE FROM logs WHERE transaction BETWEEN %s AND %s", (tx_min, tx_max))
            cur.execute("DELETE FROM transactions WHERE id BETWEEN %s AND %s", (tx_min, tx_max))
        conn.commit()


def run_single(
    base_cmd: list[str],
    cfg: BenchmarkConfig,
    blocks: int,
    run_index: int,
) -> BenchmarkResult:
    cmd = [
        *base_cmd,
        "--rpc-workers",
        str(cfg.rpc_workers),
        "--db-consumers",
        str(cfg.db_consumers),
        "--commit-size",
        str(cfg.commit_size),
        "--merge-size",
        str(cfg.merge_size),
    ]

    started = time.perf_counter()
    proc = subprocess.run(cmd, capture_output=True, text=True)
    elapsed = time.perf_counter() - started

    blocks_per_second = blocks / elapsed if elapsed > 0 else 0.0
    return BenchmarkResult(
        config=cfg,
        run_index=run_index,
        seconds=elapsed,
        blocks_per_second=blocks_per_second,
        return_code=proc.returncode,
        stdout_tail="\n".join(proc.stdout.splitlines()[-8:]),
        stderr_tail="\n".join(proc.stderr.splitlines()[-8:]),
    )


def print_leaderboard(results: list[BenchmarkResult], repeats: int) -> None:
    by_cfg: dict[BenchmarkConfig, list[BenchmarkResult]] = {}
    for result in results:
        by_cfg.setdefault(result.config, []).append(result)

    rows = []
    for cfg, cfg_results in by_cfg.items():
        successful = [r for r in cfg_results if r.return_code == 0]
        if not successful:
            rows.append((cfg, float("inf"), 0.0, 0, len(cfg_results)))
            continue

        avg_seconds = sum(r.seconds for r in successful) / len(successful)
        avg_bps = sum(r.blocks_per_second for r in successful) / len(successful)
        rows.append((cfg, avg_seconds, avg_bps, len(successful), len(cfg_results)))

    rows.sort(key=lambda r: r[2], reverse=True)

    print("\n=== Benchmark Leaderboard (sorted by avg blocks/s) ===")
    print(
        "rpc-workers db-consumers commit-size merge-size "
        "avg-seconds avg-blocks/s successful-runs"
    )
    for cfg, avg_seconds, avg_bps, ok_runs, total_runs in rows:
        avg_seconds_text = f"{avg_seconds:.3f}" if avg_seconds != float("inf") else "failed"
        avg_bps_text = f"{avg_bps:.2f}" if avg_bps > 0 else "failed"
        print(
            f"{cfg.rpc_workers:11d} {cfg.db_consumers:12d} {cfg.commit_size:11d} "
            f"{cfg.merge_size:10d} {avg_seconds_text:11} {avg_bps_text:12} "
            f"{ok_runs}/{total_runs}"
        )

    if rows and rows[0][2] > 0:
        best_cfg, best_seconds, best_bps, ok_runs, _ = rows[0]
        print("\nBest config:")
        print(
            f"rpc-workers={best_cfg.rpc_workers}, db-consumers={best_cfg.db_consumers}, "
            f"commit-size={best_cfg.commit_size}, merge-size={best_cfg.merge_size}, "
            f"avg-time={best_seconds:.3f}s, avg-throughput={best_bps:.2f} blocks/s, "
            f"successful runs={ok_runs}/{repeats}"
        )


def write_csv(path: str, results: list[BenchmarkResult]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "rpc_workers",
                "db_consumers",
                "commit_size",
                "merge_size",
                "run_index",
                "seconds",
                "blocks_per_second",
                "return_code",
                "stdout_tail",
                "stderr_tail",
            ]
        )
        for r in results:
            writer.writerow(
                [
                    r.config.rpc_workers,
                    r.config.db_consumers,
                    r.config.commit_size,
                    r.config.merge_size,
                    r.run_index,
                    f"{r.seconds:.6f}",
                    f"{r.blocks_per_second:.6f}",
                    r.return_code,
                    r.stdout_tail,
                    r.stderr_tail,
                ]
            )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark Tron ingestion configs and optimize for maximum total blocks/s. "
            "Each test processes a fixed block range (default 10,000 blocks)."
        )
    )
    parser.add_argument("--start", type=int, required=True, help="Start block (inclusive)")
    parser.add_argument("--blocks", type=int, default=10_000, help="Number of blocks per benchmark run")
    parser.add_argument("--rpc", default=os.getenv("RPC_URL"), help="RPC URL")
    parser.add_argument("--pg", default=os.getenv("PG_DSN"), help="Postgres DSN")

    parser.add_argument("--rpc-workers", default="8,12,16,24", help="Comma-separated values")
    parser.add_argument("--db-consumers", default="2,4,8", help="Comma-separated values")
    parser.add_argument("--commit-size", default="500,1000,2000", help="Comma-separated values")
    parser.add_argument("--merge-size", default="1000,2000,4000", help="Comma-separated values")
    parser.add_argument("--repeats", type=int, default=1, help="Number of repeats per config")

    parser.add_argument(
        "--cleanup-before-each-run",
        action="store_true",
        help="Delete data for the tested block range before every run for fair comparison",
    )
    parser.add_argument("--csv", default="benchmark_results.csv", help="Output CSV file path")
    parser.add_argument("--debug", default="INFO", help="Logging level")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.debug.upper(), logging.INFO),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    if args.blocks <= 0:
        raise SystemExit("--blocks must be > 0")
    if args.repeats <= 0:
        raise SystemExit("--repeats must be > 0")
    if not args.rpc:
        raise SystemExit("Missing --rpc or RPC_URL")
    if not args.pg:
        raise SystemExit("Missing --pg or PG_DSN")

    end = args.start + args.blocks - 1
    rpc_workers_values = parse_int_list(args.rpc_workers)
    db_consumers_values = parse_int_list(args.db_consumers)
    commit_size_values = parse_int_list(args.commit_size)
    merge_size_values = parse_int_list(args.merge_size)

    configs = [
        BenchmarkConfig(*combo)
        for combo in itertools.product(
            rpc_workers_values,
            db_consumers_values,
            commit_size_values,
            merge_size_values,
        )
    ]

    logging.info("Benchmark block range: %d..%d (%d blocks)", args.start, end, args.blocks)
    logging.info("Testing %d parameter combinations with %d repeat(s)", len(configs), args.repeats)

    base_cmd = [
        sys.executable,
        "main.py",
        "--start",
        str(args.start),
        "--end",
        str(end),
        "--rpc",
        args.rpc,
        "--pg",
        args.pg,
        "--metrics",
        "0",
        "--debug",
        "WARNING",
    ]

    all_results: list[BenchmarkResult] = []
    total_runs = len(configs) * args.repeats
    run_counter = 0

    for cfg in configs:
        for run_idx in range(1, args.repeats + 1):
            run_counter += 1
            logging.info(
                "[%d/%d] cfg=%s repeat=%d/%d",
                run_counter,
                total_runs,
                cfg,
                run_idx,
                args.repeats,
            )

            if args.cleanup_before_each_run:
                cleanup_block_range(args.pg, args.start, end)

            result = run_single(base_cmd, cfg, args.blocks, run_idx)
            all_results.append(result)

            if result.return_code == 0:
                logging.info(
                    "Done in %.3fs => %.2f blocks/s",
                    result.seconds,
                    result.blocks_per_second,
                )
            else:
                logging.error("Run failed with exit code %d", result.return_code)
                if result.stderr_tail:
                    logging.error("stderr tail:\n%s", result.stderr_tail)

    write_csv(args.csv, all_results)
    print_leaderboard(all_results, args.repeats)
    print(f"\nDetailed run results saved to: {args.csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())