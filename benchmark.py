#!/usr/bin/env python3
"""
Benchmark script for main.py with varying worker and batchsize settings.
Outputs results to a CSV file for graphing.
"""

import subprocess
import csv
import time
import re
import sys
import os
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv

# Configuration
WORKERS = [10, 20]          # Number of workers to test
BATCH_SIZES = [50, 100]  # Block batch sizes to test

# These parameters should be set by the user or via environment variables
START_BLOCK = 77626535                  # Set this to your desired start block
END_BLOCK = START_BLOCK + 1_000         # Set to None to use latest, or specify a block number
CHAIN = "tron"

load_dotenv()
RPC_URL = os.getenv("RPC_URL")
PG_DSN = os.getenv("PG_DSN")

def run_benchmark(workers, batch_size, start, end):
    """
    Run main.py with given settings and return the total execution time in seconds.
    Returns None if the run failed.
    """
    cmd = [
        sys.executable, "main.py",
        "--chain", CHAIN,
        "--start", str(start),
        "--workers", str(workers),
        "--block-batch-size", str(batch_size),
        "--debug", "WARNING",  # Reduce log verbosity
    ]
    
    if RPC_URL:
        cmd.extend(["--rpc", RPC_URL])
    
    if PG_DSN:
        cmd.extend(["--pg", PG_DSN])
    
    if end is not None:
        cmd.extend(["--end", str(end)])
    
    print(f"Running: workers={workers}, batch_size={batch_size}...")
    start_time = time.time()
    
    try:
        result = subprocess.run(
            cmd,
            cwd=Path(__file__).parent,
            capture_output=True,
            text=True,
            timeout=3600  # 1 hour timeout
        )
        
        elapsed = time.time() - start_time
        
        if result.returncode != 0:
            print(f"  FAILED (exit code {result.returncode})")
            print(f"  stderr: {result.stderr[:200]}")
            return None
        
        # Try to extract the actual time from logging output
        # Look for pattern: "in X.XX seconds"
        match = re.search(r'in ([\d.]+) seconds', result.stderr)
        if match:
            measured_time = float(match.group(1))
            print(f"  SUCCESS: {measured_time:.2f}s")
            return measured_time
        else:
            print(f"  SUCCESS: {elapsed:.2f}s (wall clock)")
            return elapsed
            
    except subprocess.TimeoutExpired:
        print(f"  TIMEOUT (>1 hour)")
        return None
    except Exception as e:
        print(f"  ERROR: {e}")
        return None

def main():
    """Run benchmarks and save results to CSV."""
    
    if not RPC_URL or not PG_DSN:
        print("ERROR: Missing RPC_URL and/or PG_DSN environment variables")
        print("Set them before running, or pass --rpc and --pg arguments to this script")
        sys.exit(1)
    
    results = []
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"benchmark_results_{timestamp}.csv"
    
    print(f"\n{'='*60}")
    print(f"Benchmarking main.py")
    print(f"Start block: {START_BLOCK}, End block: {END_BLOCK or 'latest'}")
    print(f"Chain: {CHAIN}")
    print(f"Workers to test: {WORKERS}")
    print(f"Batch sizes to test: {BATCH_SIZES}")
    print(f"Results will be saved to: {output_file}")
    print(f"{'='*60}\n")
    
    total_runs = len(WORKERS) * len(BATCH_SIZES)
    run_count = 0
    
    # Test 1: Vary batch size with default workers
    print("\n--- Test 1: Varying batch size (with 1st worker setting) ---")
    default_workers = WORKERS[0]
    for batch_size in BATCH_SIZES:
        run_count += 1
        print(f"[{run_count}/{total_runs}] ", end="")
        exec_time = run_benchmark(default_workers, batch_size, START_BLOCK, END_BLOCK)
        if exec_time is not None:
            results.append({
                'test': 'batch_size_vary',
                'workers': default_workers,
                'batch_size': batch_size,
                'execution_time_seconds': f"{exec_time:.2f}",
            })
    
    # Test 2: Vary workers with default batch size
    print("\n--- Test 2: Varying workers (with 1st batch size setting) ---")
    default_batch_size = BATCH_SIZES[0]
    for workers in WORKERS:
        run_count += 1
        print(f"[{run_count}/{total_runs}] ", end="")
        exec_time = run_benchmark(workers, default_batch_size, START_BLOCK, END_BLOCK)
        if exec_time is not None:
            results.append({
                'test': 'workers_vary',
                'workers': workers,
                'batch_size': default_batch_size,
                'execution_time_seconds': f"{exec_time:.2f}",
            })
    
    # Write results to CSV
    if results:
        with open(output_file, 'w', newline='') as f:
            fieldnames = ['test', 'workers', 'batch_size', 'execution_time_seconds']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        
        print(f"\n{'='*60}")
        print(f"✓ Results saved to: {output_file}")
        print(f"✓ {len(results)} successful runs")
        print(f"{'='*60}\n")
        
        # Print summary
        print("Summary of results:")
        for row in results:
            print(f"  workers={row['workers']:2d}, batch_size={row['batch_size']:4d}, time={row['execution_time_seconds']:>8s}s")
    else:
        print("\n✗ No successful benchmark runs!")
        sys.exit(1)

if __name__ == "__main__":
    main()
