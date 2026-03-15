import json
import time

import psycopg
from db_schema import ensure_schema
from model.Address import insert_addresses
from model.Block import insert_blocks
from model.Transaction import insert_transactions
from tron.TronJsonRpc import TronRpcScrapper

if __name__ == "__main__":
    with psycopg.connect(
        host="localhost",
        port=5432,
        dbname="tron1",
        user="tron1_benedikt",
        password="password"
    ) as conn:
        ensure_schema(conn)

        with TronRpcScrapper("http://10.9.0.3:8555/jsonrpc") as scraper:
            start = 10149009
            end = start + 1000
            time_start = time.time()

            blocks = scraper.get_blocks_by_numbers(list(range(start, end)), fullTrx=False)
            insert_blocks(conn, blocks)
            transactions = []
            for block in blocks:
                transactions.extend(block.transaction_hashes)

            transactions = scraper.get_transaction_receipts([tx for tx in transactions])
            addresses = set()
            for tx in transactions:
                addresses.add((tx.chain, bytes(tx.from_id)))
                addresses.add((tx.chain, bytes(tx.to_id)))
            insert_addresses(conn, addresses)
            insert_transactions(conn, transactions)

            time_end = time.time()
            print(f"Time taken: {time_end - time_start:.2f} seconds => {((1000) / (time_end - time_start)):.2f} blocks/s")