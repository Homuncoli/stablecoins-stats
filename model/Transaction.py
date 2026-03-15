
from dataclasses import dataclass

from constants import TRON_CHAIN_ID


@dataclass
class Transaction:
    chain: int
    block_number: int
    tx_index: int

    from_id: bytearray
    to_id: bytearray
    method_id: str
    value: int
    gas_price: int
    gas_used: int
    effective_gas_price: int
    success: bool

class TronTransaction(Transaction):
    def __init__(self, block_number: int, tx_index: int, from_id: bytearray, to_id: bytearray, method_id: str, value: int, gas_price: int, gas_used: int, effective_gas_price: int, success: bool):
        super().__init__(TRON_CHAIN_ID, block_number, tx_index, from_id, to_id, method_id, value, gas_price, gas_used, effective_gas_price, success)

def insert_transactions(conn, transactions: list[Transaction]) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO transactions (chain, block_number, tx_index, from_id, to_id, method_id, value, gas_price, gas_used, effective_gas_price, success)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (chain, block_number, tx_index) DO NOTHING
            """,
            [(tx.chain, tx.block_number, tx.tx_index, tx.from_id, tx.to_id, tx.method_id, tx.value, tx.gas_price, tx.gas_used, tx.effective_gas_price, tx.success) for tx in transactions]
        )
    conn.commit()