from dataclasses import dataclass
from datetime import datetime
import hashlib
import queue
from datetime import datetime, timezone

type TokenType = str # 'TRX', 'TRC10', 'TRC20', 'TRC721'
type TransactionType = str # 'TransferContract', 'TransferAssetContract', etc.
# id, result, ts, transaction_t, fee_limit, fee, energy_usage, net_fee
type TransactionDTO = tuple[int, bool, datetime, TransactionType, int, int, int, int]
# transaction, idx, asset_id, contract_addr, token_type, amount_lo, amount_hi, from_addr, from_type, to_addr, to_type, success
type TransferDTO = tuple[int, int, bytes, bytes, str, int, int, bytes, str, bytes, str, bool] 

TX_COPY_TYPES = ["int8", "bool", "timestamp", "text", "int8", "int8", "int8", "int8"]
TF_COPY_TYPES = ["int8","int2","int8","int8", "int8", "int8", "int8","bool"]

TRON_QUEUE_SIZE = 3_000
TRON_QUEUE = queue.Queue[list[tuple[TransactionDTO, list[TransferDTO]]]](TRON_QUEUE_SIZE)

def calc_trxID(trx) -> str:
        raw_bytes = trx.raw_data.SerializeToString()
        return hashlib.sha256(raw_bytes).hexdigest()

def extract_len_delimited_field(payload: bytes, field_number: int) -> bytes | None:
        i = 0
        n = len(payload)

        while i < n:
            key = 0
            shift = 0
            while i < n:
                b = payload[i]
                i += 1
                key |= (b & 0x7F) << shift
                if (b & 0x80) == 0:
                    break
                shift += 7
            else:
                return None

            wire_type = key & 0x07
            number = key >> 3

            if wire_type == 0:
                while i < n and (payload[i] & 0x80):
                    i += 1
                i += 1
            elif wire_type == 1:
                i += 8
            elif wire_type == 2:
                length = 0
                shift = 0
                while i < n:
                    b = payload[i]
                    i += 1
                    length |= (b & 0x7F) << shift
                    if (b & 0x80) == 0:
                        break
                    shift += 7
                else:
                    return None

                if i + length > n:
                    return None

                value = payload[i:i + length]
                i += length
                if number == field_number:
                    return value
            elif wire_type == 5:
                i += 4
            else:
                return None

            if i > n:
                return None

        return None

def sun_to_trx(sun: int) -> float:
    return sun / 1_000_000

def trx_to_sun(trx: float) -> int:
    return int(trx * 1_000_000)

def int_to_lo_hi(value: int) -> tuple[int, int]:
    if value < 0 or value > (2**256 - 1):
        raise ValueError(f"Value must be in range [0, 2^256 - 1], got {value}")

    lo = value & 0xFFFFFFFFFFFFFFFF  # lower 64 bits
    hi = (value >> 64) & 0xFFFFFFFFFFFFFFFF  # upper 64 bits (of lower 128)

    # Convert to signed int64 for Postgres BIGINT
    if lo > 9223372036854775807:
        lo -= 18446744073709551616
    if hi > 9223372036854775807:
        hi -= 18446744073709551616

    return lo, hi

def lo_hi_to_int(lo: int, hi: int) -> int:
    # Convert from signed int64 to unsigned
    if lo < 0:
        lo += 18446744073709551616
    if hi < 0:
        hi += 18446744073709551616

    return (hi << 64) | lo

def to_tx_binary_row(row: TransactionDTO):
    return (
        row[0],
        row[1],
        row[2].astimezone(timezone.utc).replace(tzinfo=None),
        row[3],
        row[4],
        row[5],
        row[6],
        row[7],
    )