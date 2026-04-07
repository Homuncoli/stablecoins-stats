from dataclasses import dataclass
from datetime import datetime
import hashlib
import queue

type TokenType = str # 'TRX', 'TRC10', 'TRC20', 'TRC721'
type TransactionType = str # 'TransferContract', 'TransferAssetContract', etc.
# id, result, ts, transaction_t, fee_limit, fee, energy_usage, net_fee
type TransactionDTO = tuple[int, bool, datetime, TransactionType, int, int, int, int]
# transaction, index, transfer_type, token_asset_id, token_contract_addr, token_type, value, from_addr, from_type, to_addr, to_type, success 
type TransferDTO = tuple[int, int, int, bytes, TokenType, int, bytes, str, bytes, str, bool] 

TRON_QUEUE = queue.Queue[tuple[TransactionDTO, list[TransferDTO]]](4_000_000)

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