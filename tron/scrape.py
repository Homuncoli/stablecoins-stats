import base64
from datetime import datetime, timezone
import hashlib
import logging
import os
import sys
import threading
from eth_abi import decode

import grpc

from model.Tron import TRON_QUEUE, TransactionDTO, TransferDTO, calc_trxID, extract_len_delimited_field, int_to_lo_hi, sun_to_trx

sys.path.insert(0, os.path.abspath('./tron/generated'))
sys.path.insert(0, os.path.abspath('./'))
import api.api_pb2 as api
import api.api_pb2_grpc as tron_api
from tron.generated.core import Tron_pb2 as protocol
from tron.generated.core.contract import asset_issue_contract_pb2
from tron.generated.core.contract import balance_contract_pb2
from tron.generated.core.contract import smart_contract_pb2

from metrics import timed

SCALER = 1_000
SCRAPE_PROGRESS = []

def __pair_transactions_with_infos(block, infos):
    info_by_txid = { info.id.hex(): info for info in infos.transactionInfo }
    return [ (trx, info_by_txid.get(calc_trxID(trx))) for trx in block.transactions ]

def __transfer_log_to_transfer_dto(id, j, trx, info, log, smart, logger):
    if len(log.topics) < 3: # old encoding
        from_address, to_address, value = decode(['address', 'address', 'uint256'], log.data)
        value_lo, value_hi = int_to_lo_hi(value)
        print(calc_trxID(trx), from_address, to_address, value)
    else:
        from_address = b'0x41' + log.topics[1].hex()[-40:].encode()
        to_address = b'0x41' + log.topics[2].hex()[-40:].encode()
        value = int(log.data.hex(), 16) if log.data != b'' else 0
        value_lo, value_hi = int_to_lo_hi(value)
    return (
        id,
        j,
        None,
        smart.contract_address,
        "TRC20",
        value_lo,
        value_hi,
        from_address,
        "Unknown",
        to_address,
        "Unknown",
        not bool(trx.ret[0].contractRet == protocol.Transaction.Result.SUCCESS)
    )

LOG_TO_TRANSFER = {
    'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef': __transfer_log_to_transfer_dto
}

def __log_to_transfer_dto(id, j, trx, info, log, smart, logger):
    if len(log.topics) == 0:
        return None
    
    keccak256 = log.topics[0].hex()
    if keccak256 in LOG_TO_TRANSFER:
        return LOG_TO_TRANSFER[keccak256](id, j, trx, info, log, smart, logger)
    else:
        return None
    pass

def __call_value_to_transfer_dto(id, j, trx, internal, call_value, logger):
    value_lo, value_hi = int_to_lo_hi(call_value.callValue)
    return (
        id,
        j,
        int(call_value.tokenId) if call_value.tokenId != '' else None,
        None,
        "TRX" if call_value.tokenId == '' else "TRC10",
        value_lo,
        value_hi,
        internal.caller_address,
        "Contract",
        internal.transferTo_address,
        "Unknown",
        not bool(internal.rejected)
    )

def __trigger_smart_contract(block, trx, i, info, smart, logger: logging.Logger) -> tuple[TransactionDTO, list[TransferDTO]]:
    id = block.block_header.raw_data.number * SCALER + i % SCALER
    tx : TransactionDTO =  (
        id,
        bool(trx.ret[0].contractRet == protocol.Transaction.Result.SUCCESS),
        datetime.fromtimestamp(block.block_header.raw_data.timestamp / 1000, tz=timezone.utc),
        protocol.Transaction.Contract.ContractType.Name(trx.raw_data.contract[0].type),
        trx.raw_data.fee_limit,
        info.fee if info.fee is not None else 0,
        info.receipt.energy_usage if info.receipt.energy_usage is not None else 0,
        info.receipt.net_fee if info.receipt.net_fee is not None else 0
    )

    tfs = []
    j = 0
    if smart.call_value != 0:
        value_lo, value_hi = int_to_lo_hi(smart.call_value)
        tf: TransferDTO = (
            id,
            j,
            None,
            None,
            "TRX",
            value_lo,
            value_hi,
            smart.owner_address,
            "EOA",
            smart.contract_address,
            "Contract",
            bool(trx.ret[0].contractRet == protocol.Transaction.Result.SUCCESS)
        )
        j += 1
        tfs.append(tf)

    if smart.call_token_value != 0:
        value_lo, value_hi = int_to_lo_hi(smart.call_token_value)
        tf: TransferDTO = (
            id,
            j,
            smart.token_id,
            None,
            "TRC10",
            value_lo,
            value_hi,
            smart.owner_address,
            "EOA",
            smart.contract_address,
            "Contract",
            bool(trx.ret[0].contractRet == protocol.Transaction.Result.SUCCESS)
        )
        j += 1
        tfs.append(tf)

    j = 2
    for internal in info.internal_transactions:
        for call_value in internal.callValueInfo:
            tf: TransferDTO = __call_value_to_transfer_dto(id, j, trx, internal, call_value, logger)
            tfs.append(tf)
            j += 1

    for log in info.log:
        tf: TransferDTO = __log_to_transfer_dto(id, j, trx, info, log, smart, logger)
        if tf is not None:
            tfs.append(tf)
            j += 1
        else:
            pass

    return tx, tfs

def __transfer_contract(block, trx, i, info, transfer, logger: logging.Logger) -> tuple[TransactionDTO, list[TransferDTO]]:
    id = block.block_header.raw_data.number * SCALER + i % SCALER

    tx : TransactionDTO =  (
        id,
        bool(trx.ret[0].contractRet == protocol.Transaction.Result.SUCCESS),
        datetime.fromtimestamp(block.block_header.raw_data.timestamp / 1000, tz=timezone.utc),
        protocol.Transaction.Contract.ContractType.Name(trx.raw_data.contract[0].type),
        trx.raw_data.fee_limit,
        info.fee if info.fee is not None else 0,
        info.receipt.energy_usage if info.receipt.energy_usage is not None else 0,
        info.receipt.net_fee if info.receipt.net_fee is not None else 0)

    value_lo, value_hi = int_to_lo_hi(transfer.amount)
    tf : TransferDTO = (
        id,
        0,
        None,
        None,
        "TRX",
        value_lo,
        value_hi,
        transfer.owner_address,
        "EOA",
        transfer.to_address,
        "Unknown",
        bool(trx.ret[0].contractRet == protocol.Transaction.Result.SUCCESS)
    )
    return tx, [tf]

def __transfer_asset_contract(block, trx, i, info, transfer_asset, logger: logging.Logger) -> tuple[TransactionDTO, list[TransferDTO]]:
    id = block.block_header.raw_data.number * SCALER + i % SCALER
    tx : TransactionDTO =  (
        id,
        bool(trx.ret[0].contractRet == protocol.Transaction.Result.SUCCESS),
        datetime.fromtimestamp(block.block_header.raw_data.timestamp / 1000, tz=timezone.utc),
        protocol.Transaction.Contract.ContractType.Name(trx.raw_data.contract[0].type),
        trx.raw_data.fee_limit,
        info.fee if info.fee is not None else 0,
        info.receipt.energy_usage if info.receipt.energy_usage is not None else 0,
        info.receipt.net_fee if info.receipt.net_fee is not None else 0
    )

    value_lo, value_hi = int_to_lo_hi(transfer_asset.amount)
    tf: TransferDTO = (
        id,
        0,
        int(transfer_asset.asset_name),
        None,
        "TRC10",
        value_lo,
        value_hi,
        transfer_asset.owner_address,
        "EOA",
        transfer_asset.to_address,
        "Unknown",
        bool(trx.ret[0].contractRet == protocol.Transaction.Result.SUCCESS)
    )
    return tx, [tf]


def __custom_contract(block, trx, i, info, custom, logger: logging.Logger) -> tuple[TransactionDTO, list[TransferDTO]]:
    pass

def __parse_transaction(block, trx, i, info, logger: logging.Logger) -> tuple[TransactionDTO, list[TransferDTO]] | tuple[None, None]:
    tx, tfs = None, None
    match trx.raw_data.contract[0].type:
        case protocol.Transaction.Contract.ContractType.TriggerSmartContract:
            msg = smart_contract_pb2.TriggerSmartContract()
            trx.raw_data.contract[0].parameter.Unpack(msg)
            tx, tfs = __trigger_smart_contract(block, trx, i, info, msg, logger)
        case protocol.Transaction.Contract.ContractType.TransferContract:
            msg = balance_contract_pb2.TransferContract()
            trx.raw_data.contract[0].parameter.Unpack(msg)
            tx, tfs = __transfer_contract(block, trx, i, info, msg, logger)
        case protocol.Transaction.Contract.ContractType.TransferAssetContract:
            msg = asset_issue_contract_pb2.TransferAssetContract()
            trx.raw_data.contract[0].parameter.Unpack(msg)
            tx, tfs = __transfer_asset_contract(block, trx, i, info, msg, logger)
        case protocol.Transaction.Contract.ContractType.CustomContract:
            owner_address = extract_len_delimited_field(trx.raw_data.contract[0].parameter.value,1,)
            tx, tfs = __custom_contract(block, trx, i, info, owner_address, logger)
        case _:
            pass
    return tx, tfs

def __scrape_block(stub: tron_api.WalletStub, block_num: int, logger: logging.Logger, chunk_id: int):
    block = None
    infos = None
    
    try:
        with timed("rpc", "rpc"):
            block = stub.GetBlockByNum(api.NumberMessage(num=block_num))
            infos = stub.GetTransactionInfoByBlockNum(api.NumberMessage(num=block_num))
    except grpc.RpcError as e:
        logger.fatal("RPC error while scraping block %d", block_num, exc_info=e)
        raise e
    
    block_data: list[tuple[TransactionDTO, list[TransferDTO]]] = []
    try:
        paired = None

        with timed("pairing", "rpc"):
            paired = __pair_transactions_with_infos(block, infos)

        with timed("processing", "rpc"):
            for i, (trx, info) in enumerate(paired):
                tx, tfs = __parse_transaction(block, trx, i, info, logger)
                if tx is not None and tfs is not None:
                    block_data.append((tx, tfs))

        TRON_QUEUE.put(block_data)
        SCRAPE_PROGRESS[chunk_id] += 1

    except Exception as e:
        logger.error("error processing block %d", block_num, exc_info=e)


def scrape(stub: tron_api.WalletStub, chunk_id: int, chunk_start: int, chunk_end: int, stop_event: threading.Event):
    logger = logging.getLogger(f"rpc-scraper-{chunk_id}")
    logger.debug("scraping for blocks %d to %d", chunk_start, chunk_end)

    current = chunk_start
    try:
        for block in range(chunk_start, chunk_end + 1):
            current = block

            __scrape_block(stub, block, logger, chunk_id)

            if stop_event.is_set():
                logger.info("stopped")
                break
    except Exception as e:
        logger.fatal("fatal error in RPC scraper at block %d (%d skipped): %s", block, chunk_end - current, exc_info=e)
        raise