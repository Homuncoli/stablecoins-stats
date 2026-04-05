from datetime import datetime, timezone
import hashlib
import logging
import os
import sys
import threading

import grpc

from model.Tron import TF_QUEUE, TF_QUEUE, TX_QUEUE, TransactionDTO, TransferDTO, calc_trxID, sun_to_trx

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

def __pair_transactions_with_infos(block, infos):
    info_by_txid = { info.id.hex(): info for info in infos.transactionInfo }
    return [ (trx, info_by_txid.get(calc_trxID(trx))) for trx in block.transactions ]

def __trigger_smart_contract(block, trx, i, info, smart, logger: logging.Logger):
    pass

def __transfer_contract(block, trx, i, info, transfer, logger: logging.Logger):
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
    TX_QUEUE.put(tx)

    tf : TransferDTO = (
        id,
        0,
        None,
        None,
        "TRX",
        transfer.amount,
        transfer.owner_address,
        "EOA",
        transfer.to_address,
        "Unknown",
        bool(trx.ret[0].contractRet == protocol.Transaction.Result.SUCCESS)
    )
    TF_QUEUE.put(tf)

def __transfer_asset_contract(block, trx, i, info, transfer_asset, logger: logging.Logger):
    print(f"TransferAssetContract: {transfer_asset=}")
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
    TX_QUEUE.put(tx)

    tf: TransferDTO = (
        id,
        0,
        int(transfer_asset.asset_name),
        None,
        "TRC10",
        transfer_asset.amount,
        transfer_asset.owner_address,
        "EOA",
        transfer_asset.to_address,
        "Unknown",
        bool(trx.ret[0].contractRet == protocol.Transaction.Result.SUCCESS)
    )
    TF_QUEUE.put(tf)


def __custom_contract(block, trx, i, info, custom, logger: logging.Logger):
    pass

def __parse_transaction(block, trx, i, info, logger: logging.Logger):
    match trx.raw_data.contract[0].type:
        case protocol.Transaction.Contract.ContractType.TriggerSmartContract:
            msg = smart_contract_pb2.TriggerSmartContract()
            trx.raw_data.contract[0].parameter.Unpack(msg)
            __trigger_smart_contract(block, trx, i, info, msg, logger)
        case protocol.Transaction.Contract.ContractType.TransferContract:
            msg = balance_contract_pb2.TransferContract()
            trx.raw_data.contract[0].parameter.Unpack(msg)
            __transfer_contract(block, trx, i, info, msg, logger)
        case protocol.Transaction.Contract.ContractType.TransferAssetContract:
            msg = asset_issue_contract_pb2.TransferAssetContract()
            trx.raw_data.contract[0].parameter.Unpack(msg)
            __transfer_asset_contract(block, trx, i, info, msg, logger)
        case protocol.Transaction.Contract.ContractType.CustomContract:
            owner_address = __extract_len_delimited_field(trx.raw_data.contract[0].parameter.value,1,)
            __custom_contract(block, trx, i, info, owner_address, logger)
        case _:
            return

def __scrape_block(stub: tron_api.WalletStub, block_num: int, logger: logging.Logger):
    block = None
    infos = None
    
    try:
        with timed("scraper.rpc"):
            block = stub.GetBlockByNum(api.NumberMessage(num=block_num))
            infos = stub.GetTransactionInfoByBlockNum(api.NumberMessage(num=block_num))
    except grpc.RpcError as e:
        logger.fatal("RPC error while scraping block %d", block_num, exc_info=e)
        raise e
    
    try:
        paired = None

        with timed("scraper.pairing"):
            paired = __pair_transactions_with_infos(block, infos)

        with timed("scraper.processing"):
            for i, (trx, info) in enumerate(paired):
                __parse_transaction(block, trx, i, info, logger)

    except Exception as e:
        logger.error("error processing block %d", block_num, exc_info=e)


def scrape(stub: tron_api.WalletStub, chunk_id: int, chunk_start: int, chunk_end: int, stop_event: threading.Event):
    logger = logging.getLogger(f"rpc-scraper-{chunk_id}")
    logger.debug("scraping for blocks %d to %d", chunk_start, chunk_end)

    current = chunk_start
    try:
        for block in range(chunk_start, chunk_end + 1):
            current = block

            __scrape_block(stub, block, logger)

            if stop_event.is_set():
                logger.info("stopped")
                break
    except Exception as e:
        logger.fatal("fatal error in RPC scraper at block %d (%d skipped): %s", block, chunk_end - current, exc_info=e)
        raise