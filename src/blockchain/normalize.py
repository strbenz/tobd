import os
from decimal import Decimal
from datetime import datetime, timezone
from typing import Dict, Any


def _whale_threshold() -> Decimal:
    raw = os.getenv("WBTC_WHALE_THRESHOLD_BTC", "5")
    try:
        return Decimal(raw)
    except Exception:
        return Decimal("5")


def _gas_eth_to_usd() -> Decimal:
    raw = os.getenv("GAS_ETH_TO_USD", "26000")
    try:
        return Decimal(raw)
    except Exception:
        return Decimal("26000")


def normalize_wbtc_tx(raw_tx: Dict[str, Any]) -> Dict[str, Any]:
    """
    Преобразует сырую транзакцию WBTC в нормализованный вид для raw.wbtc_transfers.
    Добавляем флаг "is_whale" и оценку комиссии в USD.
    """

    # timestamp -> datetime
    ts = int(raw_tx["timeStamp"])
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)

    # token decimals
    token_decimal = int(raw_tx.get("tokenDecimal", 8))

    # value_raw -> Decimal
    value_raw = Decimal(raw_tx["value"])
    value_wbtc = value_raw / (Decimal(10) ** token_decimal) if token_decimal > 0 else value_raw

    # газовые поля могут отсутствовать / быть пустыми
    gas = raw_tx.get("gas")
    gas_price = raw_tx.get("gasPrice")
    gas_used = raw_tx.get("gasUsed")

    gas_limit = int(gas) if gas is not None and gas != "" else None
    gas_price_wei = Decimal(gas_price) if gas_price is not None and gas_price != "" else None
    gas_used_int = int(gas_used) if gas_used is not None and gas_used != "" else None

    tx_fee_eth = None
    if gas_price_wei is not None and gas_used_int is not None:
        tx_fee_eth = (gas_price_wei * gas_used_int) / (Decimal(10) ** 18)

    tx_fee_usd = tx_fee_eth * _gas_eth_to_usd() if tx_fee_eth is not None else None

    is_whale = value_wbtc >= _whale_threshold()

    return {
        "tx_hash": raw_tx["hash"],
        "block_number": int(raw_tx["blockNumber"]),
        "block_hash": raw_tx["blockHash"],
        "time_stamp": dt,

        "nonce": int(raw_tx["nonce"]),
        "transaction_index": int(raw_tx["transactionIndex"]),

        "from_address": raw_tx["from"],
        "to_address": raw_tx["to"],

        "contract_address": raw_tx["contractAddress"],
        "token_name": raw_tx.get("tokenName", "Wrapped Bitcoin"),
        "token_symbol": raw_tx.get("tokenSymbol", "WBTC"),
        "token_decimal": token_decimal,

        "value_raw": value_raw,
        "value_wbtc": value_wbtc,
        "is_whale": is_whale,

        "gas_limit": gas_limit,
        "gas_price_wei": gas_price_wei,
        "gas_used": gas_used_int,
        "cumulative_gas_used": Decimal(raw_tx["cumulativeGasUsed"]),

        "tx_fee_eth": tx_fee_eth,
        "tx_fee_usd": tx_fee_usd,

        "input": raw_tx.get("input"),
        "method_id": raw_tx.get("methodId"),
        "function_name": raw_tx.get("functionName"),
        "confirmations": int(raw_tx["confirmations"]),
    }
