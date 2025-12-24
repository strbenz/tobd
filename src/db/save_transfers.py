import sys
from pathlib import Path
from typing import List, Dict, Any

from psycopg2.extras import execute_values

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from src.db.connection import get_pg_connection


COLUMNS = [
    "tx_hash",
    "block_number",
    "block_hash",
    "time_stamp",
    "nonce",
    "transaction_index",
    "from_address",
    "to_address",
    "contract_address",
    "token_name",
    "token_symbol",
    "token_decimal",
    "value_raw",
    "value_wbtc",
    "is_whale",
    "gas_limit",
    "gas_price_wei",
    "gas_used",
    "cumulative_gas_used",
    "tx_fee_eth",
    "tx_fee_usd",
    "input",
    "method_id",
    "function_name",
    "confirmations",
]


INSERT_SQL = f"""
INSERT INTO raw.wbtc_transfers ({", ".join(COLUMNS)})
VALUES %s
ON CONFLICT DO NOTHING
RETURNING 1;
"""


def save_transfers_batch(records: List[Dict[str, Any]]) -> int:
    """
    Сохраняет пачку нормализованных транзакций в БД.
    records — это output normalize_wbtc_tx.
    Возвращает количество записей в пачке (для логов).
    """
    if not records:
        return 0

    rows = [
        [rec.get(col) for col in COLUMNS]
        for rec in records
    ]

    conn = get_pg_connection()
    try:
        with conn, conn.cursor() as cur:
            inserted_rows = execute_values(cur, INSERT_SQL, rows, fetch=True)
        return len(inserted_rows) if inserted_rows else 0
    finally:
        conn.close()
