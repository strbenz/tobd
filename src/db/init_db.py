import time
from pathlib import Path
from typing import Iterable, Set

import psycopg2

from src.db.connection import get_pg_connection
from src.db.save_transfers import COLUMNS as RAW_COLUMNS

MODELS_SQL_PATH = Path(__file__).resolve().parent / "models.sql"

# Колонки, которые точно должны быть в raw.wbtc_transfers (без id)
REQUIRED_RAW_COLUMNS: Set[str] = {"id", *RAW_COLUMNS}
REQUIRED_ANALYTICS_COLUMNS: Set[str] = {
    "date",
    "tx_count",
    "total_volume_wbtc",
    "whale_tx_count",
    "max_tx_volume",
    "top_sender",
}


def _fetch_columns(cur, schema: str, table: str) -> Set[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s;
        """,
        (schema, table),
    )
    return {row[0] for row in cur.fetchall()}


def _ensure_table_schema(cur, schema: str, table: str, required: Iterable[str]) -> None:
    existing = _fetch_columns(cur, schema, table)
    required_set = set(required)

    if not existing:
        return  # таблицы нет — создадим ниже общим DDL

    missing = required_set - existing
    if missing:
        print(f"⚠️  Таблица {schema}.{table} не соответствует схеме (нет колонок: {missing}). Пересоздаю.")
        cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{table}" CASCADE;')


def init_db(max_retries: int = 10, retry_delay: int = 2) -> None:
    """
    Инициализирует схемы и таблицы по DDL из models.sql.
    При несовпадении схемы raw.wbtc_transfers/analytics.daily_stats — пересоздаёт таблицы.
    """
    ddl_sql = MODELS_SQL_PATH.read_text()

    for attempt in range(1, max_retries + 1):
        try:
            conn = get_pg_connection()
            conn.autocommit = True
            break
        except psycopg2.OperationalError as e:
            if attempt == max_retries:
                raise
            print(f"⏳ DB не готов ({e}). Повтор через {retry_delay}s [{attempt}/{max_retries}]")
            time.sleep(retry_delay)
    else:
        raise RuntimeError("Не удалось подключиться к БД")

    try:
        with conn, conn.cursor() as cur:
            _ensure_table_schema(cur, "raw", "wbtc_transfers", REQUIRED_RAW_COLUMNS)
            _ensure_table_schema(cur, "analytics", "daily_stats", REQUIRED_ANALYTICS_COLUMNS)

            cur.execute(ddl_sql)

        print("✅ DB init done: схемы raw/analytics и таблицы приведены к актуальной схеме")
    finally:
        conn.close()


if __name__ == "__main__":
    init_db()
