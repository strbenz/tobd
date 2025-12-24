import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from src.utils.config import load_project_dotenv
from src.db.connection import get_pg_connection

load_project_dotenv()


def rebuild_daily_stats():
    """
    Перестраивает таблицу analytics.daily_stats целиком
    из raw.wbtc_transfers.
    """
    sql = """
    TRUNCATE analytics.daily_stats;

    INSERT INTO analytics.daily_stats (
        date,
        tx_count,
        total_volume_wbtc,
        whale_tx_count,
        max_tx_volume,
        top_sender
    )
    WITH base AS (
        SELECT *
        FROM raw.wbtc_transfers
        WHERE value_wbtc > 0
    ),
    daily AS (
        SELECT
            date(time_stamp) AS date,
            COUNT(*) AS tx_count,
            SUM(value_wbtc) AS total_volume_wbtc,
            SUM(CASE WHEN is_whale THEN 1 ELSE 0 END) AS whale_tx_count,
            MAX(value_wbtc) AS max_tx_volume
        FROM base
        GROUP BY 1
    ),
    sender_rank AS (
        SELECT
            date(time_stamp) AS date,
            from_address,
            SUM(value_wbtc) AS sender_volume,
            ROW_NUMBER() OVER (PARTITION BY date(time_stamp) ORDER BY SUM(value_wbtc) DESC) AS rn
        FROM base
        GROUP BY 1, from_address
    )
    SELECT
        d.date,
        d.tx_count,
        d.total_volume_wbtc,
        d.whale_tx_count,
        d.max_tx_volume,
        sr.from_address AS top_sender
    FROM daily d
    LEFT JOIN sender_rank sr ON d.date = sr.date AND sr.rn = 1
    ORDER BY d.date;
    """

    conn = get_pg_connection()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql)
    finally:
        conn.close()


if __name__ == "__main__":
    rebuild_daily_stats()
    print("daily_stats пересчитана")
