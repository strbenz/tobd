import os
import sys
from pathlib import Path

import dask.dataframe as dd
from sqlalchemy import create_engine

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from src.utils.config import load_project_dotenv

load_project_dotenv()


def make_pg_url() -> str:
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    db   = os.getenv("PGDATABASE", "tobd")
    user = os.getenv("PGUSER", "postgres")
    pwd  = os.getenv("PGPASSWORD", "postgres")

    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"


def rebuild_daily_stats_with_dask() -> int:
    """
    Читаем сырые транзакции WBTC из raw.wbtc_transfers с помощью Dask,
    считаем дневные метрики для мониторинга китов и пишем в analytics.daily_stats.
    Возвращает количество строк, записанных в analytics.daily_stats.
    """

    pg_url = make_pg_url()
    engine = create_engine(pg_url)

    table_name = "wbtc_transfers"

    ddf = dd.read_sql_table(
        table_name=table_name,
        con=pg_url,
        index_col="id",
        schema="raw",
        npartitions=8,  # можно подобрать под свою машину
    )

    # отбрасываем пустые и дефектные записи
    ddf = ddf[ddf["value_wbtc"] > 0]

    # приводим time_stamp к дате
    ddf["date"] = ddf["time_stamp"].dt.date

    daily = ddf.groupby("date").agg(
        tx_count=("tx_hash", "count"),
        total_volume_wbtc=("value_wbtc", "sum"),
        whale_tx_count=("is_whale", "sum"),
        max_tx_volume=("value_wbtc", "max"),
    )

    sender_volume = (
        ddf[["date", "from_address", "value_wbtc"]]
        .groupby(["date", "from_address"])
        .agg(total_from_value=("value_wbtc", "sum"))
    )

    # Dask → pandas
    daily_pd = daily.compute().reset_index()
    sender_pd = sender_volume.compute().reset_index()

    # топ-10 отправителей на день
    top_senders = (
        sender_pd.sort_values(["date", "total_from_value"], ascending=[True, False])
        .groupby("date")
        .head(10)
    )
    top_sender = (
        top_senders.groupby("date")
        .first()
        .reset_index()[["date", "from_address"]]
        .rename(columns={"from_address": "top_sender"})
    )

    daily_pd = daily_pd.merge(top_sender, on="date", how="left")
    daily_pd["tx_count"] = daily_pd["tx_count"].astype(int)
    daily_pd["whale_tx_count"] = daily_pd["whale_tx_count"].astype(int)

    # на всякий случай сортируем
    daily_pd = daily_pd.sort_values("date")

    # пишем в Postgres
    # if_exists="replace": полностью пересобираем витрину
    daily_pd.to_sql(
        name="daily_stats",
        con=engine,
        schema="analytics",
        if_exists="replace",
        index=False,
    )

    rows_written = len(daily_pd)
    print(f"Записано строк в analytics.daily_stats: {rows_written}")
    return rows_written


if __name__ == "__main__":
    rebuild_daily_stats_with_dask()
