import logging
import sys
from pathlib import Path
from typing import Optional, Dict, Any

from prefect import flow, get_run_logger
from prefect.exceptions import MissingContextError

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from src.utils.config import load_project_dotenv
from src.flows.wbtc_whale_ingestion_flow import wbtc_whale_ingestion_flow, DEFAULT_MAX_PAGES
from src.flows.wbtc_daily_stats_flow import wbtc_daily_stats_flow


def _safe_logger():
    try:
        return get_run_logger()
    except MissingContextError:
        return logging.getLogger(__name__)


@flow(name="wbtc_whale_etl_flow")
def wbtc_whale_etl_flow(max_pages: Optional[int] = DEFAULT_MAX_PAGES) -> Dict[str, Any]:
    """
    Сквозной ETL:
    1) Загружает сырые транзакции WBTC в raw.wbtc_transfers.
    2) Пересчитывает analytics.daily_stats через Dask.

    Args:
        max_pages: ограничение на количество страниц для загрузки с Etherscan (None — без лимита).

    Returns:
        Словарь с результатами стадий: {"raw_saved": int, "daily_rows": int}.
    """
    load_project_dotenv()
    logger = _safe_logger()
    logger.info(f"Старт wbtc_whale_etl_flow (max_pages={max_pages})")

    raw_saved = wbtc_whale_ingestion_flow(max_pages=max_pages)
    logger.info(f"Ingestion завершён: сохранено {raw_saved} транзакций")

    daily_rows = wbtc_daily_stats_flow()
    logger.info(f"Analytics завершена: строк в analytics.daily_stats = {daily_rows}")

    result = {"raw_saved": raw_saved, "daily_rows": daily_rows}
    logger.info(f"wbtc_whale_etl_flow завершён: {result}")
    return result


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Сквозной ETL: ingestion + analytics")
    parser.add_argument(
        "--max-pages",
        type=int,
        default=DEFAULT_MAX_PAGES,
        help="Максимум страниц Etherscan для ingestion (None — без лимита)",
    )
    parser.add_argument(
        "--no-limit",
        action="store_true",
        help="Не ограничивать число страниц (эквивалент max_pages=None)",
    )

    args = parser.parse_args()
    max_pages_arg = None if args.no_limit else args.max_pages

    wbtc_whale_etl_flow(max_pages=max_pages_arg)
