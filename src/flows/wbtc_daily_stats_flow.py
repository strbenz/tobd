import sys
from pathlib import Path

from prefect import flow, task, get_run_logger

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from src.utils.config import load_project_dotenv
from src.analytics.dask_daily_stats import rebuild_daily_stats_with_dask


@task(name="build_whale_daily_stats_with_dask")
def build_whale_daily_stats_with_dask() -> int:
    """
    Пересчитывает analytics.daily_stats через Dask на основе raw.wbtc_transfers.

    Returns:
        Количество строк, записанных в analytics.daily_stats.
    """
    rows = rebuild_daily_stats_with_dask()
    return rows


@flow(name="wbtc_daily_stats_flow")
def wbtc_daily_stats_flow() -> int:
    """
    Flow: пересчитывает таблицу analytics.daily_stats с использованием Dask.

    Returns:
        Количество строк в обновлённой analytics.daily_stats.
    """
    load_project_dotenv()

    logger = get_run_logger()
    logger.info("Старт wbtc_daily_stats_flow")

    rows = build_whale_daily_stats_with_dask()

    logger.info(f"wbtc_daily_stats_flow завершён. Строк записано: {rows}")
    return rows


if __name__ == "__main__":
    wbtc_daily_stats_flow()
