import sys
from pathlib import Path
from typing import Optional, List, Dict

from prefect import flow, task, get_run_logger

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from src.utils.config import load_project_dotenv
from src.blockchain.fetch_wbtc_bulk import fetch_wbtc_all
from src.blockchain.normalize import normalize_wbtc_tx
from src.db.save_transfers import save_transfers_batch


DEFAULT_MAX_PAGES = 10
DEFAULT_BATCH_SIZE = 1000


@task(name="extract_wbtc_raw")
def extract_wbtc_raw(max_pages: Optional[int] = DEFAULT_MAX_PAGES) -> List[Dict]:
    """
    Extract: выгружает сырые WBTC транзакции из Etherscan.
    """
    return list(fetch_wbtc_all(max_pages=max_pages))


@task(name="transform_wbtc_records")
def transform_wbtc_records(raw_txs: List[Dict]) -> List[Dict]:
    """
    Transform: нормализация и расчёт whale-флага/комиссий.
    """
    return [normalize_wbtc_tx(tx) for tx in raw_txs]


@task(name="load_wbtc_records")
def load_wbtc_records(records: List[Dict], batch_size: int = DEFAULT_BATCH_SIZE) -> int:
    """
    Load: батчево сохраняет нормализованные транзакции в raw.wbtc_transfers.
    """
    logger = get_run_logger()
    saved_total = 0
    buffer: List[Dict] = []

    for rec in records:
        buffer.append(rec)
        if len(buffer) >= batch_size:
            inserted = save_transfers_batch(buffer)
            saved_total += inserted
            logger.info(f"Сохранил пачку: {inserted} записей (итого {saved_total})")
            buffer.clear()

    if buffer:
        inserted = save_transfers_batch(buffer)
        saved_total += inserted
        logger.info(f"Сохранил финальную пачку: {inserted} записей (итого {saved_total})")
        buffer.clear()

    return saved_total


@flow(name="wbtc_whale_ingestion_flow")
def wbtc_whale_ingestion_flow(max_pages: Optional[int] = DEFAULT_MAX_PAGES) -> int:
    """
    Flow: подтягивает новые транзакции WBTC и складывает в raw.wbtc_transfers.

    Args:
        max_pages: ограничение на количество страниц Etherscan (None — без лимита).

    Returns:
        Количество сохранённых транзакций.
    """
    load_project_dotenv()

    logger = get_run_logger()
    logger.info(f"Старт wbtc_whale_ingestion_flow (max_pages={max_pages})")

    raw_txs = extract_wbtc_raw(max_pages=max_pages)
    normalized = transform_wbtc_records(raw_txs)
    saved = load_wbtc_records(normalized)

    logger.info(f"wbtc_whale_ingestion_flow завершён. Сохранено транзакций: {saved}")
    return saved


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Запуск wbtc_whale_ingestion_flow")
    parser.add_argument(
        "--max-pages",
        type=int,
        default=DEFAULT_MAX_PAGES,
        help="Максимум страниц Etherscan (None — без лимита)",
    )
    parser.add_argument(
        "--no-limit",
        action="store_true",
        help="Не ограничивать число страниц (эквивалент max_pages=None)",
    )

    args = parser.parse_args()
    max_pages_arg = None if args.no_limit else args.max_pages

    wbtc_whale_ingestion_flow(max_pages=max_pages_arg)
