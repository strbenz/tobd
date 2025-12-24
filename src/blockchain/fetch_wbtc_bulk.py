import os
import sys
import time
from pathlib import Path
from typing import List, Dict, Generator, Optional, Tuple

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from src.utils.config import load_project_dotenv

load_project_dotenv()

BATCH_SIZE = 5000

WBTC_CONTRACT = "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"
ETHERSCAN_URL = "https://api.etherscan.io/v2/api"
CHAIN_ID = 1
PAGE_SIZE = 5000
END_BLOCK = 9_999_999_999
SORT_ORDER = "desc"
REQUEST_DELAY_SEC = 0.5
MAX_RESULTS_PER_WINDOW = 10_000
TOKEN_DECIMALS = 8


def load_api_keys() -> List[str]:
    """
    Берёт ключи из ENV:
    ETHERSCAN_KEYS="key1,key2,key3"
    """
    raw = os.getenv("ETHERSCAN_KEYS")
    if not raw:
        raise RuntimeError("Нет ключей: установи ETHERSCAN_KEYS='k1,k2,k3'")
    return [k.strip() for k in raw.split(",") if k.strip()]


def dust_threshold_raw() -> int:
    """
    Минимальная сумма WBTC для загрузки в минимальных единицах токена.
    ENV DUST_THRESHOLD_WBTC_BTC=0.01 по умолчанию.
    """
    raw_threshold = os.getenv("DUST_THRESHOLD_WBTC_BTC", "0.01")
    try:
        btc_threshold = float(raw_threshold)
    except ValueError:
        btc_threshold = 0.01
    return int(btc_threshold * (10 ** TOKEN_DECIMALS))


def make_request(
    api_key: str,
    page: int,
    start_block: int,
    end_block: int,
    offset: int = PAGE_SIZE,
) -> Tuple[Optional[List[Dict]], bool, str]:
    """
    Делает запрос к v2 API.

    Returns:
        (txs, window_too_large, message)
        txs: список транзакций или None при ошибке.
        window_too_large: True, если Etherscan вернул ошибку "Result window is too large".
        message: сообщение об ошибке (если есть).
    """

    params = {
        "apikey": api_key,
        "chainid": CHAIN_ID,
        "module": "account",
        "action": "tokentx",
        "contractaddress": WBTC_CONTRACT,
        "page": page,
        "offset": offset,
        "sort": SORT_ORDER,
        "startblock": start_block,
        "endblock": end_block,
    }

    try:
        resp = requests.get(ETHERSCAN_URL, params=params, timeout=20)
        if resp.status_code >= 500:
            # не тратим ключ на 5xx
            print(f"[{api_key}] Ошибка HTTP {resp.status_code}, повтор позже")
            return None, False, f"HTTP_{resp.status_code}"

        data = resp.json()

        message = str(data.get("message") or "")
        window_too_large = "Result window is too large" in message

        # API Etherscan v2 при лимитах возвращает status != "1"
        if data.get("status") != "1":
            print(f"[{api_key}] Ошибка Etherscan: {message}")
            return None, window_too_large, message

        result = data.get("result")
        if not isinstance(result, list):
            return None, window_too_large, message

        return result, window_too_large, message

    except Exception as e:
        print(f"[{api_key}] exception: {e}")
        return None, False, str(e)


def _is_not_dust(tx: Dict) -> bool:
    """
    Фильтр против "пыли": value меньше 0.01 BTC не интересен для мониторинга китов.
    """
    try:
        raw_value = int(tx.get("value", "0"))
    except (TypeError, ValueError):
        return False

    # value записан в минимальных единицах токена (10 ** TOKEN_DECIMALS)
    return raw_value >= dust_threshold_raw()


def fetch_wbtc_all(
    offset: int = PAGE_SIZE,
    max_empty_pages: int = 5,
    max_pages: Optional[int] = None,
    start_block: int = 0,
) -> Generator[Dict, None, None]:
    """
    Тянет максимум транзакций, учитывая ограничение окна Etherscan (10k результатов).
    - Переключается между API-ключами при rate-limit ошибках.
    - Идёт постранично, при достижении лимита окна сдвигает endblock до последнего
      загруженного блока и начинает новую "окно" с page=1.
    - При max_pages ограничивает количество запросов (страниц), вне зависимости
      от перезапуска окна.
    """

    api_keys = load_api_keys()
    key_index = 0
    page = 1
    current_end_block = END_BLOCK
    last_fetched_block: Optional[int] = None
    requests_made = 0
    max_page_per_window = max(1, MAX_RESULTS_PER_WINDOW // offset)

    while True:
        if max_pages is not None and requests_made >= max_pages:
            print(f"⛔ Достигнут лимит страниц ({max_pages}). Останавливаюсь.")
            return

        current_key = api_keys[key_index]

        print(f"→ page {page}, key={current_key}, endblock={current_end_block}")

        txs, window_too_large, message = make_request(
            current_key,
            page=page,
            start_block=start_block,
            end_block=current_end_block,
            offset=offset,
        )
        requests_made += 1

        if window_too_large:
            if last_fetched_block is None:
                print("Окно слишком большое, но нет сохраненного блока для сдвига.")
                return

            current_end_block = max(start_block, last_fetched_block - 1)
            page = 1
            print(f"Сдвигаю endblock до {current_end_block} из-за ограничения окна.")
            time.sleep(REQUEST_DELAY_SEC)
            continue

        if txs is None:
            # ключ отвалился — переключаем на следующий
            key_index += 1

            if key_index >= len(api_keys):
                print("❌ Все API-ключи закончились")
                return

            print(f"⚠️ Переключаюсь на ключ: {api_keys[key_index]}")
            time.sleep(REQUEST_DELAY_SEC)
            continue

        # если API вернул пустой список
        if len(txs) == 0:
            print(f"Больше транзакций нет. Сообщение API: {message}")
            return

        # отдаём транзакции наружу
        for tx in txs:
            # отбрасываем "пыль" прямо на этапе скачивания
            if _is_not_dust(tx):
                yield tx

        # блок последней транзакции в текущей странице
        last_block_raw = txs[-1].get("blockNumber")
        try:
            last_block_number = int(last_block_raw)
        except (TypeError, ValueError):
            last_block_number = None

        if last_block_number is not None:
            last_fetched_block = last_block_number

        fetched_everything = len(txs) < offset or (
            last_block_number is not None and last_block_number <= start_block
        )
        if fetched_everything:
            print("Достигнут конец диапазона.")
            return

        next_page = page + 1
        would_exceed_window = (
            next_page * offset > MAX_RESULTS_PER_WINDOW or next_page > max_page_per_window
        )
        if would_exceed_window:
            if last_fetched_block is None:
                print("Не удалось определить последний блок для сдвига окна.")
                return

            current_end_block = max(start_block, last_fetched_block - 1)
            page = 1
            print(
                f"Достигнут предел окна ({MAX_RESULTS_PER_WINDOW}). "
                f"Сдвигаю endblock до {current_end_block}."
            )
            time.sleep(REQUEST_DELAY_SEC)
            continue

        # следующая страница в текущем окне
        page = next_page
        time.sleep(REQUEST_DELAY_SEC)  # чтобы не бомбить API


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Загрузить WBTC транзакции из Etherscan")
    parser.add_argument("--start-block", type=int, default=0, help="С какого блока начинать (включительно)")
    parser.add_argument("--max-pages", type=int, default=None, help="Лимит запросов (страниц) к Etherscan")
    parser.add_argument("--offset", type=int, default=PAGE_SIZE, help="Размер страницы для Etherscan")
    parser.add_argument(
        "--save",
        action="store_true",
        help="Сохранять в БД (по умолчанию только вывод в консоль)",
    )
    args = parser.parse_args()

    if args.save:
        print("Старт массовой загрузки WBTC транзакций и сохранения в БД...\n")
        from src.blockchain.normalize import normalize_wbtc_tx
        from src.db.save_transfers import save_transfers_batch

        buffer = []

        for raw in fetch_wbtc_all(
            offset=args.offset,
            max_pages=args.max_pages,
            start_block=args.start_block,
        ):
            norm = normalize_wbtc_tx(raw)

            # защита от мусора после нормализации
            if norm["value_wbtc"] <= 0:
                continue

            buffer.append(norm)
            if len(buffer) % 10 == 0:
                print(f"  • В буфере {len(buffer)} транзакций")

            if len(buffer) >= BATCH_SIZE:
                try:
                    inserted = save_transfers_batch(buffer)
                    print(f"Сохранил пачку: {inserted} записей (буфер {len(buffer)})")
                except Exception as e:
                    print(f"❌ Ошибка сохранения пачки: {e}")
                finally:
                    buffer.clear()

        # финальный хвост
        if buffer:
            try:
                inserted = save_transfers_batch(buffer)
                print(f"Сохранил финальную пачку: {inserted} записей (буфер {len(buffer)})")
            except Exception as e:
                print(f"❌ Ошибка сохранения финальной пачки: {e}")
            finally:
                buffer.clear()

        print("Готово.")
    else:
        print("Старт загрузки WBTC транзакций (только вывод в консоль)...\n")
        count = 0
        for raw in fetch_wbtc_all(
            offset=args.offset,
            max_pages=args.max_pages,
            start_block=args.start_block,
        ):
            count += 1
            print(raw)
        print(f"\nГотово. Всего получено {count} транзакций.")
