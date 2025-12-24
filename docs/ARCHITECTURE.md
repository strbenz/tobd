# WBTC Whale Monitor — Архитектура

## Конвейер
- Источник данных: Etherscan v2 API (`tokentx` по контракту WBTC).
- Оркестрация: Prefect 2.x, ingestion flow состоит из задач `extract_wbtc_raw` → `transform_wbtc_records` → `load_wbtc_records`; аналитика отдельным flow.
- Хранилище сырых данных: Postgres схема `raw`, таблица `wbtc_transfers` (уникальный ключ tx_hash+contract, индексы по сумме/whale/timestamp).
- Обработка: Dask DataFrame читает `raw.wbtc_transfers`, считает дневные метрики (tx_count, total_volume_wbtc, whale_tx_count, max_tx_volume, top_sender).
- Хранилище витрины: Postgres схема `analytics`, таблица `daily_stats`.
- Визуализация: Grafana поверх Postgres (datasource `db` из docker-compose).

## Потоки Prefect
- `wbtc_whale_ingestion_flow(max_pages=None)`: тянет WBTC из Etherscan, фильтрует пыль < `DUST_THRESHOLD_WBTC_BTC` (0.01 по умолчанию), нормализует с расчётом `is_whale`, пишет батчами в `raw.wbtc_transfers`.
- `wbtc_daily_stats_flow()`: запускает Dask-агрегации и пересобирает `analytics.daily_stats`.
- `wbtc_whale_etl_flow(max_pages=None)`: сквозной сценарий ingestion → analytics.

## Переменные окружения
- `ETHERSCAN_KEYS` — список API ключей через запятую (обязателен).
- `DUST_THRESHOLD_WBTC_BTC` — минимальная сумма для загрузки (default `0.01`).
- `WBTC_WHALE_THRESHOLD_BTC` — порог для флага `is_whale` (default `5`).
- `GAS_ETH_TO_USD` — курс ETH→USD для оценки комиссии (default `26000`).
- `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` — подключение к Postgres (по умолчанию совпадает с docker-compose).

## Развёртывание
1. Создать `.env` с `ETHERSCAN_KEYS` и при необходимости PG/порогами.
2. `docker compose up -d --build` — поднимет Postgres/Grafana/app, выполнит init_db.
3. Выполнить ingestion/analytics/etl командой `python -m src.flows.<flow> ...` внутри контейнера app.
4. Grafana на `http://localhost:3000` (admin/admin), подключить datasource Postgres и импортировать дашборд из `dashboards/wbtc_whale_dashboard.json`.
