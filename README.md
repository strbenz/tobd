## Проект по дисциплине "ТОБД"
## WBTC Whale Monitor

Кратко: тянем транзакции Wrapped Bitcoin из Etherscan, выкидываем пыль, помечаем крупные переводы, храним в Postgres, считаем дневные китовые метрики через Dask, показываем в Grafana. Оркестрация — Prefect 2.x, развёртывание — docker-compose.

### Быстрый запуск
```bash
docker compose up -d --build   # поднимет Postgres, Grafana, app; app выполнит init_db

# Загрузка WBTC (ingestion)
docker compose exec app python -m src.flows.wbtc_whale_ingestion_flow --no-limit  # или --max-pages n

# Пересчёт китовой аналитики
docker compose exec app python -m src.flows.wbtc_daily_stats_flow

# Сквозной ETL
docker compose exec app python -m src.flows.wbtc_whale_etl_flow --no-limit

# Проверить объём данных
docker compose exec db psql -U postgres -d tobd -c "select count(*) from raw.wbtc_transfers;"
docker compose exec db psql -U postgres -d tobd -c "select count(*) from analytics.daily_stats;"
```

### Что есть
- Ingestion: `fetch_wbtc_bulk.py` (Etherscan v2, контракт WBTC, окно 10k, фильтр пыли), flow `wbtc_whale_ingestion_flow` (extract → transform → load).
- Normalization: `normalize_wbtc_tx` — сумма в WBTC, флаг `is_whale` (>= 5 BTC по умолчанию), комиссии в ETH/USD.
- Analytics: `dask_daily_stats.py` — дневные метрики (whale_tx_count, max_tx_volume, top_sender, total_volume_wbtc).
- Хранилище: Postgres схемы `raw` и `analytics`, DDL в `src/db/models.sql`.
- Визуализация: Grafana `http://localhost:3000` (admin/admin), datasource Postgres.

### Переменные окружения
- `ETHERSCAN_KEYS` — обязательный список ключей через запятую.
- `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` — Postgres (по умолчанию как в compose).
- `DUST_THRESHOLD_WBTC_BTC` — отсекаем пыль (default 0.01).
- `WBTC_WHALE_THRESHOLD_BTC` — порог кита (default 5).
- `GAS_ETH_TO_USD` — курс для комиссии (default 26000).

### Grafana
- Datasource: Postgres (`db`, host=db, port=5432, user=postgres, pass=123, db=tobd).
- Дашборд: `dashboards/wbtc_whale_dashboard.json`.

### Полезно знать
- Etherscan отдаёт максимум 10k записей за окно — код сам сдвигает `endblock` и ротирует ключи.
- Пыль (< 0.01 BTC) отбрасывается ещё при загрузке.
- Очистить данные, но оставить схему: `TRUNCATE raw.wbtc_transfers, analytics.daily_stats RESTART IDENTITY;`. Полный сброс — `docker compose down -v`.

### Где почитать
- Архитектура: `docs/ARCHITECTURE.md`.

### Авторы
- Страченков Артём
- Мищенко Александр.
