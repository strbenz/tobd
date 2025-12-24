CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS raw.wbtc_transfers (
    id                  BIGSERIAL PRIMARY KEY,

    -- базовая идентификация
    tx_hash             VARCHAR(66) NOT NULL,
    block_number        BIGINT      NOT NULL,
    block_hash          VARCHAR(66) NOT NULL,
    time_stamp          TIMESTAMPTZ NOT NULL,

    nonce               BIGINT,
    transaction_index   INTEGER,

    from_address        VARCHAR(64) NOT NULL,
    to_address          VARCHAR(64) NOT NULL,

    -- токен
    contract_address    VARCHAR(64) NOT NULL,
    token_name          TEXT        NOT NULL,
    token_symbol        TEXT        NOT NULL,
    token_decimal       INTEGER     NOT NULL,

    value_raw           NUMERIC(78, 0) NOT NULL,
    value_wbtc          NUMERIC(38, 8) NOT NULL,
    is_whale            BOOLEAN      NOT NULL DEFAULT FALSE,

    -- газ / комиссия
    gas_limit           BIGINT,
    gas_price_wei       NUMERIC(78, 0),
    gas_used            BIGINT,
    cumulative_gas_used NUMERIC(78, 0),
    tx_fee_eth          NUMERIC(38, 18),
    tx_fee_usd          NUMERIC(38, 2),

    -- вызов
    input               TEXT,
    method_id           VARCHAR(18),
    function_name       TEXT,

    confirmations       BIGINT,

    CONSTRAINT ux_wbtc_transfer UNIQUE (tx_hash, contract_address)
);

CREATE INDEX IF NOT EXISTS idx_wbtc_value_desc ON raw.wbtc_transfers (value_wbtc DESC);
CREATE INDEX IF NOT EXISTS idx_wbtc_is_whale ON raw.wbtc_transfers (is_whale) WHERE is_whale = TRUE;
CREATE INDEX IF NOT EXISTS idx_wbtc_timestamp ON raw.wbtc_transfers (time_stamp);

CREATE TABLE IF NOT EXISTS analytics.daily_stats (
    date                DATE PRIMARY KEY,
    tx_count            BIGINT      NOT NULL,
    total_volume_wbtc   NUMERIC(38, 8) NOT NULL,
    whale_tx_count      BIGINT      NOT NULL,
    max_tx_volume       NUMERIC(38, 8),
    top_sender          TEXT
);
