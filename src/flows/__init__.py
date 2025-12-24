"""
Prefect flows for the WBTC Whale Monitor.
Exports individual modules for convenient imports in tests and CLI.
"""

from .wbtc_whale_etl_flow import wbtc_whale_etl_flow  # noqa: F401
from .wbtc_whale_ingestion_flow import wbtc_whale_ingestion_flow  # noqa: F401
from .wbtc_daily_stats_flow import wbtc_daily_stats_flow  # noqa: F401
