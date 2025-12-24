import importlib

etl_module = importlib.import_module("src.flows.wbtc_whale_etl_flow")


def test_etl_flow_calls_subflows(monkeypatch):
    calls = []

    def fake_raw(max_pages=None):
        calls.append(("raw", max_pages))
        return 5

    def fake_daily():
        calls.append(("daily", None))
        return 3

    monkeypatch.setattr(etl_module, "wbtc_whale_ingestion_flow", fake_raw)
    monkeypatch.setattr(etl_module, "wbtc_daily_stats_flow", fake_daily)

    result = etl_module.wbtc_whale_etl_flow.fn(max_pages=7)

    assert result == {"raw_saved": 5, "daily_rows": 3}
    assert calls == [("raw", 7), ("daily", None)]
