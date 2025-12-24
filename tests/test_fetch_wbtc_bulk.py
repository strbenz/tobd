import os
import sys
import types
import unittest
from typing import Dict, List
from unittest.mock import patch

try:
    import requests  # type: ignore
except ImportError:
    requests = types.SimpleNamespace(get=lambda *args, **kwargs: None)  # type: ignore
    sys.modules["requests"] = requests

if "dotenv" not in sys.modules:
    sys.modules["dotenv"] = types.SimpleNamespace(load_dotenv=lambda *args, **kwargs: None)

from src.blockchain.fetch_wbtc_bulk import END_BLOCK, fetch_wbtc_all


class FetchWbtcBulkTests(unittest.TestCase):
    def setUp(self) -> None:
        self._orig_keys = os.environ.get("ETHERSCAN_KEYS")
        os.environ["ETHERSCAN_KEYS"] = "k1,k2"

    def tearDown(self) -> None:
        if self._orig_keys is None:
            os.environ.pop("ETHERSCAN_KEYS", None)
        else:
            os.environ["ETHERSCAN_KEYS"] = self._orig_keys

    @staticmethod
    def _fake_response(payload: Dict) -> object:
        class FakeResp:
            def __init__(self, data: Dict):
                self._data = data
                self.status_code = 200

            def json(self) -> Dict:
                return self._data

        return FakeResp(payload)

    def test_shifts_endblock_after_window_limit_error(self) -> None:
        responses: List[Dict] = [
            {"status": "1", "result": [{"blockNumber": "105", "value": "1000000"}, {"blockNumber": "104", "value": "1000000"}]},
            {"status": "0", "message": "Result window is too large", "result": []},
            {"status": "1", "result": [{"blockNumber": "103", "value": "1000000"}, {"blockNumber": "102", "value": "1000000"}]},
            {"status": "1", "result": []},
        ]
        calls: List[Dict] = []

        def fake_get(url: str, params=None, timeout=None):
            calls.append(dict(params))
            return self._fake_response(responses.pop(0))

        with patch("src.blockchain.fetch_wbtc_bulk.requests.get", side_effect=fake_get):
            with patch("src.blockchain.fetch_wbtc_bulk.time.sleep", return_value=None):
                blocks = [tx["blockNumber"] for tx in fetch_wbtc_all(offset=2, start_block=0)]

        self.assertEqual(blocks, ["105", "104", "103", "102"])
        self.assertGreaterEqual(len(calls), 3)
        self.assertEqual(int(calls[0]["endblock"]), END_BLOCK)
        self.assertEqual(int(calls[2]["endblock"]), 103)

    def test_switches_api_key_on_error(self) -> None:
        responses: List[Dict] = [
            {"status": "0", "message": "NOTOK"},
            {"status": "1", "result": [{"blockNumber": "5", "value": "1000000"}]},
            {"status": "1", "result": []},
        ]
        keys_used: List[str] = []

        def fake_get(url: str, params=None, timeout=None):
            keys_used.append(params["apikey"])
            return self._fake_response(responses.pop(0))

        with patch("src.blockchain.fetch_wbtc_bulk.requests.get", side_effect=fake_get):
            with patch("src.blockchain.fetch_wbtc_bulk.time.sleep", return_value=None):
                blocks = [tx["blockNumber"] for tx in fetch_wbtc_all(offset=1, start_block=0)]

        self.assertEqual(blocks, ["5"])
        self.assertEqual(keys_used[:2], ["k1", "k2"])

    def test_respects_max_pages_limit(self) -> None:
        responses: List[Dict] = [
            {"status": "1", "result": [{"blockNumber": "10", "value": "1000000"}]},
            {"status": "1", "result": [{"blockNumber": "9", "value": "1000000"}]},
            {"status": "1", "result": []},
        ]
        call_count = 0

        def fake_get(url: str, params=None, timeout=None):
            nonlocal call_count
            call_count += 1
            return self._fake_response(responses.pop(0))

        with patch("src.blockchain.fetch_wbtc_bulk.requests.get", side_effect=fake_get):
            with patch("src.blockchain.fetch_wbtc_bulk.time.sleep", return_value=None):
                blocks = [
                    tx["blockNumber"]
                    for tx in fetch_wbtc_all(offset=1, max_pages=1, start_block=0)
                ]

        self.assertEqual(blocks, ["10"])
        self.assertEqual(call_count, 1)

    def test_filters_dust_transactions(self) -> None:
        responses: List[Dict] = [
            {
                "status": "1",
                "result": [
                    {"blockNumber": "10", "value": str(1_000_000)},  # 0.01 WBTC (with 8 decimals)
                    {"blockNumber": "9", "value": "100"},  # dust
                ],
            },
            {"status": "1", "result": []},
        ]

        def fake_get(url: str, params=None, timeout=None):
            return self._fake_response(responses.pop(0))

        with patch("src.blockchain.fetch_wbtc_bulk.requests.get", side_effect=fake_get):
            with patch("src.blockchain.fetch_wbtc_bulk.time.sleep", return_value=None):
                blocks = [tx["blockNumber"] for tx in fetch_wbtc_all(offset=2, start_block=0)]

        self.assertEqual(blocks, ["10"])


if __name__ == "__main__":
    unittest.main()
