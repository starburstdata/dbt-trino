import time
import unittest
from unittest.mock import MagicMock, patch

import requests

from dbt.adapters.trino.starburst.api_client import (
    CLIENT_INFO_HEADER,
    DEFAULT_MAX_COLUMN_DESCRIPTIONS_PER_REQUEST,
    StarburstDiscoveryClient,
)


class TestStarburstDiscoveryClient(unittest.TestCase):
    def setUp(self):
        self.client = StarburstDiscoveryClient(
            starburst_url="https://my-tenant.galaxy.starburst.io",
            client_id="test-client-id",
            secret_key="test-secret-key",
        )

    def test_base_url(self):
        self.assertEqual(
            self.client.base_url,
            "https://my-tenant.galaxy.starburst.io/public/api/v1",
        )

    def test_token_url(self):
        self.assertEqual(
            self.client._token_url,
            "https://my-tenant.galaxy.starburst.io/oauth/v2/token",
        )

    def test_url_trailing_slash_stripped(self):
        client = StarburstDiscoveryClient(
            starburst_url="https://my-tenant.galaxy.starburst.io/",
            client_id="c",
            secret_key="s",
        )
        self.assertEqual(client.base_url, "https://my-tenant.galaxy.starburst.io/public/api/v1")

    def test_attribution_header_set(self):
        self.assertTrue(self.client._session.headers[CLIENT_INFO_HEADER].startswith("dbt-trino/"))

    # -- Token tests --

    def test_ensure_token_success(self):
        self.client._session = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "access_token": "test-bearer-token",
            "token_type": "Bearer",
            "expires_in": 600,
        }
        mock_response.raise_for_status = MagicMock()
        self.client._session.post.return_value = mock_response

        self.assertTrue(self.client._ensure_token())
        self.assertEqual(self.client._access_token, "test-bearer-token")

    def test_ensure_token_skips_when_valid(self):
        self.client._access_token = "existing-token"
        self.client._token_expires_at = time.time() + 300
        self.client._session = MagicMock()

        self.assertTrue(self.client._ensure_token())
        self.client._session.post.assert_not_called()

    def test_ensure_token_refreshes_when_expired(self):
        self.client._access_token = "old-token"
        self.client._token_expires_at = time.time() - 10
        self.client._session = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {"access_token": "new-token", "expires_in": 600}
        mock_response.raise_for_status = MagicMock()
        self.client._session.post.return_value = mock_response

        self.assertTrue(self.client._ensure_token())
        self.assertEqual(self.client._access_token, "new-token")

    def test_ensure_token_failure(self):
        self.client._session = MagicMock()
        self.client._session.post.side_effect = requests.ConnectionError("refused")
        self.assertFalse(self.client._ensure_token())

    def test_ensure_token_handles_malformed_payload(self):
        self.client._session = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {"token_type": "Bearer"}  # missing access_token
        mock_response.raise_for_status = MagicMock()
        self.client._session.post.return_value = mock_response
        self.assertFalse(self.client._ensure_token())

    def test_ensure_token_handles_non_dict_payload(self):
        self.client._session = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = ["unexpected", "list"]
        mock_response.raise_for_status = MagicMock()
        self.client._session.post.return_value = mock_response
        self.assertFalse(self.client._ensure_token())

    def test_retry_adapter_mounted_for_rate_limiting(self):
        adapter = self.client._session.get_adapter("https://my-tenant.galaxy.starburst.io")
        self.assertIn(429, adapter.max_retries.status_forcelist)
        self.assertTrue(adapter.max_retries.respect_retry_after_header)

    # -- _api_request / _api_get / _api_patch --

    @patch.object(StarburstDiscoveryClient, "_ensure_token", return_value=False)
    def test_api_request_raises_when_token_unavailable(self, mock_token):
        self.client._session = MagicMock()
        with self.assertRaises(requests.RequestException):
            self.client._api_get("/catalog")
        self.client._session.get.assert_not_called()

    @patch.object(StarburstDiscoveryClient, "_ensure_token", return_value=True)
    def test_api_get_returns_extracted_results(self, mock_token):
        self.client._session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"{}"
        mock_response.json.return_value = {
            "nextPageToken": "",
            "result": [{"catalogId": "c-1", "catalogName": "cat1"}],
        }
        mock_response.raise_for_status = MagicMock()
        self.client._session.get.return_value = mock_response

        result = self.client._api_get("/catalog")
        self.assertEqual(result, [{"catalogId": "c-1", "catalogName": "cat1"}])

    @patch.object(StarburstDiscoveryClient, "_ensure_token", return_value=True)
    def test_api_get_handles_list_response(self, mock_token):
        self.client._session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"[]"
        mock_response.json.return_value = [{"id": "1", "name": "x"}]
        mock_response.raise_for_status = MagicMock()
        self.client._session.get.return_value = mock_response

        result = self.client._api_get("/some-endpoint")
        self.assertEqual(result, [{"id": "1", "name": "x"}])

    @patch.object(StarburstDiscoveryClient, "_ensure_token", return_value=True)
    def test_api_request_handles_204_no_content(self, mock_token):
        self.client._session = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 204
        mock_response.content = b""
        mock_response.raise_for_status = MagicMock()
        self.client._session.patch.return_value = mock_response

        result = self.client._api_patch("/some/path", {"key": "value"})
        self.assertEqual(result, [])
        mock_response.json.assert_not_called()

    @patch.object(StarburstDiscoveryClient, "_ensure_token", return_value=True)
    def test_api_get_propagates_request_exception(self, mock_token):
        self.client._session = MagicMock()
        self.client._session.get.side_effect = requests.ConnectionError("refused")

        with self.assertRaises(requests.ConnectionError):
            self.client._api_get("/catalog")

    @patch.object(StarburstDiscoveryClient, "_ensure_token", return_value=True)
    def test_api_patch_propagates_request_exception(self, mock_token):
        self.client._session = MagicMock()
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("404")
        self.client._session.patch.return_value = mock_response

        with self.assertRaises(requests.HTTPError):
            self.client._api_patch("/path", {})

    # -- Catalog ID resolution --

    @patch.object(StarburstDiscoveryClient, "_api_get")
    def test_resolve_catalog_id_success(self, mock_get):
        mock_get.return_value = [
            {"catalogId": "c-111", "catalogName": "other"},
            {"catalogId": "c-222", "catalogName": "my_catalog"},
        ]
        self.assertEqual(self.client._resolve_catalog_id("my_catalog"), "c-222")

    @patch.object(StarburstDiscoveryClient, "_api_get")
    def test_resolve_catalog_id_not_found(self, mock_get):
        mock_get.return_value = [{"catalogId": "c-111", "catalogName": "other"}]
        self.assertIsNone(self.client._resolve_catalog_id("missing"))

    @patch.object(StarburstDiscoveryClient, "_api_get")
    def test_resolve_catalog_id_propagates_request_error(self, mock_get):
        mock_get.side_effect = requests.ConnectionError("refused")
        with self.assertRaises(requests.ConnectionError):
            self.client._resolve_catalog_id("my_catalog")

    @patch.object(StarburstDiscoveryClient, "_api_get")
    def test_catalog_id_cached(self, mock_get):
        mock_get.return_value = [{"catalogId": "c-222", "catalogName": "my_catalog"}]
        self.client._get_catalog_id("my_catalog")
        self.client._get_catalog_id("my_catalog")
        mock_get.assert_called_once()

    @patch.object(StarburstDiscoveryClient, "_api_get")
    def test_get_catalog_id_returns_none_and_does_not_cache_on_request_error(self, mock_get):
        mock_get.side_effect = requests.ConnectionError("refused")
        self.assertIsNone(self.client._get_catalog_id("my_catalog"))
        self.assertNotIn("my_catalog", self.client._catalog_ids)

        # A later, successful call for the same catalog should retry, not
        # stay silently disabled because of the earlier transient failure.
        mock_get.side_effect = None
        mock_get.return_value = [{"catalogId": "c-222", "catalogName": "my_catalog"}]
        self.assertEqual(self.client._get_catalog_id("my_catalog"), "c-222")

    # -- update_table_description --

    @patch.object(StarburstDiscoveryClient, "_api_patch", return_value=[])
    @patch.object(StarburstDiscoveryClient, "_get_catalog_id", return_value="c-222")
    def test_update_table_description(self, mock_cat, mock_patch):
        self.client.update_table_description("my_catalog", "my_schema", "my_table", "A test table")
        mock_cat.assert_called_with("my_catalog")
        mock_patch.assert_called_once_with(
            "/catalog/c-222/schema/my_schema/table/my_table",
            {"description": "A test table"},
        )

    @patch.object(StarburstDiscoveryClient, "_get_catalog_id", return_value=None)
    def test_update_table_returns_none_when_catalog_missing(self, mock_cat):
        self.assertIsNone(self.client.update_table_description("bad", "s", "t", "desc"))

    @patch.object(StarburstDiscoveryClient, "_api_patch")
    @patch.object(StarburstDiscoveryClient, "_get_catalog_id", return_value="c-222")
    def test_update_table_propagates_request_error(self, mock_cat, mock_patch):
        mock_patch.side_effect = requests.HTTPError("500")
        with self.assertRaises(requests.HTTPError):
            self.client.update_table_description("cat", "s", "t", "desc")

    @patch.object(StarburstDiscoveryClient, "_api_patch", return_value=[])
    @patch.object(StarburstDiscoveryClient, "_get_catalog_id", return_value="c-222")
    def test_update_table_description_encodes_schema_and_table(self, mock_cat, mock_patch):
        self.client.update_table_description("my_catalog", "s/c#h?ema", "ta/ble", "desc")
        mock_patch.assert_called_once_with(
            "/catalog/c-222/schema/s%2Fc%23h%3Fema/table/ta%2Fble",
            {"description": "desc"},
        )

    # -- update_column_descriptions (batch) --

    @patch.object(StarburstDiscoveryClient, "_api_patch", return_value=[])
    @patch.object(StarburstDiscoveryClient, "_get_catalog_id", return_value="c-222")
    def test_update_column_descriptions_batch(self, mock_cat, mock_patch):
        self.client.update_column_descriptions(
            "my_catalog",
            "my_schema",
            "my_table",
            {"col_a": "desc a", "col_b": "desc b"},
        )
        mock_patch.assert_called_once_with(
            "/catalog/c-222/schema/my_schema/table/my_table/column",
            {"descriptions": {"col_a": "desc a", "col_b": "desc b"}},
        )

    def test_default_max_column_batch_size(self):
        self.assertEqual(
            self.client._max_column_batch_size,
            DEFAULT_MAX_COLUMN_DESCRIPTIONS_PER_REQUEST,
        )

    @patch.object(StarburstDiscoveryClient, "_api_patch", return_value=[])
    @patch.object(StarburstDiscoveryClient, "_get_catalog_id", return_value="c-222")
    def test_update_column_descriptions_chunks_over_limit(self, mock_cat, mock_patch):
        descriptions = {
            f"col{i}": f"desc{i}" for i in range(DEFAULT_MAX_COLUMN_DESCRIPTIONS_PER_REQUEST + 5)
        }
        self.client.update_column_descriptions("cat", "s", "t", descriptions)
        # 105 columns -> two chunks (100 + 5)
        self.assertEqual(mock_patch.call_count, 2)
        first_chunk = mock_patch.call_args_list[0].args[1]["descriptions"]
        second_chunk = mock_patch.call_args_list[1].args[1]["descriptions"]
        self.assertEqual(len(first_chunk), DEFAULT_MAX_COLUMN_DESCRIPTIONS_PER_REQUEST)
        self.assertEqual(len(second_chunk), 5)

    @patch.object(StarburstDiscoveryClient, "_api_patch", return_value=[])
    @patch.object(StarburstDiscoveryClient, "_get_catalog_id", return_value="c-222")
    def test_update_column_descriptions_respects_custom_batch_size(self, mock_cat, mock_patch):
        client = StarburstDiscoveryClient(
            starburst_url="https://my-tenant.galaxy.starburst.io",
            client_id="c",
            secret_key="s",
            max_column_batch_size=2,
        )
        descriptions = {f"col{i}": f"desc{i}" for i in range(5)}
        client.update_column_descriptions("cat", "s", "t", descriptions)
        # 5 columns with batch size 2 -> three chunks (2 + 2 + 1)
        self.assertEqual(mock_patch.call_count, 3)
        chunk_sizes = [len(call.args[1]["descriptions"]) for call in mock_patch.call_args_list]
        self.assertEqual(chunk_sizes, [2, 2, 1])

    @patch.object(StarburstDiscoveryClient, "_api_patch")
    @patch.object(StarburstDiscoveryClient, "_get_catalog_id", return_value="c-222")
    def test_update_column_descriptions_skips_empty_map(self, mock_cat, mock_patch):
        self.client.update_column_descriptions("cat", "s", "t", {})
        mock_patch.assert_not_called()

    @patch.object(StarburstDiscoveryClient, "_get_catalog_id", return_value=None)
    def test_update_column_descriptions_noop_when_catalog_missing(self, mock_cat):
        # Should not raise even though no catalog id resolves.
        self.client.update_column_descriptions("bad", "s", "t", {"c": "d"})

    @patch.object(StarburstDiscoveryClient, "_api_patch")
    @patch.object(StarburstDiscoveryClient, "_get_catalog_id", return_value="c-222")
    def test_update_column_descriptions_propagates_request_error(self, mock_cat, mock_patch):
        mock_patch.side_effect = requests.HTTPError("500")
        with self.assertRaises(requests.HTTPError):
            self.client.update_column_descriptions("cat", "s", "t", {"c": "d"})

    @patch.object(StarburstDiscoveryClient, "_api_patch", return_value=[])
    @patch.object(StarburstDiscoveryClient, "_get_catalog_id", return_value="c-222")
    def test_update_column_descriptions_encodes_schema_and_table(self, mock_cat, mock_patch):
        self.client.update_column_descriptions("my_catalog", "s/c#h?ema", "ta/ble", {"c": "d"})
        mock_patch.assert_called_once_with(
            "/catalog/c-222/schema/s%2Fc%23h%3Fema/table/ta%2Fble/column",
            {"descriptions": {"c": "d"}},
        )


if __name__ == "__main__":
    unittest.main()
