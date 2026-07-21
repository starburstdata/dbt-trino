import unittest
from unittest.mock import MagicMock, patch

import requests
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.trino.starburst.catalog_sync import StarburstCatalogSync


class TestStarburstCatalogSync(unittest.TestCase):
    def _make_credentials(self, **overrides):
        creds = MagicMock()
        creds.starburst_url = "https://my-tenant.galaxy.starburst.io"
        creds.starburst_client_id = "test-client-id"
        creds.starburst_secret_key = "test-secret-key"
        creds.starburst_metadata_failure_strategy = "continue_on_error"
        creds.starburst_max_column_batch_size = 100
        for k, v in overrides.items():
            setattr(creds, k, v)
        return creds

    def test_init_extracts_credentials(self):
        creds = self._make_credentials()
        sync = StarburstCatalogSync(creds)
        self.assertEqual(sync._starburst_url, "https://my-tenant.galaxy.starburst.io")
        self.assertEqual(sync._client_id, "test-client-id")
        self.assertEqual(sync._secret_key, "test-secret-key")
        self.assertEqual(sync._failure_strategy, "continue_on_error")

    def test_failure_strategy_defaults_to_continue(self):
        creds = self._make_credentials(starburst_metadata_failure_strategy=None)
        sync = StarburstCatalogSync(creds)
        self.assertEqual(sync._failure_strategy, "continue_on_error")

    @patch("dbt.adapters.trino.starburst.catalog_sync.StarburstDiscoveryClient")
    def test_lazy_client_initialization(self, mock_client_cls):
        creds = self._make_credentials()
        sync = StarburstCatalogSync(creds)

        mock_client_cls.assert_not_called()
        _ = sync.client

        mock_client_cls.assert_called_once_with(
            starburst_url="https://my-tenant.galaxy.starburst.io",
            client_id="test-client-id",
            secret_key="test-secret-key",
            max_column_batch_size=100,
        )

    @patch("dbt.adapters.trino.starburst.catalog_sync.StarburstDiscoveryClient")
    def test_custom_max_column_batch_size_passed_to_client(self, mock_client_cls):
        creds = self._make_credentials(starburst_max_column_batch_size=250)
        sync = StarburstCatalogSync(creds)
        _ = sync.client
        self.assertEqual(mock_client_cls.call_args.kwargs["max_column_batch_size"], 250)

    def test_max_column_batch_size_defaults_when_unset(self):
        creds = self._make_credentials(starburst_max_column_batch_size=None)
        sync = StarburstCatalogSync(creds)
        self.assertEqual(sync._max_column_batch_size, 100)

    def test_client_returns_none_without_client_id(self):
        creds = self._make_credentials(starburst_client_id=None)
        sync = StarburstCatalogSync(creds)
        self.assertIsNone(sync.client)

    def test_client_returns_none_without_secret_key(self):
        creds = self._make_credentials(starburst_secret_key=None)
        sync = StarburstCatalogSync(creds)
        self.assertIsNone(sync.client)

    @patch("dbt.adapters.trino.starburst.catalog_sync.StarburstDiscoveryClient")
    def test_sync_relation_description(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        sync = StarburstCatalogSync(self._make_credentials())
        sync.sync_relation_description("my_catalog", "my_schema", "my_table", "A great table")

        mock_client.update_table_description.assert_called_once_with(
            "my_catalog", "my_schema", "my_table", "A great table"
        )

    @patch("dbt.adapters.trino.starburst.catalog_sync.StarburstDiscoveryClient")
    def test_sync_column_descriptions_batches_non_empty(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        sync = StarburstCatalogSync(self._make_credentials())
        columns = {
            "id": {"description": "Primary key", "name": "id"},
            "name": {"description": "User name", "name": "name"},
            "empty_col": {"description": "", "name": "empty_col"},
            "no_desc": {"name": "no_desc"},
        }
        sync.sync_column_descriptions("my_catalog", "my_schema", "my_table", columns)

        # Single batch call with only the non-empty descriptions.
        mock_client.update_column_descriptions.assert_called_once_with(
            "my_catalog",
            "my_schema",
            "my_table",
            {"id": "Primary key", "name": "User name"},
        )

    @patch("dbt.adapters.trino.starburst.catalog_sync.StarburstDiscoveryClient")
    def test_sync_column_descriptions_skips_when_all_empty(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        sync = StarburstCatalogSync(self._make_credentials())
        columns = {"col1": {"description": ""}, "col2": {"name": "col2"}}
        sync.sync_column_descriptions("my_catalog", "my_schema", "my_table", columns)

        mock_client.update_column_descriptions.assert_not_called()

    # -- Failure strategy --

    @patch("dbt.adapters.trino.starburst.catalog_sync.StarburstDiscoveryClient")
    def test_continue_on_error_swallows_exception(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client.update_table_description.side_effect = requests.HTTPError("500")
        mock_client_cls.return_value = mock_client

        sync = StarburstCatalogSync(self._make_credentials())
        # Should not raise.
        sync.sync_relation_description("c", "s", "t", "desc")

    @patch("dbt.adapters.trino.starburst.catalog_sync.StarburstDiscoveryClient")
    def test_fail_fast_raises(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client.update_table_description.side_effect = requests.HTTPError("500")
        mock_client_cls.return_value = mock_client

        sync = StarburstCatalogSync(
            self._make_credentials(starburst_metadata_failure_strategy="fail_fast")
        )
        with self.assertRaises(DbtRuntimeError):
            sync.sync_relation_description("c", "s", "t", "desc")

    @patch("dbt.adapters.trino.starburst.catalog_sync.StarburstDiscoveryClient")
    def test_fail_fast_raises_on_column_error(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client.update_column_descriptions.side_effect = requests.HTTPError("500")
        mock_client_cls.return_value = mock_client

        sync = StarburstCatalogSync(
            self._make_credentials(starburst_metadata_failure_strategy="fail_fast")
        )
        with self.assertRaises(DbtRuntimeError):
            sync.sync_column_descriptions("c", "s", "t", {"col": {"description": "d"}})


if __name__ == "__main__":
    unittest.main()
