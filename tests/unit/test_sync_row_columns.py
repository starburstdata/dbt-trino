import unittest
from multiprocessing import get_context
from unittest.mock import patch

from dbt.adapters.base import Column

from dbt.adapters.trino import TrinoAdapter, TrinoRelation

from .utils import config_from_parts_or_dicts


class TestSyncRowColumns(unittest.TestCase):
    def setUp(self):
        profile_cfg = {
            "outputs": {
                "test": {
                    "type": "trino",
                    "catalog": "iceberg",
                    "host": "localhost",
                    "port": 8080,
                    "schema": "dbt_test_schema",
                    "method": "none",
                    "user": "trino_user",
                }
            },
            "target": "test",
        }
        project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "config-version": 2,
        }
        self.config = config_from_parts_or_dicts(project_cfg, profile_cfg)
        self.adapter = TrinoAdapter(self.config, get_context("spawn"))

    def _target_relation(self):
        return TrinoRelation.create(
            database="iceberg",
            schema="dbt_test_schema",
            identifier="test_model",
        )

    def test_row_to_scalar_change_skips_nested_diffing(self):
        schema_changes_dict = {
            "source_columns": [
                Column("payload", "row(nested_field varchar)"),
            ],
            "target_columns": [
                Column("payload", "varchar"),
            ],
            "new_target_types": [
                {
                    "column_name": "payload",
                    "new_type": "row(nested_field varchar)",
                }
            ],
            "source_not_in_target": [],
            "target_not_in_source": [],
        }

        with patch.object(self.adapter, "execute") as mock_execute:
            result = self.adapter.sync_row_columns(
                "sync_all_columns",
                self._target_relation(),
                schema_changes_dict,
            )

        mock_execute.assert_not_called()
        self.assertIs(result, schema_changes_dict)

    def test_scalar_to_row_change_skips_nested_diffing(self):
        schema_changes_dict = {
            "source_columns": [
                Column("payload", "varchar"),
            ],
            "target_columns": [
                Column("payload", "row(nested_field varchar)"),
            ],
            "new_target_types": [
                {
                    "column_name": "payload",
                    "new_type": "varchar",
                }
            ],
            "source_not_in_target": [],
            "target_not_in_source": [],
        }

        with patch.object(self.adapter, "execute") as mock_execute:
            result = self.adapter.sync_row_columns(
                "sync_all_columns",
                self._target_relation(),
                schema_changes_dict,
            )

        mock_execute.assert_not_called()
        self.assertIs(result, schema_changes_dict)

    def test_format_nested_column_path_quotes_mixed_case_segment(self):
        formatted = self.adapter._format_nested_column_path("payload.Order")
        self.assertIn('"order"', formatted.lower())
        self.assertIn("payload", formatted.lower())


if __name__ == "__main__":
    unittest.main()
