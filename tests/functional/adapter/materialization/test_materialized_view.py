import pytest
from dbt.tests.util import check_relation_types, run_dbt


@pytest.mark.iceberg
class TestIcebergMaterializedViewExists:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "materializedview",
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_view.sql": "select 1 a",
            "my_table.sql": """ {{
    config(materialized='table')
}}
select 1 a""",
        }

    def test_mv_is_dropped_when_model_runs_view(self, project):
        project.adapter.execute("CREATE OR REPLACE MATERIALIZED VIEW my_view AS SELECT 2 b")
        project.adapter.execute("CREATE OR REPLACE MATERIALIZED VIEW my_table AS SELECT 2 b")

        # check relation types
        expected = {
            "my_table": "materializedview",
            "my_view": "materializedview",
        }
        check_relation_types(project.adapter, expected)

        model_count = len(run_dbt(["run"]))
        assert model_count == 2

        # check relation types
        expected = {
            "my_view": "view",
            "my_table": "table",
        }
        check_relation_types(project.adapter, expected)
