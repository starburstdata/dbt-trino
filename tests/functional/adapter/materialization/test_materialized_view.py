import pytest
from dbt.tests.util import (
    check_relation_types,
    check_relations_equal,
    run_dbt,
    run_dbt_and_capture,
    run_sql_with_adapter,
)

from tests.functional.adapter.materialization.fixtures import (
    model_cte_sql,
    model_sql,
    seed_csv,
)


# TODO: teardown_method is needed to properly remove relations and schemas after tests.
#  It could be refactored and simplified when CASCADE will be supported in Iceberg, delta, hive connectors
@pytest.mark.iceberg
class TestIcebergMaterializedViewBase:
    @pytest.fixture(scope="function", autouse=True)
    def teardown_method(self, project):
        yield
        # Drop materialized views first, then drop schema
        sql = "select * from system.metadata.materialized_views"
        results = run_sql_with_adapter(project.adapter, sql, fetch="all")
        for mv in results:
            project.run_sql(f"drop materialized view {mv[0]}.{mv[1]}.{mv[2]}")

        relation = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema
        )
        project.adapter.drop_schema(relation)


@pytest.mark.iceberg
class TestIcebergMaterializedViewExists(TestIcebergMaterializedViewBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "materialized_view",
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
            "my_table": "materialized_view",
            "my_view": "materialized_view",
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


@pytest.mark.iceberg
class TestIcebergMaterializedViewWithCTE(TestIcebergMaterializedViewBase):
    # Configuration in dbt_project.yml
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "mv_cte_test",
            "models": {
                "+materialized": "materialized_view",
            },
            "seeds": {
                "+column_types": {"some_date": "timestamp(6)"},
            },
        }

    # Everything that goes in the "seeds" directory
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": seed_csv,
        }

    # Everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "mat_view.sql": model_cte_sql,
        }

    def test_mv_with_cte_is_created(self, project):
        # Create MV
        results = run_dbt(["run"], expect_pass=True)
        assert len(results) == 1


@pytest.mark.iceberg
class TestIcebergMaterializedViewCreate(TestIcebergMaterializedViewBase):
    # Configuration in dbt_project.yml
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "mv_test",
            "models": {
                "+materialized": "materialized_view",
            },
            "seeds": {
                "+column_types": {"some_date": "timestamp(6)"},
            },
        }

    # Everything that goes in the "seeds" directory
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": seed_csv,
        }

    # Everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "mat_view.sql": model_sql,
        }

    def test_mv_is_created_and_refreshed(self, project):
        catalog = project.adapter.config.credentials.database
        schema = project.adapter.config.credentials.schema

        # Seed seed
        results = run_dbt(["seed"], expect_pass=True)
        assert len(results) == 1

        # Create MV
        results = run_dbt(["run"], expect_pass=True)
        assert len(results) == 1

        # Check if the data was loaded correctly
        check_relations_equal(project.adapter, ["seed", "mat_view"])

        # Add one row to seed
        sql = f"""INSERT INTO {catalog}.{schema}.seed
        VALUES (5, 'Mateo', timestamp '2014-09-07 17:04:27')"""
        run_sql_with_adapter(project.adapter, sql, fetch="all")

        # Refresh MV
        results = run_dbt(["run"], expect_pass=True)
        assert len(results) == 1

        # Check if one row is added in MV
        sql = f"select * from {catalog}.{schema}.mat_view"
        results = run_sql_with_adapter(project.adapter, sql, fetch="all")
        assert len(results) == 5


@pytest.mark.iceberg
class TestIcebergMaterializedViewDropAndCreate(TestIcebergMaterializedViewBase):
    # Configuration in dbt_project.yml
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "mv_test",
            "models": {
                "+materialized": "materialized_view",
                "+full_refresh": True,
            },
            "seeds": {
                "+column_types": {"some_date": "timestamp(6)"},
            },
        }

    # Everything that goes in the "seeds" directory
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": seed_csv,
        }

    # Everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "mat_view_overrides_table.sql": model_sql,
            "mat_view_overrides_view.sql": model_sql,
            "mat_view_overrides_materialized_view.sql": model_sql,
        }

    def test_mv_overrides_relation(self, project):
        # Create relation with same name
        project.adapter.execute("CREATE VIEW mat_view_overrides_view AS SELECT 3 c")
        project.adapter.execute("CREATE TABLE mat_view_overrides_table AS SELECT 4 d")
        project.adapter.execute(
            "CREATE MATERIALIZED VIEW mat_view_overrides_materialized_view AS SELECT 5 e"
        )

        # Seed seed
        results = run_dbt(["seed"], expect_pass=True)
        assert len(results) == 1

        # Create MVs, already existing relations with same name should be dropped
        results = run_dbt(["run"], expect_pass=True)
        assert len(results) == 3

        # Check if MVs were created correctly
        expected = {
            "mat_view_overrides_view": "materialized_view",
            "mat_view_overrides_table": "materialized_view",
            "mat_view_overrides_materialized_view": "materialized_view",
        }
        check_relation_types(project.adapter, expected)

        check_relations_equal(
            project.adapter,
            [
                "seed",
                "mat_view_overrides_view",
                "mat_view_overrides_table",
                "mat_view_overrides_materialized_view",
            ],
        )


@pytest.mark.iceberg
@pytest.mark.skip_profile("starburst_galaxy")
class TestIcebergMaterializedViewProperties(TestIcebergMaterializedViewBase):
    # Configuration in dbt_project.yml
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "mv_test",
            "models": {
                "+materialized": "materialized_view",
                "+properties": {"format": "'PARQUET'"},
            },
            "seeds": {
                "+column_types": {"some_date": "timestamp(6)"},
            },
        }

    # Everything that goes in the "seeds" directory
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": seed_csv,
        }

    # Everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "mat_view.sql": model_sql,
        }

    def test_set_mv_properties(self, project):
        catalog = project.adapter.config.credentials.database
        schema = project.adapter.config.credentials.schema

        # Seed seed
        results = run_dbt(["seed"], expect_pass=True)
        assert len(results) == 1

        # Create MV
        results = run_dbt(["run"], expect_pass=True)
        assert len(results) == 1

        # Retrieve MV properties
        sql = f"SHOW CREATE MATERIALIZED VIEW {catalog}.{schema}.mat_view"
        results = run_sql_with_adapter(project.adapter, sql, fetch="all")
        assert "format = 'PARQUET'" in results[0][0]


@pytest.mark.iceberg
class TestIcebergMaterializedViewWithGracePeriod(TestIcebergMaterializedViewBase):
    # Configuration in dbt_project.yml
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "mv_test",
            "models": {
                "+materialized": "materialized_view",
                "+grace_period": "INTERVAL '3' SECOND",
            },
        }

    # Everything that goes in the "seeds" directory
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": seed_csv,
        }

    # Everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "mat_view.sql": model_sql,
        }

    def test_set_mv_properties(self, project):
        # Seed seed
        results = run_dbt(["seed"], expect_pass=True)
        assert len(results) == 1

        # Create MV
        results, log_output = run_dbt_and_capture(["run", "--debug"], expect_pass=True)
        assert len(results) == 1
        assert "grace period INTERVAL '3' SECOND" in log_output

        # Check if MVs were created correctly
        check_relation_types(project.adapter, {"mat_view": "materialized_view"})
