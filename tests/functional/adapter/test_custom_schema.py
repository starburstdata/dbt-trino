from abc import ABC, abstractmethod

import pytest
from dbt.tests.util import run_dbt, run_sql_with_adapter

seed_csv = """
id,name,date
1,Easton,1981-05-20 06:46:51
2,Lillian,1978-09-03 18:10:33
3,Jeremiah,1982-03-11 03:59:51
4,Nolan,1976-05-06 20:21:35
""".lstrip()


class CustomSchemaBase(ABC):
    """
    This test is meant to ensure that Trino table, view, incremental materialization
    works as expected for custom schemas
    """

    # set custom schema name
    custom_schema_name = "very_custom_schema_name"

    @property
    @abstractmethod
    def table_type(self):
        pass

    @property
    @abstractmethod
    def materialization(self):
        pass

    # define model
    def custom_schema_model(self, materialization):
        return f"""
                    {{{{
                        config(
                        materialized="{materialization}",
                        schema="{self.custom_schema_name}"
                        )
                    }}}}
                    select * from {{{{ ref('seed') }}}}
                """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "+column_types": {"date": "timestamp(6)"},
            },
        }

    # everything that goes in the "seeds" directory
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": seed_csv,
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            f"custom_schema_{self.materialization()}_model.sql": self.custom_schema_model(
                self.materialization()
            )
        }

    def test_custom_schema_trino(self, project):
        # Seed seeds, run models.
        results = run_dbt(["seed"], expect_pass=True)
        assert len(results) == 1
        results = run_dbt(["run"], expect_pass=True)
        assert len(results) == 1

        # Fetch info from information_schema about just created table/view.
        sql = f"""
            select * from {project.adapter.config.credentials.database}.information_schema.tables
            where table_catalog = '{project.adapter.config.credentials.database}'
            and table_schema = '{project.adapter.config.credentials.schema}_{self.custom_schema_name}'
        """.strip()
        results = run_sql_with_adapter(project.adapter, sql, fetch="all")

        # Check if fetched info is as expected to be.
        assert len(results) == 1
        assert results[0][0] == project.adapter.config.credentials.database
        assert (
            results[0][1]
            == f"{project.adapter.config.credentials.schema}_{self.custom_schema_name}"
        )
        assert results[0][2] == f"custom_schema_{self.materialization()}_model"
        assert results[0][3] == self.table_type()


class TestCustomSchemaTable(CustomSchemaBase):
    def materialization(self):
        return "table"

    def table_type(self):
        return "BASE TABLE"


class TestCustomSchemaView(CustomSchemaBase):
    def materialization(self):
        return "view"

    def table_type(self):
        return "VIEW"


class TestCustomSchemaIncremental(CustomSchemaBase):
    def materialization(self):
        return "incremental"

    def table_type(self):
        return "BASE TABLE"
