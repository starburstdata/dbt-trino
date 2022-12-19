from abc import ABC, abstractmethod

import pytest
from dbt.tests.util import run_dbt, run_sql_with_adapter


class CustomSchemaBase(ABC):
    """
    This test is meant to ensure that get_incremental_tmp_relation_type macro
    is returning expected values on certain inputs.
    """

    @property
    @abstractmethod
    def expected_types(self):
        # Expected table/view type returned from created model.
        # Order based on columns' order in model definition.
        return ["table", "view", "view", "view", "table", "view", "table"]

    # define model
    def incremental_model(self):
        return """
                    select
                    '{{ get_incremental_tmp_relation_type('delete+insert', 'foo', 'sql') }}' AS delete_plus_insert_strategy,
                    '{{ get_incremental_tmp_relation_type('append', 'foo', 'sql') }}' AS append_strategy,
                    '{{ get_incremental_tmp_relation_type('default', 'foo', 'sql') }}' AS default_strategy,
                    '{{ get_incremental_tmp_relation_type('merge', 'foo', 'sql') }}' AS merge_strategy,
                    '{{ get_incremental_tmp_relation_type('foo', 'some_unique_key', 'sql') }}' AS unique_key,
                    '{{ get_incremental_tmp_relation_type('foo', None, 'sql') }}' AS no_unique_key,
                    '{{ get_incremental_tmp_relation_type('default', 'foo', 'python') }}' AS python_model
                """

    @pytest.fixture(scope="class")
    @abstractmethod
    def project_config_update(self):
        pass

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {"test_get_incremental_tmp_relation_type.sql": self.incremental_model()}

    def test_get_incremental_tmp_relation_type(self, project):
        # Run models.
        results = run_dbt(["run"], expect_pass=True)
        assert len(results) == 1

        # Fetch info from get_incremental_tmp_relation_type macro output.
        sql = f"""
            select * from {project.adapter.config.credentials.database}.{project.adapter.config.credentials.schema}.test_get_incremental_tmp_relation_type
        """.strip()
        results = run_sql_with_adapter(project.adapter, sql, fetch="all")

        # Check if fetched info is as expected to be.
        assert len(results) == 1
        assert results[0] == self.expected_types


class TestViewsEnabled(CustomSchemaBase):
    @property
    def expected_types(self):
        return super().expected_types

    @pytest.fixture(scope="class")
    def project_config_update(self):
        # Not specifying views_enabled config,
        # as it is 'True' by default
        pass


class TestViewsNotEnabled(CustomSchemaBase):
    @property
    def expected_types(self):
        # Expected type is 'table' for every config,
        # as views_enabled is set to 'False'.
        return ["table" for _ in super().expected_types]

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {"+views_enabled": False},
        }
