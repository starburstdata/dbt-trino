import pytest
from dbt.tests.util import check_relations_equal, run_dbt

from tests.functional.adapter.materialization.fixtures import (
    incremental_append_new_columns,
    incremental_append_new_columns_remove_one_sql,
    incremental_append_new_columns_remove_one_target_sql,
    incremental_append_new_columns_target_sql,
    incremental_fail_sql,
    incremental_ignore_sql,
    incremental_ignore_target_sql,
    incremental_sync_all_columns_diff_data_types_sql,
    incremental_sync_all_columns_diff_data_types_target_sql,
    incremental_sync_all_columns_sql,
    incremental_sync_all_columns_target_sql,
    model_a_sql,
    schema_base_yml,
    select_from_a_sql,
    select_from_incremental_append_new_columns_remove_one_sql,
    select_from_incremental_append_new_columns_remove_one_target_sql,
    select_from_incremental_append_new_columns_sql,
    select_from_incremental_append_new_columns_target_sql,
    select_from_incremental_ignore_sql,
    select_from_incremental_ignore_target_sql,
    select_from_incremental_sync_all_columns_diff_data_types_sql,
    select_from_incremental_sync_all_columns_diff_data_types_target_sql,
    select_from_incremental_sync_all_columns_sql,
    select_from_incremental_sync_all_columns_target_sql,
)


class OnSchemaChangeBase:
    # configuration in dbt_project.yml
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "on_schema_change"}

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": model_a_sql,
            "incremental_ignore.sql": incremental_ignore_sql,
            "incremental_ignore_target.sql": incremental_ignore_target_sql,
            "incremental_append_new_columns.sql": incremental_append_new_columns,
            "incremental_append_new_columns_target.sql": incremental_append_new_columns_target_sql,
            "incremental_append_new_columns_remove_one.sql": incremental_append_new_columns_remove_one_sql,
            "incremental_append_new_columns_remove_one_target.sql": incremental_append_new_columns_remove_one_target_sql,
            "incremental_fail.sql": incremental_fail_sql,
            "incremental_sync_all_columns.sql": incremental_sync_all_columns_sql,
            "incremental_sync_all_columns_target.sql": incremental_sync_all_columns_target_sql,
            "incremental_sync_all_columns_diff_data_types.sql": incremental_sync_all_columns_diff_data_types_sql,
            "incremental_sync_all_columns_diff_data_types_target.sql": incremental_sync_all_columns_diff_data_types_target_sql,
            "schema.yml": schema_base_yml,
        }

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "select_from_a.sql": select_from_a_sql,
            "select_from_incremental_append_new_columns.sql": select_from_incremental_append_new_columns_sql,
            "select_from_incremental_append_new_columns_remove_one.sql": select_from_incremental_append_new_columns_remove_one_sql,
            "select_from_incremental_append_new_columns_remove_one_target.sql": select_from_incremental_append_new_columns_remove_one_target_sql,
            "select_from_incremental_append_new_columns_target.sql": select_from_incremental_append_new_columns_target_sql,
            "select_from_incremental_ignore.sql": select_from_incremental_ignore_sql,
            "select_from_incremental_ignore_target.sql": select_from_incremental_ignore_target_sql,
            "select_from_incremental_sync_all_columns.sql": select_from_incremental_sync_all_columns_sql,
            "select_from_incremental_sync_all_columns_target.sql": select_from_incremental_sync_all_columns_target_sql,
            "select_from_incremental_sync_all_columns_diff_data_types.sql": select_from_incremental_sync_all_columns_diff_data_types_sql,
            "select_from_incremental_sync_all_columns_diff_data_types_target.sql": select_from_incremental_sync_all_columns_diff_data_types_target_sql,
        }

    def list_tests_and_assert(self, include, exclude, expected_tests):
        list_args = ["ls", "--resource-type", "test"]
        if include:
            list_args.extend(("--select", include))
        if exclude:
            list_args.extend(("--exclude", exclude))
        listed = run_dbt(list_args)
        print(listed)
        assert len(listed) == len(expected_tests)
        test_names = [name.split(".")[-1] for name in listed]
        assert sorted(test_names) == sorted(expected_tests)

    def run_tests_and_assert(
        self, project, include, exclude, expected_tests, compare_source, compare_target
    ):
        run_args = ["run"]
        if include:
            run_args.extend(("--models", include))
        results_one = run_dbt(run_args)
        results_two = run_dbt(run_args)

        assert len(results_one) == 3
        assert len(results_two) == 3

        test_args = ["test"]
        if include:
            test_args.extend(("--models", include))
        if exclude:
            test_args.extend(("--exclude", exclude))

        results = run_dbt(test_args)
        tests_run = [r.node.name for r in results]
        assert len(tests_run) == len(expected_tests)
        assert sorted(tests_run) == sorted(expected_tests)
        check_relations_equal(project.adapter, [compare_source, compare_target])

    def run_incremental_ignore(self, project):
        select = "model_a incremental_ignore incremental_ignore_target"
        compare_source = "incremental_ignore"
        compare_target = "incremental_ignore_target"
        exclude = None
        expected = [
            "select_from_a",
            "select_from_incremental_ignore",
            "select_from_incremental_ignore_target",
            "unique_model_a_id",
            "unique_incremental_ignore_id",
            "unique_incremental_ignore_target_id",
        ]

        self.list_tests_and_assert(select, exclude, expected)
        self.run_tests_and_assert(
            project, select, exclude, expected, compare_source, compare_target
        )

    def run_incremental_append_new_columns(self, project):
        select = "model_a incremental_append_new_columns incremental_append_new_columns_target"
        compare_source = "incremental_append_new_columns"
        compare_target = "incremental_append_new_columns_target"
        exclude = None
        expected = [
            "select_from_a",
            "select_from_incremental_append_new_columns",
            "select_from_incremental_append_new_columns_target",
            "unique_model_a_id",
            "unique_incremental_append_new_columns_id",
            "unique_incremental_append_new_columns_target_id",
        ]
        self.list_tests_and_assert(select, exclude, expected)
        self.run_tests_and_assert(
            project, select, exclude, expected, compare_source, compare_target
        )

    def run_incremental_append_new_columns_remove_one(self, project):
        select = "model_a incremental_append_new_columns_remove_one incremental_append_new_columns_remove_one_target"
        compare_source = "incremental_append_new_columns_remove_one"
        compare_target = "incremental_append_new_columns_remove_one_target"
        exclude = None
        expected = [
            "select_from_a",
            "select_from_incremental_append_new_columns_remove_one",
            "select_from_incremental_append_new_columns_remove_one_target",
            "unique_model_a_id",
            "unique_incremental_append_new_columns_remove_one_id",
            "unique_incremental_append_new_columns_remove_one_target_id",
        ]
        self.run_tests_and_assert(
            project, select, exclude, expected, compare_source, compare_target
        )

    def run_incremental_sync_all_columns(self, project):
        select = "model_a incremental_sync_all_columns incremental_sync_all_columns_target"
        compare_source = "incremental_sync_all_columns"
        compare_target = "incremental_sync_all_columns_target"
        exclude = None
        expected = [
            "select_from_a",
            "select_from_incremental_sync_all_columns",
            "select_from_incremental_sync_all_columns_target",
            "unique_model_a_id",
            "unique_incremental_sync_all_columns_id",
            "unique_incremental_sync_all_columns_target_id",
        ]
        self.list_tests_and_assert(select, exclude, expected)
        self.run_tests_and_assert(
            project, select, exclude, expected, compare_source, compare_target
        )

    def run_incremental_sync_all_columns_data_type_change(self, project):
        select = "model_a incremental_sync_all_columns_diff_data_types incremental_sync_all_columns_diff_data_types_target"
        compare_source = "incremental_sync_all_columns_diff_data_types"
        compare_target = "incremental_sync_all_columns_diff_data_types_target"
        exclude = None
        expected = [
            "select_from_a",
            "select_from_incremental_sync_all_columns_diff_data_types",
            "select_from_incremental_sync_all_columns_diff_data_types_target",
            "unique_model_a_id",
            "unique_incremental_sync_all_columns_diff_data_types_id",
            "unique_incremental_sync_all_columns_diff_data_types_target_id",
        ]
        self.list_tests_and_assert(select, exclude, expected)
        self.run_tests_and_assert(
            project, select, exclude, expected, compare_source, compare_target
        )

    def run_incremental_fail_on_schema_change(self, _):
        select = "model_a incremental_fail"
        run_dbt(["run", "--models", select, "--full-refresh"])
        results_two = run_dbt(["run", "--models", select], expect_pass=False)
        assert "Compilation Error" in results_two[1].message

    def test_run_incremental_ignore(self, project):
        self.run_incremental_ignore(project)

    def test_run_incremental_append_new_columns(self, project):
        self.run_incremental_append_new_columns(project)
        self.run_incremental_append_new_columns_remove_one(project)

    def test_run_incremental_sync_all_columns(self, project):
        self.run_incremental_sync_all_columns(project)

    def test_run_incremental_sync_all_columns_data_type_change(self, project):
        self.run_incremental_sync_all_columns_data_type_change(project)

    def test_run_incremental_fail_on_schema_change(self, project):
        self.run_incremental_fail_on_schema_change(project)


@pytest.mark.iceberg
class TestIcebergOnSchemaChange(OnSchemaChangeBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "on_schema_change_iceberg",
            "models": {"+incremental_strategy": "merge"},
        }


@pytest.mark.delta
class TestDeltaOnSchemaChange(OnSchemaChangeBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "on_schema_change_delta",
            "models": {
                "+on_table_exists": "drop",
                "+incremental_strategy": "merge",
            },
        }

    @pytest.mark.xfail(reason="This connector does not support dropping columns")
    def test_run_incremental_sync_all_columns(self, project):
        super(TestDeltaOnSchemaChange, self).test_run_incremental_sync_all_columns(project)

    @pytest.mark.xfail(reason="This connector does not support dropping columns")
    def test_run_incremental_sync_all_columns_data_type_change(self, project):
        super(
            TestDeltaOnSchemaChange, self
        ).test_run_incremental_sync_all_columns_data_type_change(project)
