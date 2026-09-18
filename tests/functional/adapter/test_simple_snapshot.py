import pytest
from dbt.tests.adapter.simple_snapshot.test_snapshot import (
    BaseSimpleSnapshot,
    BaseSnapshotCheck,
)
from dbt.tests.fixtures.project import TestProjInfo
from dbt.tests.util import run_dbt

iceberg_macro_override_sql = """
{% macro trino__current_timestamp() -%}
    current_timestamp(6)
{%- endmacro %}
"""


class TrinoSimpleSnapshot(BaseSimpleSnapshot):
    def test_updates_are_captured_by_snapshot(self, project: TestProjInfo) -> None:
        """
        Update the last 5 records. Show that all ids are current, but the last 5 reflect updates.
        """
        self.update_fact_records(
            {"updated_at": "updated_at + interval '1' day"}, "id between 16 and 20"
        )
        run_dbt(["snapshot"])
        self._assert_results(
            ids_with_current_snapshot_records=range(1, 21),
            ids_with_closed_out_snapshot_records=range(16, 21),
        )

    def test_new_column_captured_by_snapshot(self, project: TestProjInfo) -> None:
        """
        Add a column to `fact` and populate the last 10 records with a non-null value.
        Show that all ids are current, but the last 10 reflect updates and the first 10 don't
        i.e. if the column is added, but not updated, the record doesn't reflect that it's updated
        """
        self.add_fact_column("full_name", "varchar(200)")
        self.update_fact_records(
            {
                "full_name": "first_name || ' ' || last_name",
                "updated_at": "updated_at + interval '1' day",
            },
            "id between 11 and 20",
        )
        run_dbt(["snapshot"])
        self._assert_results(
            ids_with_current_snapshot_records=range(1, 21),
            ids_with_closed_out_snapshot_records=range(11, 21),
        )


    def test_staging_table_is_recreated(self, project: TestProjInfo) -> None:
        """
        It may happen that staging temporary table still exists, from previous run.
        Snapshot should work properly even in such situation.
        """

        project.run_sql(f"create table {project.test_schema}.snapshot__dbt_tmp (id int)")

        run_dbt(["snapshot"])



class TrinoSnapshotCheck(BaseSnapshotCheck):
    def test_column_selection_is_reflected_in_snapshot(self, project: TestProjInfo) -> None:
        """
        Update the first 10 records on a non-tracked column.
        Update the middle 10 records on a tracked column. (hence records 6-10 are updated on both)
        Show that all ids are current, and only the tracked column updates are reflected in `snapshot`.
        """
        self.update_fact_records(
            {"last_name": "substring(last_name, 1, 3)"}, "id between 1 and 10"
        )  # not tracked
        self.update_fact_records(
            {"email": "substring(email, 1, 3)"}, "id between 6 and 15"
        )  # tracked
        run_dbt(["snapshot"])
        self._assert_results(
            ids_with_current_snapshot_records=range(1, 21),
            ids_with_closed_out_snapshot_records=range(6, 16),
        )


@pytest.mark.iceberg
class TestIcebergSimpleSnapshot(TrinoSimpleSnapshot):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "+column_types": {"updated_at": "timestamp(6)"},
            },
        }


@pytest.mark.delta
class TestDeltaSimpleSnapshot(TrinoSimpleSnapshot):
    pass


@pytest.mark.iceberg
class TestIcebergSnapshotCheck(TrinoSnapshotCheck):
    @pytest.fixture(scope="class")
    def macros(self):
        return {"iceberg.sql": iceberg_macro_override_sql}


@pytest.mark.delta
class TestDeltaSnapshotCheck(TrinoSnapshotCheck):
    pass
