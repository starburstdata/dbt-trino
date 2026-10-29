import re

import pytest
from dbt.tests.adapter.incremental.test_incremental_microbatch import BaseMicrobatch
from dbt.tests.util import (
    patch_microbatch_end_time,
    relation_from_name,
    run_dbt_and_capture,
)


@pytest.mark.iceberg
class TestTrinoMicrobatchIceberg(BaseMicrobatch):
    pass


BATCH_DAYS = 8
CONCURRENT_MARKER = "is being run concurrently"

CONCURRENT_INPUT_MODEL_SQL = "{{ config(materialized='table', event_time='event_time') }}\n" + (
    "\nunion all\n".join(
        f"select {day} as id, TIMESTAMP '2020-01-{day:02d} 00:00:00-0' as event_time"
        for day in range(1, BATCH_DAYS + 1)
    )
)

CONCURRENT_MICROBATCH_MODEL_SQL = """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    unique_key='id',
    event_time='event_time',
    batch_size='day',
    begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0),
    concurrent_batches=true,
) }}
select * from {{ ref('input_model') }}
"""

LOCATION_MICROBATCH_MODEL_SQL = CONCURRENT_MICROBATCH_MODEL_SQL.replace(
    "concurrent_batches=true,",
    "concurrent_batches=true,\n"
    "    properties={'location': \"'s3a://datalake/\" ~ target.schema ~ \"/microbatch_model'\"},",
)


class BaseTrinoMicrobatchConcurrency:
    model_sql = CONCURRENT_MICROBATCH_MODEL_SQL

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": CONCURRENT_INPUT_MODEL_SQL,
            "microbatch_model.sql": self.model_sql,
        }

    def assert_one_row_per_id(self, project):
        relation = relation_from_name(project.adapter, "microbatch_model")
        rows = project.run_sql(f"select id from {relation} order by id", fetch="all")
        assert [row[0] for row in rows] == list(range(1, BATCH_DAYS + 1))

    def assert_no_temp_tables_left(self, project):
        rows = project.run_sql(
            f"select table_name from {project.database}.information_schema.tables "
            f"where table_schema = '{project.test_schema}' "
            "and starts_with(table_name, 'microbatch_model__dbt_tmp')",
            fetch="all",
        )
        assert rows == []

    def build(self, expect_pass):
        with patch_microbatch_end_time(f"2020-01-{BATCH_DAYS:02d} 13:57:00"):
            return run_dbt_and_capture(
                ["--debug", "run", "--threads", "4"], expect_pass=expect_pass
            )[1]

    def reprocess(self, expect_pass):
        return run_dbt_and_capture(
            [
                "--debug",
                "run",
                "--threads",
                "4",
                "--select",
                "microbatch_model",
                "--event-time-start",
                "2020-01-01",
                "--event-time-end",
                f"2020-01-{BATCH_DAYS + 1:02d}",
            ],
            expect_pass=expect_pass,
        )[1]


@pytest.mark.iceberg
class TestTrinoMicrobatchSequentialByDefaultIceberg(BaseTrinoMicrobatchConcurrency):
    """`concurrent_batches` has no effect until `use_concurrent_microbatch` is enabled."""

    def test_batches_run_sequentially(self, project):
        assert CONCURRENT_MARKER not in self.build(expect_pass=True)
        self.assert_one_row_per_id(project)
        self.assert_no_temp_tables_left(project)

        assert CONCURRENT_MARKER not in self.reprocess(expect_pass=True)
        self.assert_one_row_per_id(project)
        self.assert_no_temp_tables_left(project)


@pytest.mark.iceberg
class TestTrinoMicrobatchConcurrentIceberg(BaseTrinoMicrobatchConcurrency):
    """Only scheduling is checked: parallel commits to one Iceberg table can intermittently fail."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_concurrent_microbatch": True}}

    def test_batches_are_scheduled_concurrently(self, project):
        assert CONCURRENT_MARKER in self.build(expect_pass=None)
        assert CONCURRENT_MARKER in self.reprocess(expect_pass=None)


# Galaxy cannot write to the `datalake` bucket used by the local setup.
@pytest.mark.iceberg
@pytest.mark.skip_profile("starburst_galaxy")
class TestTrinoMicrobatchTempLocationIceberg(BaseTrinoMicrobatchConcurrency):
    """Each batch's temporary table needs its own location, or concurrent batches collide."""

    model_sql = LOCATION_MICROBATCH_MODEL_SQL

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_concurrent_microbatch": True}}

    def test_batches_use_distinct_temp_locations(self, project):
        logs = self.build(expect_pass=None)

        locations = re.findall(r"location = '(s3a://[^']*__dbt_tmp[^']*)'", logs)
        # The first batch creates the target table directly, without a temporary table.
        assert len(set(locations)) == BATCH_DAYS - 1, locations
