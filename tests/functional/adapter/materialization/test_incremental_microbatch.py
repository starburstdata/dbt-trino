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

CONCURRENT_INPUT_MODEL_SQL = "{{ config(materialized='table', event_time='event_time') }}\n" + (
    "\nunion all\n".join(
        f"select {day} as id, TIMESTAMP '2020-01-0{day} 00:00:00-0' as event_time"
        for day in range(1, BATCH_DAYS + 1)
    )
)

# Concurrent batches only avoid Iceberg commit conflicts when each batch's
# delete stays within its own partitions.
CONCURRENT_MICROBATCH_MODEL_SQL = """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    unique_key='id',
    event_time='event_time',
    batch_size='day',
    begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0),
    concurrent_batches=true,
    properties={'partitioning': "ARRAY['day(event_time)']"},
) }}
select * from {{ ref('input_model') }}
"""


@pytest.mark.iceberg
class TestTrinoMicrobatchConcurrentIceberg:
    expected_concurrent_batches = BATCH_DAYS - 2

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "input_model.sql": CONCURRENT_INPUT_MODEL_SQL,
            "microbatch_model.sql": CONCURRENT_MICROBATCH_MODEL_SQL,
        }

    def assert_one_row_per_id(self, project):
        relation = relation_from_name(project.adapter, "microbatch_model")
        rows = project.run_sql(f"select id from {relation} order by id", fetch="all")
        assert [row[0] for row in rows] == list(range(1, BATCH_DAYS + 1))

    def test_batches_run_concurrently(self, project):
        # The first run creates the table, then inserts into it.
        with patch_microbatch_end_time(f"2020-01-0{BATCH_DAYS} 13:57:00"):
            _, logs = run_dbt_and_capture(["--debug", "run", "--threads", "4"])
        assert logs.count("is being run concurrently") == self.expected_concurrent_batches
        self.assert_one_row_per_id(project)

        # Reprocessing every batch deletes from and inserts into the table.
        _, logs = run_dbt_and_capture(
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
                f"2020-01-0{BATCH_DAYS + 1}",
            ]
        )
        assert logs.count("is being run concurrently") == self.expected_concurrent_batches
        self.assert_one_row_per_id(project)


@pytest.mark.iceberg
class TestTrinoMicrobatchConcurrencyDisabledIceberg(TestTrinoMicrobatchConcurrentIceberg):
    expected_concurrent_batches = 0

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_concurrent_microbatch": False}}


@pytest.mark.iceberg
class TestTrinoMicrobatchConcurrentLocationIceberg(TestTrinoMicrobatchConcurrentIceberg):
    """Each concurrent batch's temporary table needs its own location."""

    @pytest.fixture(scope="class")
    def models(self):
        microbatch_model_sql = CONCURRENT_MICROBATCH_MODEL_SQL.replace(
            "properties={",
            "properties={'location': \"'s3a://datalake/\" ~ target.schema ~ \"/microbatch_model'\", ",
        )
        return {
            "input_model.sql": CONCURRENT_INPUT_MODEL_SQL,
            "microbatch_model.sql": microbatch_model_sql,
        }
