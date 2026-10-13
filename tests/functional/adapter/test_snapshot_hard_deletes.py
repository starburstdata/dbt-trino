import pytest
from dbt.tests.adapter.simple_snapshot.test_ephemeral_snapshot_hard_deletes import (
    BaseSnapshotEphemeralHardDeletes,
    BaseSnapshotNewColumnSpecificCheckCols,
    BaseSnapshotNewColumnTimestampStrategy,
    BaseSnapshotNewColumnWithDeletes,
)

iceberg_macro_override_sql = """
{% macro trino__current_timestamp() -%}
    current_timestamp(6)
{%- endmacro %}
"""

# The upstream fixtures use untyped timestamp literals and ADD COLUMN ... DEFAULT,
# neither of which Trino's Iceberg connector accepts, so they are rewritten here.
# The new column is left NULL; the tests only need the schema change.
source_create_sql = """
create table {database}.{schema}.src_customers (
    id INTEGER,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(50),
    updated_at TIMESTAMP(6)
)
"""

source_insert_sql = """
insert into {database}.{schema}.src_customers (id, first_name, last_name, email, updated_at) values
(1, 'John', 'Doe', 'john.doe@example.com', TIMESTAMP '2023-01-01 10:00:00'),
(2, 'Jane', 'Smith', 'jane.smith@example.com', TIMESTAMP '2023-01-02 11:00:00'),
(3, 'Bob', 'Johnson', 'bob.johnson@example.com', TIMESTAMP '2023-01-03 12:00:00')
"""

source_alter_sql = """
alter table {database}.{schema}.src_customers add column dummy_column VARCHAR(50)
"""

source_delete_sql = """
delete from {database}.{schema}.src_customers where id = 3
"""


class TrinoSnapshotHardDeletesFixtures:
    @pytest.fixture(scope="class")
    def macros(self):
        return {"iceberg.sql": iceberg_macro_override_sql}

    @pytest.fixture(scope="class")
    def source_create_sql(self):
        return source_create_sql

    @pytest.fixture(scope="class")
    def source_insert_sql(self):
        return source_insert_sql

    @pytest.fixture(scope="class")
    def source_alter_sql(self):
        return source_alter_sql

    @pytest.fixture(scope="class")
    def source_delete_sql(self):
        return source_delete_sql


@pytest.mark.iceberg
class TestTrinoSnapshotEphemeralHardDeletes(
    TrinoSnapshotHardDeletesFixtures, BaseSnapshotEphemeralHardDeletes
):
    pass


@pytest.mark.iceberg
class TestTrinoSnapshotNewColumnTimestampStrategy(
    TrinoSnapshotHardDeletesFixtures, BaseSnapshotNewColumnTimestampStrategy
):
    pass


@pytest.mark.iceberg
class TestTrinoSnapshotNewColumnSpecificCheckCols(
    TrinoSnapshotHardDeletesFixtures, BaseSnapshotNewColumnSpecificCheckCols
):
    pass


@pytest.mark.iceberg
class TestTrinoSnapshotNewColumnWithDeletes(
    TrinoSnapshotHardDeletesFixtures, BaseSnapshotNewColumnWithDeletes
):
    pass
