import pytest
from dbt.tests.util import check_relations_equal, run_dbt


_MODELS__ROW_BASE = """
{{
    config(materialized='table')
}}

with source_data as (
    select 1 as id, cast(row('foo') as row(nested_field varchar)) as payload union all
    select 2 as id, cast(row('bar') as row(nested_field varchar)) as payload
)

select * from source_data
"""

_MODELS__INCREMENTAL_ROW_APPEND = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='append_new_columns',
        incremental_strategy='merge'
    )
}}

with source_data as (
    select 1 as id, cast(row('foo', cast(null as varchar)) as row(nested_field varchar, extra_field varchar)) as payload union all
    select 2 as id, cast(row('bar', 'baz') as row(nested_field varchar, extra_field varchar)) as payload union all
    select 3 as id, cast(row('qux', 'quux') as row(nested_field varchar, extra_field varchar)) as payload
)

{% if is_incremental() %}
    select
        id,
        cast(
            row(payload.nested_field, payload.extra_field)
            as row(nested_field varchar, extra_field varchar)
        ) as payload
    from source_data
{% else %}
    select
        id,
        cast(row(payload.nested_field) as row(nested_field varchar)) as payload
    from source_data
    where id <= 2
{% endif %}
"""

_MODELS__INCREMENTAL_ROW_APPEND_EXPECTED = """
{{
    config(materialized='table')
}}

select
    id,
    cast(
        row(payload.nested_field, payload.extra_field)
        as row(nested_field varchar, extra_field varchar)
    ) as payload
from {{ ref('incremental_row_append') }}
order by id
"""

_MODELS__INCREMENTAL_ROW_SYNC = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='sync_all_columns',
        incremental_strategy='merge'
    )
}}

with source_data as (
    select 1 as id, cast(row('foo', 'baz') as row(nested_field varchar, extra_field varchar)) as payload union all
    select 2 as id, cast(row('bar', 'qux') as row(nested_field varchar, extra_field varchar)) as payload
)

{% if is_incremental() %}
    select
        id,
        cast(row(payload.nested_field) as row(nested_field varchar)) as payload
    from source_data
{% else %}
    select * from source_data
{% endif %}
"""

_MODELS__INCREMENTAL_ROW_SYNC_EXPECTED = """
{{
    config(materialized='table')
}}

select
    id,
    cast(row(payload.nested_field) as row(nested_field varchar)) as payload
from {{ ref('incremental_row_sync') }}
order by id
"""

_MODELS__DEEPLY_NESTED_ROW_BASE = """
{{
    config(materialized='table')
}}

with source_data as (
    select 1 as id,
        cast(
            row(
                'level1',
                row(
                    'level2',
                    row('level3')
                )
            ) as row(
                l1_field varchar,
                level2 row(
                    l2_field varchar,
                    level3 row(l3_field varchar)
                )
            )
        ) as payload
    union all
    select 2 as id,
        cast(
            row(
                'level1_b',
                row(
                    'level2_b',
                    row('level3_b')
                )
            ) as row(
                l1_field varchar,
                level2 row(
                    l2_field varchar,
                    level3 row(l3_field varchar)
                )
            )
        ) as payload
)

select * from source_data
"""

_MODELS__INCREMENTAL_DEEPLY_NESTED_ROW_APPEND = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='append_new_columns',
        incremental_strategy='merge'
    )
}}

with source_data as (
    select 1 as id,
        cast(
            row(
                'level1',
                'new_l1',
                row(
                    'level2',
                    'new_l2',
                    row(
                        'level3',
                        'new_l3'
                    )
                )
            ) as row(
                l1_field varchar,
                l1_new_field varchar,
                level2 row(
                    l2_field varchar,
                    l2_new_field varchar,
                    level3 row(
                        l3_field varchar,
                        l3_new_field varchar
                    )
                )
            )
        ) as payload
    union all
    select 2 as id,
        cast(
            row(
                'level1_b',
                'new_l1_b',
                row(
                    'level2_b',
                    'new_l2_b',
                    row(
                        'level3_b',
                        'new_l3_b'
                    )
                )
            ) as row(
                l1_field varchar,
                l1_new_field varchar,
                level2 row(
                    l2_field varchar,
                    l2_new_field varchar,
                    level3 row(
                        l3_field varchar,
                        l3_new_field varchar
                    )
                )
            )
        ) as payload
    union all
    select 3 as id,
        cast(
            row(
                'level1_c',
                'new_l1_c',
                row(
                    'level2_c',
                    'new_l2_c',
                    row(
                        'level3_c',
                        'new_l3_c'
                    )
                )
            ) as row(
                l1_field varchar,
                l1_new_field varchar,
                level2 row(
                    l2_field varchar,
                    l2_new_field varchar,
                    level3 row(
                        l3_field varchar,
                        l3_new_field varchar
                    )
                )
            )
        ) as payload
)

{% if is_incremental() %}
    select
        id,
        cast(
            row(
                payload.l1_field,
                row(
                    payload.level2.l2_field,
                    row(
                        payload.level2.level3.l3_field,
                        payload.level2.level3.l3_new_field
                    ),
                    payload.level2.l2_new_field
                ),
                payload.l1_new_field
            ) as row(
                l1_field varchar,
                level2 row(
                    l2_field varchar,
                    level3 row(
                        l3_field varchar,
                        l3_new_field varchar
                    ),
                    l2_new_field varchar
                ),
                l1_new_field varchar
            )
        ) as payload
    from source_data
{% else %}
    select
        id,
        cast(
            row(
                payload.l1_field,
                row(
                    payload.level2.l2_field,
                    row(payload.level2.level3.l3_field)
                )
            ) as row(
                l1_field varchar,
                level2 row(
                    l2_field varchar,
                    level3 row(l3_field varchar)
                )
            )
        ) as payload
    from source_data
    where id <= 2
{% endif %}
"""


@pytest.mark.iceberg
class TestIncrementalNestedRowOnSchemaChange:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "row_base.sql": _MODELS__ROW_BASE,
            "incremental_row_append.sql": _MODELS__INCREMENTAL_ROW_APPEND,
            "incremental_row_append_expected.sql": _MODELS__INCREMENTAL_ROW_APPEND_EXPECTED,
            "incremental_row_sync.sql": _MODELS__INCREMENTAL_ROW_SYNC,
            "incremental_row_sync_expected.sql": _MODELS__INCREMENTAL_ROW_SYNC_EXPECTED,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "incremental_nested_row_on_schema_change",
            "models": {"+incremental_strategy": "merge"},
        }

    def test_incremental_append_nested_row_fields(self, project):
        run_dbt(["run", "--models", "row_base incremental_row_append"])
        run_dbt(["run", "--models", "row_base incremental_row_append"])
        run_dbt(["run", "--models", "incremental_row_append_expected"])
        check_relations_equal(
            project.adapter,
            ["incremental_row_append", "incremental_row_append_expected"],
        )

    def test_incremental_sync_nested_row_fields(self, project):
        run_dbt(["run", "--models", "row_base incremental_row_sync"])
        run_dbt(["run", "--models", "row_base incremental_row_sync"])
        run_dbt(["run", "--models", "incremental_row_sync_expected"])
        check_relations_equal(
            project.adapter,
            ["incremental_row_sync", "incremental_row_sync_expected"],
        )


@pytest.mark.iceberg
class TestIncrementalDeeplyNestedRowOnSchemaChange:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "deeply_nested_row_base.sql": _MODELS__DEEPLY_NESTED_ROW_BASE,
            "incremental_deeply_nested_row_append.sql": _MODELS__INCREMENTAL_DEEPLY_NESTED_ROW_APPEND,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "incremental_deeply_nested_row_on_schema_change",
            "models": {"+incremental_strategy": "merge"},
        }

    def test_incremental_append_deeply_nested_row_fields(self, project):
        run_dbt(
            [
                "run",
                "--models",
                "deeply_nested_row_base incremental_deeply_nested_row_append",
            ]
        )
        run_dbt(
            [
                "run",
                "--models",
                "deeply_nested_row_base incremental_deeply_nested_row_append",
            ]
        )

        relation = project.adapter.Relation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="incremental_deeply_nested_row_append",
        )
        result = project.run_sql(f"SELECT COUNT(*) as cnt FROM {relation}", fetch="one")
        assert result[0] == 3
