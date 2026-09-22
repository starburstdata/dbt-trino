import pytest
from dbt.tests.util import check_relations_equal, get_relation_columns, run_dbt

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

select 1 as id, cast(row('foo', cast(null as varchar)) as row(nested_field varchar, extra_field varchar)) as payload
union all
select 2 as id, cast(row('bar', 'baz') as row(nested_field varchar, extra_field varchar)) as payload
union all
select 3 as id, cast(row('qux', 'quux') as row(nested_field varchar, extra_field varchar)) as payload
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

select 1 as id, cast(row('foo') as row(nested_field varchar)) as payload
union all
select 2 as id, cast(row('bar') as row(nested_field varchar)) as payload
order by id
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

_MODELS__INCREMENTAL_DEEPLY_NESTED_ROW_APPEND_EXPECTED = """
{{
    config(materialized='table')
}}

select 1 as id,
    cast(
        row(
            'level1',
            row(
                'level2',
                row('level3', 'new_l3'),
                'new_l2'
            ),
            'new_l1'
        ) as row(
            l1_field varchar,
            level2 row(
                l2_field varchar,
                level3 row(l3_field varchar, l3_new_field varchar),
                l2_new_field varchar
            ),
            l1_new_field varchar
        )
    ) as payload
union all
select 2 as id,
    cast(
        row(
            'level1_b',
            row(
                'level2_b',
                row('level3_b', 'new_l3_b'),
                'new_l2_b'
            ),
            'new_l1_b'
        ) as row(
            l1_field varchar,
            level2 row(
                l2_field varchar,
                level3 row(l3_field varchar, l3_new_field varchar),
                l2_new_field varchar
            ),
            l1_new_field varchar
        )
    ) as payload
union all
select 3 as id,
    cast(
        row(
            'level1_c',
            row(
                'level2_c',
                row('level3_c', 'new_l3_c'),
                'new_l2_c'
            ),
            'new_l1_c'
        ) as row(
            l1_field varchar,
            level2 row(
                l2_field varchar,
                level3 row(l3_field varchar, l3_new_field varchar),
                l2_new_field varchar
            ),
            l1_new_field varchar
        )
    ) as payload
order by id
"""


@pytest.mark.iceberg
class TestIncrementalNestedRowOnSchemaChange:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_row_append.sql": _MODELS__INCREMENTAL_ROW_APPEND,
            "incremental_row_append_expected.sql": _MODELS__INCREMENTAL_ROW_APPEND_EXPECTED,
            "incremental_row_sync.sql": _MODELS__INCREMENTAL_ROW_SYNC,
            "incremental_row_sync_expected.sql": _MODELS__INCREMENTAL_ROW_SYNC_EXPECTED,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "incremental_nested_row_on_schema_change",
            "models": {
                "+incremental_strategy": "merge",
                "+sync_nested_columns": True,
            },
        }

    def test_incremental_append_nested_row_fields(self, project):
        run_dbt(["run", "--models", "incremental_row_append"])
        run_dbt(["run", "--models", "incremental_row_append"])
        run_dbt(["run", "--models", "incremental_row_append_expected"])
        check_relations_equal(
            project.adapter,
            ["incremental_row_append", "incremental_row_append_expected"],
        )

    def test_incremental_sync_nested_row_fields(self, project):
        run_dbt(["run", "--models", "incremental_row_sync"])
        run_dbt(["run", "--models", "incremental_row_sync"])
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
            "incremental_deeply_nested_row_append.sql": _MODELS__INCREMENTAL_DEEPLY_NESTED_ROW_APPEND,
            "incremental_deeply_nested_row_append_expected.sql": _MODELS__INCREMENTAL_DEEPLY_NESTED_ROW_APPEND_EXPECTED,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "incremental_deeply_nested_row_on_schema_change",
            "models": {
                "+incremental_strategy": "merge",
                "+sync_nested_columns": True,
            },
        }

    def test_incremental_append_deeply_nested_row_fields(self, project):
        run_dbt(["run", "--models", "incremental_deeply_nested_row_append"])
        run_dbt(["run", "--models", "incremental_deeply_nested_row_append"])
        run_dbt(["run", "--models", "incremental_deeply_nested_row_append_expected"])
        check_relations_equal(
            project.adapter,
            [
                "incremental_deeply_nested_row_append",
                "incremental_deeply_nested_row_append_expected",
            ],
        )


@pytest.mark.iceberg
class TestIncrementalNestedRowDefaultBehavior:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_row_append.sql": _MODELS__INCREMENTAL_ROW_APPEND,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "incremental_nested_row_default_behavior",
            "models": {"+incremental_strategy": "merge"},
        }

    def test_nested_row_schema_change_skipped_by_default(self, project):
        run_dbt(["run", "--models", "incremental_row_append"])
        results = run_dbt(
            ["run", "--models", "incremental_row_append"],
            expect_pass=False,
        )
        failed_results = [result for result in results if result.status == "error"]
        assert len(failed_results) == 1
        assert "TYPE_MISMATCH" in failed_results[0].message


_MODELS__INCREMENTAL_ROW_TYPE_CHANGE = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='sync_all_columns',
        incremental_strategy='merge'
    )
}}

with source_data as (
    select 1 as id, cast(row(1) as row(nested_field bigint)) as payload union all
    select 2 as id, cast(row(2) as row(nested_field bigint)) as payload
)

{% if is_incremental() %}
    select * from source_data
{% else %}
    select 1 as id, cast(row(1) as row(nested_field integer)) as payload
    union all
    select 2 as id, cast(row(2) as row(nested_field integer)) as payload
{% endif %}
"""

_MODELS__INCREMENTAL_ROW_TYPE_CHANGE_EXPECTED = """
{{
    config(materialized='table')
}}

select 1 as id, cast(row(1) as row(nested_field bigint)) as payload
union all
select 2 as id, cast(row(2) as row(nested_field bigint)) as payload
order by id
"""

_MODELS__INCREMENTAL_ROW_APPEND_KEEPS_REMOVED = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='append_new_columns',
        incremental_strategy='merge'
    )
}}

with source_data as (
    select 1 as id, cast(row('foo', 'keep_me') as row(nested_field varchar, extra_field varchar)) as payload union all
    select 2 as id, cast(row('bar', 'keep_too') as row(nested_field varchar, extra_field varchar)) as payload
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

_MODELS__INCREMENTAL_ROW_APPEND_KEEPS_REMOVED_EXPECTED = """
{{
    config(materialized='table')
}}

select 1 as id, cast(row('foo', 'keep_me') as row(nested_field varchar, extra_field varchar)) as payload
union all
select 2 as id, cast(row('bar', 'keep_too') as row(nested_field varchar, extra_field varchar)) as payload
order by id
"""

_MODELS__INCREMENTAL_QUOTED_ROW_FIELD = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='append_new_columns',
        incremental_strategy='merge'
    )
}}

with source_data as (
    select 1 as id, cast(row(10, 20) as row("Order" bigint, "Total" bigint)) as payload
)

{% if is_incremental() %}
    select * from source_data
{% else %}
    select id, cast(row(payload."Order") as row("Order" bigint)) as payload from source_data
{% endif %}
"""

_MODELS__INCREMENTAL_QUOTED_ROW_FIELD_EXPECTED = """
{{
    config(materialized='table')
}}

select 1 as id, cast(row(10, 20) as row("Order" bigint, "Total" bigint)) as payload
"""

_MODELS__INCREMENTAL_DEEPLY_NESTED_ROW_SYNC = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='sync_all_columns',
        incremental_strategy='merge'
    )
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

{% if is_incremental() %}
    select * from source_data
{% else %}
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
{% endif %}
"""

_MODELS__INCREMENTAL_DEEPLY_NESTED_ROW_SYNC_EXPECTED = """
{{
    config(materialized='table')
}}

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
order by id
"""

_MODELS__INCREMENTAL_ROW_TYPE_CHANGE_DEFAULT = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='sync_all_columns',
        incremental_strategy='merge'
    )
}}

with source_data as (
    select 1 as id, cast(row(1) as row(nested_field bigint)) as payload
)

{% if is_incremental() %}
    select * from source_data
{% else %}
    select 1 as id, cast(row(1) as row(nested_field integer)) as payload
{% endif %}
"""


@pytest.mark.iceberg
class TestIncrementalNestedRowTypeChange:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_row_type_change.sql": _MODELS__INCREMENTAL_ROW_TYPE_CHANGE,
            "incremental_row_type_change_expected.sql": _MODELS__INCREMENTAL_ROW_TYPE_CHANGE_EXPECTED,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "incremental_nested_row_type_change",
            "models": {
                "+incremental_strategy": "merge",
                "+sync_nested_columns": True,
            },
        }

    def test_incremental_sync_nested_row_type_change(self, project):
        run_dbt(["run", "--models", "incremental_row_type_change"])
        run_dbt(["run", "--models", "incremental_row_type_change"])
        run_dbt(["run", "--models", "incremental_row_type_change_expected"])
        check_relations_equal(
            project.adapter,
            ["incremental_row_type_change", "incremental_row_type_change_expected"],
        )


@pytest.mark.iceberg
class TestIncrementalNestedRowAppendKeepsRemovedField:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_row_append_keeps_removed.sql": _MODELS__INCREMENTAL_ROW_APPEND_KEEPS_REMOVED,
            "incremental_row_append_keeps_removed_expected.sql": _MODELS__INCREMENTAL_ROW_APPEND_KEEPS_REMOVED_EXPECTED,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "incremental_nested_row_append_keeps_removed",
            "models": {
                "+incremental_strategy": "merge",
                "+sync_nested_columns": True,
            },
        }

    def test_append_mode_does_not_drop_removed_nested_field(self, project):
        run_dbt(["run", "--models", "incremental_row_append_keeps_removed"])
        results = run_dbt(
            ["run", "--models", "incremental_row_append_keeps_removed"],
            expect_pass=False,
        )
        failed_results = [result for result in results if result.status == "error"]
        assert len(failed_results) == 1
        assert "TYPE_MISMATCH" in failed_results[0].message

        payload_type = next(
            column[1]
            for column in get_relation_columns(
                project.adapter, "incremental_row_append_keeps_removed"
            )
            if column[0] == "payload"
        )
        assert "extra_field" in payload_type


@pytest.mark.iceberg
class TestIncrementalQuotedNestedRowField:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_quoted_row_field.sql": _MODELS__INCREMENTAL_QUOTED_ROW_FIELD,
            "incremental_quoted_row_field_expected.sql": _MODELS__INCREMENTAL_QUOTED_ROW_FIELD_EXPECTED,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "incremental_quoted_nested_row_field",
            "models": {
                "+incremental_strategy": "merge",
                "+sync_nested_columns": True,
            },
        }

    def test_incremental_append_quoted_nested_row_field(self, project):
        run_dbt(["run", "--models", "incremental_quoted_row_field"])
        run_dbt(["run", "--models", "incremental_quoted_row_field"])
        run_dbt(["run", "--models", "incremental_quoted_row_field_expected"])
        check_relations_equal(
            project.adapter,
            ["incremental_quoted_row_field", "incremental_quoted_row_field_expected"],
        )


@pytest.mark.iceberg
class TestIncrementalDeeplyNestedRowSync:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_deeply_nested_row_sync.sql": _MODELS__INCREMENTAL_DEEPLY_NESTED_ROW_SYNC,
            "incremental_deeply_nested_row_sync_expected.sql": _MODELS__INCREMENTAL_DEEPLY_NESTED_ROW_SYNC_EXPECTED,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "incremental_deeply_nested_row_sync",
            "models": {
                "+incremental_strategy": "merge",
                "+sync_nested_columns": True,
            },
        }

    def test_incremental_sync_deeply_nested_row_removals(self, project):
        run_dbt(["run", "--models", "incremental_deeply_nested_row_sync"])
        run_dbt(["run", "--models", "incremental_deeply_nested_row_sync"])
        run_dbt(["run", "--models", "incremental_deeply_nested_row_sync_expected"])
        check_relations_equal(
            project.adapter,
            [
                "incremental_deeply_nested_row_sync",
                "incremental_deeply_nested_row_sync_expected",
            ],
        )


@pytest.mark.iceberg
class TestIncrementalNestedRowDefaultTypeChange:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_row_type_change_default.sql": _MODELS__INCREMENTAL_ROW_TYPE_CHANGE_DEFAULT,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "incremental_nested_row_default_type_change",
            "models": {"+incremental_strategy": "merge"},
        }

    def test_nested_row_type_change_without_nested_sync_opt_in(self, project):
        run_dbt(["run", "--models", "incremental_row_type_change_default"])
        run_dbt(["run", "--models", "incremental_row_type_change_default"])

        payload_type = next(
            column[1]
            for column in get_relation_columns(
                project.adapter, "incremental_row_type_change_default"
            )
            if column[0] == "payload"
        )
        assert "bigint" in payload_type.lower()
