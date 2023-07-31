import pytest
from dbt.tests.adapter.dbt_clone.fixtures import (
    custom_can_clone_tables_false_macros_sql,
    get_schema_name_sql,
    infinite_macros_sql,
    macros_sql,
)
from dbt.tests.adapter.dbt_clone.test_dbt_clone import BaseCloneNotPossible

iceberg_macro_override_sql = """
{% macro trino__current_timestamp() -%}
    current_timestamp(6)
{%- endmacro %}
"""


class TestTrinoCloneNotPossible(BaseCloneNotPossible):
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "macros.sql": macros_sql,
            "my_can_clone_tables.sql": custom_can_clone_tables_false_macros_sql,
            "infinite_macros.sql": infinite_macros_sql,
            "get_schema_name.sql": get_schema_name_sql,
            "iceberg.sql": iceberg_macro_override_sql,
        }

    # TODO: below method probably should be implemented in base class (on dbt-core side)
    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=f"{project.test_schema}_seeds"
            )
            project.adapter.drop_schema(relation)

            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)
