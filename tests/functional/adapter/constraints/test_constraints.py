import pytest
from dbt.tests.adapter.constraints.fixtures import (
    my_incremental_model_sql,
    my_model_incremental_wrong_name_sql,
    my_model_incremental_wrong_order_sql,
    my_model_sql,
    my_model_view_wrong_name_sql,
    my_model_view_wrong_order_sql,
    my_model_wrong_name_sql,
    my_model_wrong_order_sql,
)
from dbt.tests.adapter.constraints.test_constraints import (
    BaseConstraintsRollback,
    BaseConstraintsRuntimeDdlEnforcement,
    BaseIncrementalConstraintsColumnsEqual,
    BaseIncrementalConstraintsRollback,
    BaseIncrementalConstraintsRuntimeDdlEnforcement,
    BaseModelConstraintsRuntimeEnforcement,
    BaseTableConstraintsColumnsEqual,
    BaseViewConstraintsColumnsEqual,
)

from tests.functional.adapter.constraints.fixtures import (
    trino_constrained_model_schema_yml,
    trino_model_char_value_to_int_column,
    trino_model_schema_yml,
)

_expected_sql_trino = """
create table <model_identifier> (
    id integer,
    color varchar,
    date_day varchar
) ;
insert into <model_identifier>
(
    select
        id,
        color,
        date_day from
    (
        select
            'blue' as color,
            1 as id,
            '2019-01-01' as date_day
    ) as model_subq
)
;
"""


class TrinoColumnEqualSetup:
    @pytest.fixture
    def string_type(self):
        return "VARCHAR"

    @pytest.fixture
    def data_types(self, schema_int_type, int_type, string_type):
        # sql_column_value, schema_data_type, error_data_type
        return [
            ["1", schema_int_type, int_type],
            ["'1'", string_type, string_type],
            ["cast('2019-01-01' as date)", "date", "DATE"],
            ["true", "boolean", "BOOLEAN"],
            ["cast('2013-11-03 00:00:00-07' as TIMESTAMP)", "timestamp(6)", "TIMESTAMP"],
            [
                "cast('2013-11-03 00:00:00-07' as TIMESTAMP WITH TIME ZONE)",
                "timestamp(6)",
                "TIMESTAMP",
            ],
            ["ARRAY['a','b','c']", "ARRAY(VARCHAR)", "ARRAY"],
            ["ARRAY[1,2,3]", "ARRAY(INTEGER)", "ARRAY"],
            ["cast('1' as DECIMAL)", "DECIMAL", "DECIMAL"],
        ]


class TestTrinoTableConstraintsColumnsEqual(
    TrinoColumnEqualSetup, BaseTableConstraintsColumnsEqual
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_wrong_name_sql,
            "constraints_schema.yml": trino_model_schema_yml,
        }


class TestTrinoViewConstraintsColumnsEqual(TrinoColumnEqualSetup, BaseViewConstraintsColumnsEqual):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_view_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_view_wrong_name_sql,
            "constraints_schema.yml": trino_model_schema_yml,
        }


class TestTrinoIncrementalConstraintsColumnsEqual(
    TrinoColumnEqualSetup, BaseIncrementalConstraintsColumnsEqual
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_incremental_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_incremental_wrong_name_sql,
            "constraints_schema.yml": trino_model_schema_yml,
        }


class TestTrinoTableConstraintsRuntimeDdlEnforcement(BaseConstraintsRuntimeDdlEnforcement):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_wrong_order_sql,
            "constraints_schema.yml": trino_model_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return _expected_sql_trino


class TestTrinoTableConstraintsRollback(BaseConstraintsRollback):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "constraints_schema.yml": trino_model_schema_yml,
        }

    # We are trying to load char value to integer column to break constraint.
    # In BaseConstraintsRollback it is checked by violating not-null constraint,
    # But not-null constraint is not supported in dbt-trino
    @pytest.fixture(scope="class")
    def null_model_sql(self):
        return trino_model_char_value_to_int_column

    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return [
            "Please ensure the name, data_type, and number of columns in your contract match the columns in your model's definition."
        ]


class TestTrinoIncrementalConstraintsRuntimeDdlEnforcement(
    BaseIncrementalConstraintsRuntimeDdlEnforcement
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_incremental_wrong_order_sql,
            "constraints_schema.yml": trino_model_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return _expected_sql_trino


class TestTrinoIncrementalConstraintsRollback(BaseIncrementalConstraintsRollback):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_incremental_model_sql,
            "constraints_schema.yml": trino_model_schema_yml,
        }

    # We are trying to load char value to integer column to break constraint.
    # In BaseConstraintsRollback it is checked by violating not-null constraint,
    # But not-null constraint is not supported in dbt-trino
    @pytest.fixture(scope="class")
    def null_model_sql(self):
        return trino_model_char_value_to_int_column

    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return [
            "Please ensure the name, data_type, and number of columns in your contract match the columns in your model's definition."
        ]


class TestTrinoModelConstraintsRuntimeEnforcement(BaseModelConstraintsRuntimeEnforcement):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "constraints_schema.yml": trino_constrained_model_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return """
create table <model_identifier> (
    id integer,
    color varchar,
    date_day varchar
) ;
insert into <model_identifier>
(
    select
        id,
        color,
        date_day from
    (
        select
            1 as id,
            'blue' as color,
            '2019-01-01' as date_day
    ) as model_subq
)
;
"""
