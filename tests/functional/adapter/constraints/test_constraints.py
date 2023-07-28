import pytest
from dbt.tests.adapter.constraints.fixtures import (
    my_incremental_model_sql,
    my_model_incremental_wrong_name_sql,
    my_model_incremental_wrong_order_sql,
    my_model_sql,
    my_model_view_wrong_name_sql,
    my_model_view_wrong_order_sql,
    my_model_with_quoted_column_name_sql,
    my_model_wrong_name_sql,
    my_model_wrong_order_sql,
)
from dbt.tests.adapter.constraints.test_constraints import (
    BaseConstraintQuotedColumn,
    BaseConstraintsRollback,
    BaseConstraintsRuntimeDdlEnforcement,
    BaseIncrementalConstraintsColumnsEqual,
    BaseIncrementalConstraintsRollback,
    BaseIncrementalConstraintsRuntimeDdlEnforcement,
    BaseIncrementalContractSqlHeader,
    BaseModelConstraintsRuntimeEnforcement,
    BaseTableConstraintsColumnsEqual,
    BaseTableContractSqlHeader,
    BaseViewConstraintsColumnsEqual,
)

from tests.functional.adapter.constraints.fixtures import (
    trino_constrained_model_schema_yml,
    trino_model_contract_header_schema_yml,
    trino_model_contract_sql_header_sql,
    trino_model_incremental_contract_sql_header_sql,
    trino_model_quoted_column_schema_yml,
    trino_model_schema_yml,
)

_expected_sql_trino = """
create table <model_identifier> (
    "id" integer not null,
    color varchar,
    date_day varchar
) ;
insert into <model_identifier>
(
    select
        "id",
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


@pytest.mark.iceberg
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


@pytest.mark.iceberg
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


@pytest.mark.iceberg
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


@pytest.mark.iceberg
class TestTrinoTableConstraintsRollback(BaseConstraintsRollback):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "constraints_schema.yml": trino_model_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["NULL value not allowed for NOT NULL column: id"]


@pytest.mark.iceberg
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


@pytest.mark.iceberg
class TestTrinoIncrementalConstraintsRollback(BaseIncrementalConstraintsRollback):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_incremental_model_sql,
            "constraints_schema.yml": trino_model_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["NULL value not allowed for NOT NULL column: id"]


class TestTrinoTableContractSqlHeader(BaseTableContractSqlHeader):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_contract_sql_header.sql": trino_model_contract_sql_header_sql,
            "constraints_schema.yml": trino_model_contract_header_schema_yml,
        }


class TestTrinoIncrementalContractSqlHeader(BaseIncrementalContractSqlHeader):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_contract_sql_header.sql": trino_model_incremental_contract_sql_header_sql,
            "constraints_schema.yml": trino_model_contract_header_schema_yml,
        }


@pytest.mark.iceberg
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
    "id" integer not null,
    color varchar,
    date_day varchar
) ;
insert into <model_identifier>
(
    select
        "id",
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


@pytest.mark.iceberg
class TestTrinoConstraintQuotedColumn(BaseConstraintQuotedColumn):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_with_quoted_column_name_sql,
            "constraints_schema.yml": trino_model_quoted_column_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return """
create table <model_identifier> (
    id integer not null,
    "from" varchar not null,
    date_day varchar
) ;
insert into <model_identifier>
(
    select id, "from", date_day
    from (
        select
          'blue' as "from",
          1 as id,
          '2019-01-01' as date_day
    ) as model_subq
);
"""
