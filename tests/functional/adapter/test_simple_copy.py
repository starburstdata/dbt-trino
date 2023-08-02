import pytest
from dbt.tests.adapter.simple_copy.test_simple_copy import (
    EmptyModelsArentRunBase,
    SimpleCopyBase,
)
from dbt.tests.util import run_dbt


@pytest.mark.iceberg
class TestSimpleCopyBase(SimpleCopyBase):
    def test_simple_copy_with_materialized_views(self, project):
        project.run_sql(f"create table {project.test_schema}.unrelated_table (id int)")
        sql = f"""
            create materialized view {project.test_schema}.unrelated_materialized_view as (
                select * from {project.test_schema}.unrelated_table
            )
        """
        project.run_sql(sql)
        sql = f"""
            create view {project.test_schema}.unrelated_view as (
                select * from {project.test_schema}.unrelated_materialized_view
            )
        """
        project.run_sql(sql)
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt()
        assert len(results) == 7

        # clean up
        # TODO: check if this clean-up is still needed
        #  after implementing CASCADE in iceberg, delta, hive connectors
        #  if not, entire method could be deleted
        project.run_sql("drop view unrelated_view")
        project.run_sql("drop materialized view unrelated_materialized_view")
        project.run_sql("drop table unrelated_table")


# Trino implementation of dbt.tests.fixtures.project.TestProjInfo.get_tables_in_schema
# which use `like` instead of `ilike`
def trino_get_tables_in_schema(prj):
    sql = """
            select table_name,
                    case when table_type = 'BASE TABLE' then 'table'
                         when table_type = 'VIEW' then 'view'
                         else table_type
                    end as materialization
            from information_schema.tables
            where {}
            order by table_name
            """
    sql = sql.format("lower({}) like lower('{}')".format("table_schema", prj.test_schema))
    result = prj.run_sql(sql, fetch="all")
    return {model_name: materialization for (model_name, materialization) in result}


class TestEmptyModelsArentRun(EmptyModelsArentRunBase):
    def test_dbt_doesnt_run_empty_models(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt()
        assert len(results) == 7

        tables = trino_get_tables_in_schema(project)

        assert "empty" not in tables.keys()
        assert "disabled" not in tables.keys()
