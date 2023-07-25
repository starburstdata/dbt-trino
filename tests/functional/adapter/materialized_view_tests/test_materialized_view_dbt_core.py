from typing import Optional, Tuple

import pytest
from dbt.adapters.base.relation import BaseRelation
from dbt.tests.adapter.materialized_view.basic import MaterializedViewBasic
from dbt.tests.util import get_model_file, run_dbt, run_sql_with_adapter, set_model_file

from tests.functional.adapter.materialized_view_tests.utils import query_relation_type


@pytest.mark.iceberg
class TestTrinoMaterializedViewsBasic(MaterializedViewBasic):
    @staticmethod
    def insert_record(project, table: BaseRelation, record: Tuple[int, int]):
        my_id, value = record
        project.run_sql(f"insert into {table} (id, value) values ({my_id}, {value})")

    @staticmethod
    def refresh_materialized_view(project, materialized_view: BaseRelation):
        sql = f"refresh materialized view {materialized_view}"
        project.run_sql(sql)

    @staticmethod
    def query_row_count(project, relation: BaseRelation) -> int:
        sql = f"select count(*) from {relation}"
        return project.run_sql(sql, fetch="one")[0]

    @staticmethod
    def query_relation_type(project, relation: BaseRelation) -> Optional[str]:
        return query_relation_type(project, relation)

    # TODO: remove `setup` fixture when CASCADE will be supported in Iceberg, delta, hive connectors
    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project, my_materialized_view):
        run_dbt(["seed"])
        run_dbt(["run", "--models", my_materialized_view.identifier, "--full-refresh"])

        # the tests touch these files, store their contents in memory
        initial_model = get_model_file(project, my_materialized_view)

        yield

        # and then reset them after the test runs
        set_model_file(project, my_materialized_view, initial_model)

        # Drop materialized views first, then drop schema
        sql = "select * from system.metadata.materialized_views"
        results = run_sql_with_adapter(project.adapter, sql, fetch="all")
        for mv in results:
            project.run_sql(f"drop materialized view {mv[0]}.{mv[1]}.{mv[2]}")

        relation = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema
        )
        project.adapter.drop_schema(relation)

    @pytest.mark.skip(
        reason="""
    on iceberg:
    If the data is outdated, the materialized view behaves like a normal view,
    and the data is queried directly from the base tables.
    https://trino.io/docs/current/connector/iceberg.html#materialized-views
    """
    )
    def test_materialized_view_only_updates_after_refresh(self):
        pass
