from typing import Optional

from dbt.adapters.base.relation import BaseRelation

from dbt.adapters.trino.relation import TrinoRelation


def query_relation_type(project, relation: BaseRelation) -> Optional[str]:
    assert isinstance(relation, TrinoRelation)
    sql = f"""
    select
      case when mv.name is not null then 'materialized_view'
           when t.table_type = 'BASE TABLE' then 'table'
           when t.table_type = 'VIEW' then 'view'
           else t.table_type
      end as table_type
    from {relation.information_schema()}.tables t
    left join system.metadata.materialized_views mv
          on mv.catalog_name = t.table_catalog and mv.schema_name = t.table_schema and mv.name = t.table_name
    where t.table_schema = '{relation.schema.lower()}'
          and (mv.catalog_name is null or mv.catalog_name =  '{relation.database.lower()}')
          and (mv.schema_name is null or mv.schema_name =  '{relation.schema.lower()}')
          and t.table_name = '{relation.identifier.lower()}'
    """
    results = project.run_sql(sql, fetch="all")
    if len(results) == 0:
        return None
    elif len(results) > 1:
        raise ValueError(f"More than one instance of {relation.name} found!")
    else:
        return results[0][0]
