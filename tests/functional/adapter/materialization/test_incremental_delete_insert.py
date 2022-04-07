from dbt.tests.adapter.basic.files import (
    schema_base_yml,
    model_incremental,
)
import pytest
from dbt.tests.util import run_dbt, check_relations_equal, relation_from_name


config_materialized_incremental = """
  {{ config(materialized="incremental", incremental_strategy="delete+insert", unique_key="id") }}
"""
incremental_sql = config_materialized_incremental + model_incremental

seeds_base_csv = """
id,name,some_date
1,Easton,1981-05-20T06:46:51
2,Lillian,1978-09-03T18:10:33
3,Jeremiah,1982-03-11T03:59:51
4,Nolan,1976-05-06T20:21:35
5,Hannah,1982-06-23T05:41:26
6,Eleanor,1991-08-10T23:12:21
7,Lily,1971-03-29T14:58:02
8,Jonathan,1988-02-26T02:55:24
9,Adrian,1994-02-09T13:14:23
10,Nora,1976-03-01T16:51:39
""".lstrip()

seeds_updated_csv = """
id,name,some_date
1,Easton,1981-05-20T06:46:51
2,Lillian,1978-09-03T18:10:33
3,Jeremiah,1982-03-11T03:59:51
7,Lily,1989-03-29T14:58:02
8,Jona,1988-02-26T02:55:24
9,Adriana,1994-02-09T13:14:28
11,Mateo,2014-09-07T17:04:27
12,Julian,2000-02-04T11:48:30
13,Gabriel,2001-07-10T07:32:52
14,Isaac,2002-11-24T03:22:28
15,Levi,2009-11-15T11:57:15
16,Elizabeth,2005-04-09T03:50:11
17,Grayson,2019-08-06T19:28:17
18,Dylan,2014-03-01T11:50:41
19,Jayden,2009-06-06T07:12:49
20,Luke,2003-12-05T21:42:18
""".lstrip()


@pytest.mark.postgresql
class TestIncremeantalDeleteInsert:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "incremental"}

    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental.sql": incremental_sql, "schema.yml": schema_base_yml}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"base.csv": seeds_base_csv, "updated.csv": seeds_updated_csv}

    def test_incremental_delete_insert(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 2

        # base table rowcount
        relation = relation_from_name(project.adapter, "base")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 10

        # added table rowcount
        relation = relation_from_name(project.adapter, "updated")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 16

        # run command
        # the "seed_name" var changes the seed identifier in the schema file
        results = run_dbt(["run", "--vars", "seed_name: base"])
        assert len(results) == 1

        # check relations equal
        check_relations_equal(project.adapter, ["base", "incremental"])

        # change seed_name var
        # the "seed_name" var changes the seed identifier in the schema file
        results = run_dbt(["run", "--vars", "seed_name: updated"])
        assert len(results) == 1

        # check relations equal
        check_relations_equal(project.adapter, ["updated", "incremental"])

        # get catalog from docs generate
        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == 3
        assert len(catalog.sources) == 1
