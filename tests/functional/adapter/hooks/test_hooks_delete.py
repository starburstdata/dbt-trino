# Test hooks with DELETE statement
import pytest
from dbt.tests.util import run_dbt, run_sql_with_adapter

seed = """
id,name,some_date
1,Easton,1981-05-20
2,Lillian,1978-09-03
3,Jeremiah,1982-03-11
4,Nolan,1976-05-06
5,Hannah,1982-06-23ľ
6,Eleanor,1991-08-10
7,Lily,1971-03-29
8,Jonathan,1988-02-26
9,Adrian,1994-02-09
10,Nora,1976-03-01
""".lstrip()

model = """
  {{ config(
        materialized="table",
        on_table_exists = 'drop'
     )
  }}
  select * from {{ ref('seed') }}
"""


class BaseTestHooksDelete:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": seed,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": model,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "pre-hook": "DELETE FROM seed WHERE name IN ('Jeremiah','Eleanor');",
                "post-hook": "DELETE FROM seed WHERE name IN ('Nolan','Jonathan','Nora');",
            }
        }

    def test_pre_and_post_run_hooks(self, project, dbt_profile_target):
        # Run seed
        results = run_dbt(["seed"], expect_pass=True)
        assert len(results) == 1

        # Check if table has all rows
        sql_seed = "SELECT COUNT(*) from seed"
        query_results = run_sql_with_adapter(project.adapter, sql_seed, fetch="all")
        assert query_results[0][0] == 10

        # Run model, hooks should run DELETE statements
        results = run_dbt(["run"], expect_pass=True)
        assert len(results) == 1

        # 2 rows were deleted in pre-hook
        sql_model = "SELECT COUNT(*) from model"
        query_results = run_sql_with_adapter(project.adapter, sql_model, fetch="all")
        assert query_results[0][0] == 8

        # 2 rows were deleted in pre-hook, and 3 in post-hook
        query_results = run_sql_with_adapter(project.adapter, sql_seed, fetch="all")
        assert query_results[0][0] == 5


@pytest.mark.delta
class TestBaseTestHooksDeleteDelta(BaseTestHooksDelete):
    pass


@pytest.mark.iceberg
class TestBaseTestHooksDeleteIceberg(BaseTestHooksDelete):
    pass
