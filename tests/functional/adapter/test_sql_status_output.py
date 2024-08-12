import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture

seed_csv = """
id,name,some_date
1,Easton,1981-05-20 06:46:51
2,Lillian,1978-09-03 18:10:33
3,Jeremiah,1982-03-11 03:59:51
4,Nolan,1976-05-06 20:21:35
""".lstrip()

model_sql = """
select * from {{ ref('seed') }}
"""


class TestSqlStatusOutput:
    """
    Testing if SQL status output contains update_type and rowcount
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": seed_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "materialization_table.sql": model_sql,
            "materialization_view.sql": model_sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "sql_status_output",
            "models": {
                "sql_status_output": {
                    "materialization_table": {"+materialized": "table"},
                    "materialization_view": {"+materialized": "view"},
                }
            },
        }

    def test_run_seed_test(self, project):
        results = run_dbt(["seed"], expect_pass=True)
        assert len(results) == 1

        results, logs = run_dbt_and_capture(["--no-use-colors", "run"], expect_pass=True)
        assert len(results) == 2
        assert (
            f" of 2 OK created sql table model {project.test_schema}.materialization_table  [CREATE TABLE (4 rows) in "
            in logs
        )
        assert (
            f" of 2 OK created sql view model {project.test_schema}.materialization_view  [CREATE VIEW in "
            in logs
        )
