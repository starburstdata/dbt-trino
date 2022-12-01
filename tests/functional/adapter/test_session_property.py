import pytest
from dbt.tests.util import run_dbt

set_session_property = "set session query_max_run_time='20s'"


class TestSessionProperty:
    """
    This test is ensuring that setting session properties through pre_hook is working as expected.
    Test is asserting, that session property passed in 'pre_hook' config in model definition
    matches pre_hook value extracted from RunExecutionResult object.
    """

    @property
    def schema(self):
        return "default"

    def session_property_model(self, prehook):
        return f"""
                    {{{{
                        config(
                            pre_hook="{prehook}"
                        )
                    }}}}
                    select 'OK' as status
                """

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {"session_property_model.sql": self.session_property_model(set_session_property)}

    def test_custom_schema_trino(self, project):

        # Run models.
        results = run_dbt(["run"], expect_pass=True)
        assert len(results) == 1
        assert set_session_property == results.results[0].node.config.pre_hook[0].sql
