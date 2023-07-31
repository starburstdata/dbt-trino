import pytest
from dbt.tests.adapter.dbt_debug.test_dbt_debug import (
    BaseDebug,
    BaseDebugProfileVariable,
)
from dbt.tests.util import run_dbt


class TestDebugTrino(BaseDebug):
    # TODO: below teardown method probably should be implemented in base class (on dbt-core side)
    @pytest.fixture(scope="function", autouse=True)
    def teardown_method(self, project):
        yield
        project.run_sql(f"drop schema if exists {project.test_schema}")

    def test_ok_trino(self, project):
        run_dbt(["debug"])
        assert "ERROR" not in self.capsys.readouterr().out


class TestDebugProfileVariableTrino(BaseDebugProfileVariable):
    # TODO: below teardown method probably should be implemented in base class (on dbt-core side)
    @pytest.fixture(scope="function", autouse=True)
    def teardown_method(self, project):
        yield
        project.run_sql(f"drop schema if exists {project.test_schema}")

    def test_ok_trino(self, project):
        run_dbt(["debug"])
        assert "ERROR" not in self.capsys.readouterr().out
