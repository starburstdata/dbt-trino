from dbt.tests.adapter.dbt_debug.test_dbt_debug import (
    BaseDebug,
    BaseDebugProfileVariable,
)
from dbt.tests.util import run_dbt


class TestDebugTrino(BaseDebug):
    def test_ok_trino(self, project):
        run_dbt(["debug"])
        assert "ERROR" not in self.capsys.readouterr().out


class TestDebugProfileVariableTrino(BaseDebugProfileVariable):
    def test_ok_trino(self, project):
        run_dbt(["debug"])
        assert "ERROR" not in self.capsys.readouterr().out
