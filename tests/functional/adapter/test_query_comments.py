import json

import pytest
from dbt.exceptions import DbtRuntimeError
from dbt.tests.adapter.query_comment.test_query_comment import (
    BaseDefaultQueryComments,
    BaseEmptyQueryComments,
    BaseMacroArgsQueryComments,
    BaseMacroInvalidQueryComments,
    BaseMacroQueryComments,
    BaseNullQueryComments,
    BaseQueryComments,
)
from dbt.tests.util import run_dbt_and_capture
from dbt.version import __version__ as dbt_version


# TODO: below tests could be simplified to just
# pass statements, when tests in dbt.tests.adapter
# will be fixed
class BaseDefaultQueryCommentsTrino(BaseDefaultQueryComments):
    def run_get_json(self, expect_pass=True):
        res, raw_logs = run_dbt_and_capture(
            ["--debug", "--log-format=json", "run"], expect_pass=expect_pass
        )

        # empty lists evaluate as False
        assert len(res) > 0
        query = res[0].adapter_response["query"]
        return raw_logs, query


class TestQueryCommentsTrino(BaseDefaultQueryCommentsTrino, BaseQueryComments):
    def test_matches_comment(self) -> bool:
        logs, query = self.run_get_json()
        assert r"/* dbt\nrules! */\n" in logs
        assert query.startswith("/* dbt\nrules! */\n")


class TestMacroQueryCommentsTrino(BaseDefaultQueryCommentsTrino, BaseMacroQueryComments):
    def test_matches_comment(self) -> bool:
        logs, query = self.run_get_json()
        assert r"/* dbt macros\nare pretty cool */\n" in logs
        assert query.startswith("/* dbt macros\nare pretty cool */\n")


class TestMacroArgsQueryCommentsTrino(BaseDefaultQueryCommentsTrino, BaseMacroArgsQueryComments):
    def test_matches_comment(self) -> bool:
        logs, query = self.run_get_json()
        expected_dct = {
            "app": "dbt++",
            "dbt_version": dbt_version,
            "macro_version": "0.1.0",
            "message": "blah: default",
        }
        expected = "/* {} */\n".format(json.dumps(expected_dct, sort_keys=True))
        assert expected in query


class TestMacroInvalidQueryCommentsTrino(BaseMacroInvalidQueryComments):
    def test_run_assert_comments(self):
        with pytest.raises(DbtRuntimeError):
            self.run_get_json(expect_pass=False)


class TestNullQueryCommentsTrino(BaseDefaultQueryCommentsTrino, BaseNullQueryComments):
    def test_matches_comment(self) -> bool:
        logs, query = self.run_get_json()
        assert "/*" not in logs or "*/" not in logs
        assert not query.startswith("/*")


class TestEmptyQueryCommentsTrino(BaseDefaultQueryCommentsTrino, BaseEmptyQueryComments):
    def test_matches_comment(self) -> bool:
        logs, query = self.run_get_json()
        assert "/*" not in logs or "*/" not in logs
        assert not query.startswith("/*")
