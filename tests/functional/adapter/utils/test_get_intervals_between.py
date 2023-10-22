import pytest
from dbt.tests.adapter.utils.base_utils import BaseUtils
from dbt.tests.adapter.utils.fixture_get_intervals_between import (
    models__test_get_intervals_between_yml,
)

from tests.functional.adapter.utils.fixture_get_intervals_between import (
    models__trino_test_get_intervals_between_sql,
)


class BaseGetIntervalsBetween(BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_get_intervals_between.yml": models__test_get_intervals_between_yml,
            "test_get_intervals_between.sql": self.interpolate_macro_namespace(
                models__trino_test_get_intervals_between_sql, "get_intervals_between"
            ),
        }


class TestGetIntervalsBetween(BaseGetIntervalsBetween):
    pass
