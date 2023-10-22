import pytest
from dbt.tests.adapter.utils.base_utils import BaseUtils
from dbt.tests.adapter.utils.fixture_date_spine import models__test_date_spine_yml

from tests.functional.adapter.utils.fixture_date_spine import (
    models__trino_test_date_spine_sql,
)


class BaseDateSpine(BaseUtils):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_date_spine.yml": models__test_date_spine_yml,
            "test_date_spine.sql": self.interpolate_macro_namespace(
                models__trino_test_date_spine_sql, "date_spine"
            ),
        }


class TestDateSpine(BaseDateSpine):
    pass
