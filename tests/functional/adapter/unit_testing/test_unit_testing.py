import pytest
from dbt.tests.adapter.unit_testing.test_case_insensitivity import (
    BaseUnitTestCaseInsensivity,
)
from dbt.tests.adapter.unit_testing.test_invalid_input import BaseUnitTestInvalidInput
from dbt.tests.adapter.unit_testing.test_types import BaseUnitTestingTypes


class TestTrinoUnitTestingTypes(BaseUnitTestingTypes):
    @pytest.fixture
    def data_types(self):
        # sql_value, yaml_value
        return [
            ["1", "1"],
            ["'1'", "1"],
            ["true", "true"],
            ["DATE '2020-01-02'", "2020-01-02"],
            ["TIMESTAMP '2013-11-03 00:00:00'", "2013-11-03 00:00:00"],
            ["TIMESTAMP '2013-11-03 00:00:00-0'", "2013-11-03 00:00:00-0"],
            ["DECIMAL '1'", "1"],
            [
                """JSON '{"bar": "baz", "balance": 7.77, "active": false}'""",
                """'{"bar": "baz", "balance": 7.77, "active": false}'""",
            ],
        ]


class TestTrinoUnitTestCaseInsensitivity(BaseUnitTestCaseInsensivity):
    pass


class TestTrinoUnitTestInvalidInput(BaseUnitTestInvalidInput):
    pass
