import pytest

from tests.functional.adapter.test_basic import TestIncrementalTrino


@pytest.fixture(scope="class")
def unique_schema(request, prefix) -> str:
    return "ScHeMaWiThMiXeDCaSe"


class TestTrinoQuotePolicy(TestIncrementalTrino):
    pass
