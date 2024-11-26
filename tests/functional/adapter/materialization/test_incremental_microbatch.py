import pytest
from dbt.tests.adapter.incremental.test_incremental_microbatch import BaseMicrobatch


@pytest.mark.iceberg
class TestTrinoMicrobatchIceberg(BaseMicrobatch):
    pass
