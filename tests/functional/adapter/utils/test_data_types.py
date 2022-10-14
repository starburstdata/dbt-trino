import pytest
from dbt.tests.adapter.utils.data_types.test_type_bigint import BaseTypeBigInt
from dbt.tests.adapter.utils.data_types.test_type_boolean import BaseTypeBoolean
from dbt.tests.adapter.utils.data_types.test_type_float import BaseTypeFloat
from dbt.tests.adapter.utils.data_types.test_type_int import BaseTypeInt
from dbt.tests.adapter.utils.data_types.test_type_numeric import BaseTypeNumeric
from dbt.tests.adapter.utils.data_types.test_type_string import BaseTypeString
from dbt.tests.adapter.utils.data_types.test_type_timestamp import BaseTypeTimestamp


class TestTypeBigInt(BaseTypeBigInt):
    pass


class TestTypeFloat(BaseTypeFloat):
    pass


class TestTypeInt(BaseTypeInt):
    pass


class TestTypeNumeric(BaseTypeNumeric):
    def numeric_fixture_type(self):
        return "decimal(28,6)"


class TestTypeString(BaseTypeString):
    pass


# TODO: Re-enable when https://github.com/trinodb/trino/pull/13981 is merged
@pytest.mark.skip_profile("starburst_galaxy")
class TestTypeTimestamp(BaseTypeTimestamp):
    pass


class TestTypeBoolean(BaseTypeBoolean):
    pass
