import pytest
from dbt.tests.adapter.utils.fixture_datediff import models__test_datediff_yml
from dbt.tests.adapter.utils.test_any_value import BaseAnyValue
from dbt.tests.adapter.utils.test_array_append import BaseArrayAppend
from dbt.tests.adapter.utils.test_array_concat import BaseArrayConcat
from dbt.tests.adapter.utils.test_array_construct import BaseArrayConstruct
from dbt.tests.adapter.utils.test_bool_or import BaseBoolOr
from dbt.tests.adapter.utils.test_cast_bool_to_text import BaseCastBoolToText
from dbt.tests.adapter.utils.test_concat import BaseConcat
from dbt.tests.adapter.utils.test_current_timestamp import BaseCurrentTimestampAware
from dbt.tests.adapter.utils.test_date_trunc import BaseDateTrunc
from dbt.tests.adapter.utils.test_dateadd import BaseDateAdd
from dbt.tests.adapter.utils.test_datediff import BaseDateDiff
from dbt.tests.adapter.utils.test_equals import BaseEquals
from dbt.tests.adapter.utils.test_escape_single_quotes import (
    BaseEscapeSingleQuotesQuote,
)
from dbt.tests.adapter.utils.test_except import BaseExcept
from dbt.tests.adapter.utils.test_hash import BaseHash
from dbt.tests.adapter.utils.test_intersect import BaseIntersect
from dbt.tests.adapter.utils.test_last_day import BaseLastDay
from dbt.tests.adapter.utils.test_length import BaseLength
from dbt.tests.adapter.utils.test_listagg import BaseListagg
from dbt.tests.adapter.utils.test_position import BasePosition
from dbt.tests.adapter.utils.test_replace import BaseReplace
from dbt.tests.adapter.utils.test_right import BaseRight
from dbt.tests.adapter.utils.test_safe_cast import BaseSafeCast
from dbt.tests.adapter.utils.test_split_part import BaseSplitPart
from dbt.tests.adapter.utils.test_string_literal import BaseStringLiteral
from dbt.tests.adapter.utils.test_validate_sql import BaseValidateSqlMethod

from tests.functional.adapter.fixture_datediff import (
    models__test_datediff_sql,
    seeds__data_datediff_csv,
)

models__array_append_expected_sql = """
select 1 as id, {{ array_construct([1,2,3,4]) }} as array_col
"""


models__array_append_actual_sql = """
select 1 as id, {{ array_append(array_construct([1,2,3]), 4) }} as array_col
"""

models__array_concat_expected_sql = """
select 1 as id, {{ array_construct([1,2,3,4,5,6]) }} as array_col
"""


models__array_concat_actual_sql = """
select 1 as id, {{ array_concat(array_construct([1,2,3]), array_construct([4,5,6])) }} as array_col
"""


class TestAnyValue(BaseAnyValue):
    pass


# Only partially because of https://github.com/trinodb/trino/issues/13
# No way to concat an array with null or empty array
class TestArrayAppend(BaseArrayAppend):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "actual.sql": models__array_append_actual_sql,
            "expected.sql": models__array_append_expected_sql,
        }


# Only partially because of https://github.com/trinodb/trino/issues/13
# No way to concat an array with null or empty array
class TestArrayConcat(BaseArrayConcat):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "actual.sql": models__array_concat_actual_sql,
            "expected.sql": models__array_concat_expected_sql,
        }


class TestArrayConstruct(BaseArrayConstruct):
    pass


class TestBoolOr(BaseBoolOr):
    pass


class TestCastBoolToText(BaseCastBoolToText):
    pass


class TestConcat(BaseConcat):
    pass


class TestCurrentTimestamp(BaseCurrentTimestampAware):
    pass


class TestDateAdd(BaseDateAdd):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test_date_add",
            "seeds": {
                "+column_types": {
                    "from_time": "timestamp(6)",
                    "result": "timestamp(6)",
                },
            },
        }


class TestDateDiff(BaseDateDiff):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test_date_diff",
            "seeds": {
                "+column_types": {"first_date": "timestamp(6)", "second_date": "timestamp(6)"},
            },
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_datediff.csv": seeds__data_datediff_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_datediff.yml": models__test_datediff_yml,
            "test_datediff.sql": self.interpolate_macro_namespace(
                models__test_datediff_sql, "datediff"
            ),
        }


class TestDateTrunc(BaseDateTrunc):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test_date_diff",
            "seeds": {
                "+column_types": {"updated_at": "timestamp(6)"},
            },
        }


class TestEquals(BaseEquals):
    pass


class TestEscapeSingleQuotes(BaseEscapeSingleQuotesQuote):
    pass


class TestExcept(BaseExcept):
    pass


class TestHash(BaseHash):
    pass


class TestIntersect(BaseIntersect):
    pass


class TestLastDay(BaseLastDay):
    pass


class TestLength(BaseLength):
    pass


class TestListagg(BaseListagg):
    pass


class TestPosition(BasePosition):
    pass


class TestReplace(BaseReplace):
    pass


class TestRight(BaseRight):
    pass


class TestSafeCast(BaseSafeCast):
    pass


class TestSplitPart(BaseSplitPart):
    pass


class TestStringLiteral(BaseStringLiteral):
    pass


class TestValidateSqlMethod(BaseValidateSqlMethod):
    pass
