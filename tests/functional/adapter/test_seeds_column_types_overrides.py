import pytest
from dbt.tests.util import get_connection, relation_from_name, run_dbt

boolean_type = """
boolean_example
true
""".lstrip()

datetime_type = """
date_example,time_example,time_p_example,time_tz_example,timestamp_example,timestamp_p_example,timestamp_tz_example,timestamp_p_tz_example,interval_ym_example,interval_ds_example
2018-01-05,01:02:03.456,01:02:03.456789,01:02:03.456 -08:00,2020-06-10 15:55:23.383,2020-06-10 15:55:23.383345,2001-08-22 03:04:05.321-08:00,2001-08-22 03:04:05.321456-08:00,'3' MONTH,'2' DAY
,,,,,,,,,
""".lstrip()

number_type = """
integer_example,tinyint_example,smallint_example,bigint_example,real_example,double_example,decimal_example,decimal_p_example
1,2,3,4,10.3e0,10.3e0,1.1,1.23
,,,,,,
""".lstrip()

string_type = """varchar_example,varchar_n_example,char_example,char_n_example,varbinary_example,json_example
test,abc,d,ghi,65683F,"{""k1"":1,""k2"":23,""k3"":456}"
,,,,,
""".lstrip()

seed_types = {
    "boolean_type": {
        "boolean_example": "boolean",
    },
    "datetime_type": {
        "date_example": "date",
        "time_example": "time",
        "time_p_example": "time(6)",
        "time_tz_example": "time with time zone",
        "timestamp_example": "timestamp",
        "timestamp_p_example": "timestamp(6)",
        "timestamp_tz_example": "timestamp with time zone",
        "timestamp_p_tz_example": "timestamp(6) with time zone",
        "interval_ym_example": "interval year to month",
        "interval_ds_example": "interval day to second",
    },
    "number_type": {
        "integer_example": "integer",
        "tinyint_example": "tinyint",
        "smallint_example": "smallint",
        "bigint_example": "bigint",
        "real_example": "real",
        "double_example": "double",
        "decimal_example": "decimal",
        "decimal_p_example": "decimal(3,2)",
    },
    "string_type": {
        "varchar_example": "varchar",
        "varchar_n_example": "varchar(10)",
        "char_example": "char",
        "char_n_example": "char(10)",
        "varbinary_example": "varbinary",
        "json_example": "json",
    },
}


# function copied from dbt.tests.util. Original function doesn't return numeric_precision and numeric_scale.
def get_relation_columns(adapter, name):
    relation = relation_from_name(adapter, name)
    with get_connection(adapter):
        columns = adapter.get_columns_in_relation(relation)
        return sorted(
            (
                (c.name, c.dtype, c.char_size, c.numeric_precision, c.numeric_scale)
                for c in columns
            ),
            key=lambda x: x[0],
        )


@pytest.mark.skip_profile("starburst_galaxy")
class TestSeedsColumnTypesOverrides:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "test": {
                    "boolean_type": {"+column_types": seed_types["boolean_type"]},
                    "datetime_type": {"+column_types": seed_types["datetime_type"]},
                    "number_type": {"+column_types": seed_types["number_type"]},
                    "string_type": {"+column_types": seed_types["string_type"]},
                }
            }
        }

    # everything that goes in the "seeds" directory
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "boolean_type.csv": boolean_type,
            "datetime_type.csv": datetime_type,
            "number_type.csv": number_type,
            "string_type.csv": string_type,
        }

    def test_seeds_column_overrides(self, project):
        # seed seeds
        results = run_dbt(["seed"], expect_pass=True)
        assert len(results) == 4

        actual_columns = {}
        for seed_name, seed_columns in seed_types.items():
            # retrieve information about columns from trino
            actual_columns[seed_name] = get_relation_columns(project.adapter, seed_name)

        assert actual_columns == {
            "boolean_type": [
                ("boolean_example", "boolean", None, None, None),
            ],
            "datetime_type": [
                ("date_example", "date", None, None, None),
                ("interval_ds_example", "interval day to second", None, None, None),
                ("interval_ym_example", "interval year to month", None, None, None),
                ("time_example", "time(3)", None, None, None),
                ("time_p_example", "time(6)", None, None, None),
                ("time_tz_example", "time(3) with time zone", None, None, None),
                ("timestamp_example", "timestamp(3)", None, None, None),
                ("timestamp_p_example", "timestamp(6)", None, None, None),
                ("timestamp_p_tz_example", "timestamp(6) with time zone", None, None, None),
                ("timestamp_tz_example", "timestamp(3) with time zone", None, None, None),
            ],
            "number_type": [
                ("bigint_example", "bigint", None, None, None),
                ("decimal_example", "decimal", None, 38, 0),
                ("decimal_p_example", "decimal", None, 3, 2),
                ("double_example", "double", None, None, None),
                ("integer_example", "integer", None, None, None),
                ("real_example", "real", None, None, None),
                ("smallint_example", "smallint", None, None, None),
                ("tinyint_example", "tinyint", None, None, None),
            ],
            "string_type": [
                ("char_example", "char", 1, None, None),
                ("char_n_example", "char", 10, None, None),
                ("json_example", "json", None, None, None),
                ("varbinary_example", "varbinary", None, None, None),
                ("varchar_example", "varchar", None, None, None),
                ("varchar_n_example", "varchar", 10, None, None),
            ],
        }
