import pytest
from dbt.tests.adapter.basic.expected_catalog import base_expected_catalog, no_stats
from dbt.tests.adapter.basic.files import generic_test_seed_yml
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_docs_generate import BaseDocsGenerate
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_incremental import (
    BaseIncremental,
    BaseIncrementalNotSchemaChange,
)
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral,
)
from dbt.tests.adapter.basic.test_validate_connection import BaseValidateConnection
from dbt.tests.util import run_dbt

seeds_base_csv = """
id,name,some_date
1,Easton,1981-05-20 06:46:51
2,Lillian,1978-09-03 18:10:33
3,Jeremiah,1982-03-11 03:59:51
4,Nolan,1976-05-06 20:21:35
5,Hannah,1982-06-23 05:41:26
6,Eleanor,1991-08-10 23:12:21
7,Lily,1971-03-29 14:58:02
8,Jonathan,1988-02-26 02:55:24
9,Adrian,1994-02-09 13:14:23
10,Nora,1976-03-01 16:51:39
""".lstrip()


seeds_added_csv = (
    seeds_base_csv
    + """
11,Mateo,2014-09-07 17:04:27
12,Julian,2000-02-04 11:48:30
13,Gabriel,2001-07-10 07:32:52
14,Isaac,2002-11-24 03:22:28
15,Levi,2009-11-15 11:57:15
16,Elizabeth,2005-04-09 03:50:11
17,Grayson,2019-08-06 19:28:17
18,Dylan,2014-03-01 11:50:41
19,Jayden,2009-06-06 07:12:49
20,Luke,2003-12-05 21:42:18
""".lstrip()
)


seed__schema_yml = """
version: 2
seeds:
  - name: seed
    description: "The test seed"
    columns:
      - name: id
        description: The user ID number
      - name: first_name
        description: The user's first name
      - name: email
        description: The user's email
      - name: ip_address
        description: The user's IP address
      - name: updated_at
        description: The last time this user's email was updated
"""

seed__seed_csv = """id,first_name,email,ip_address,updated_at
1,Larry,lking0@miitbeian.gov.cn,69.135.206.194,2008-09-12 19:08:31
"""

incremental_not_schema_change_sql = """
{{ config(materialized="incremental", unique_key="user_id_current_time",on_schema_change="sync_all_columns") }}
select
    '1' || '-' || cast(current_timestamp as varchar) as user_id_current_time,
    {% if is_incremental() %}
        'thisis18characters' as platform
    {% else %}
        'okthisis20characters' as platform
    {% endif %}
"""


class TestAdapterMethods(BaseAdapterMethod):
    pass


# TODO Internal Galaxy issue: type=INTERNAL_ERROR, name=GENERIC_INTERNAL_ERROR,
# message="Unexpected response status (Internal Server Error) performing operation: entity created
@pytest.mark.skip_profile("starburst_galaxy")
class TestSimpleMaterializationsTrino(BaseSimpleMaterializations):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "base",
            "seeds": {
                "+column_types": {"some_date": "timestamp(6)"},
            },
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
        }


class TestSingularTestsTrino(BaseSingularTests):
    pass


class TestSingularTestsEphemeralTrino(BaseSingularTestsEphemeral):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "singular_tests_ephemeral",
            "seeds": {
                "+column_types": {"some_date": "timestamp(6)"},
            },
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
        }


class TestEmptyTrino(BaseEmpty):
    pass


class TestEphemeralTrino(BaseEphemeral):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "ephemeral",
            "seeds": {
                "+column_types": {"some_date": "timestamp(6)"},
            },
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
        }


class TestIncrementalTrino(BaseIncremental):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "incremental",
            "seeds": {
                "+column_types": {"some_date": "timestamp(6)"},
            },
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"base.csv": seeds_base_csv, "added.csv": seeds_added_csv}


class TestIncrementalFullRefreshTrino(TestIncrementalTrino):
    def test_incremental(self, project):
        super().test_incremental(project)
        results = run_dbt(["run", "--vars", "seed_name: base", "--full-refresh"])
        assert len(results) == 1


class TestIncrementalNotSchemaChangeTrino(BaseIncrementalNotSchemaChange):
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_not_schema_change.sql": incremental_not_schema_change_sql}


class TestGenericTestsTrino(BaseGenericTests):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "generic_tests",
            "seeds": {
                "+column_types": {"some_date": "timestamp(6)"},
            },
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"base.csv": seeds_base_csv, "schema.yml": generic_test_seed_yml}


class TestTrinoValidateConnection(BaseValidateConnection):
    pass


class TestDocsGenerateTrino(BaseDocsGenerate):
    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        alternate_schema = unique_schema + "_test"
        return {
            "asset-paths": ["assets", "invalid-asset-paths"],
            "vars": {
                "test_schema": unique_schema,
                "alternate_schema": alternate_schema,
            },
            "seeds": {
                "quote_columns": True,
                "+column_types": {"updated_at": "timestamp(6)"},
            },
            "quoting": {"identifier": False},
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"schema.yml": seed__schema_yml, "seed.csv": seed__seed_csv}

    @pytest.fixture(scope="class")
    def expected_catalog(self, project, profile_user):
        return base_expected_catalog(
            project,
            role=None,
            id_type="integer",
            text_type="varchar",
            time_type="timestamp(6)",
            view_type="VIEW",
            table_type="BASE TABLE",
            model_stats=no_stats(),
        )
