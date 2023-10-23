from pathlib import Path

import pytest
from dbt.tests.adapter.simple_seed.test_seed import (
    TestBasicSeedTests as CoreTestBasicSeedTests,
)
from dbt.tests.adapter.simple_seed.test_seed import (
    TestSeedConfigFullRefreshOff as CoreTestSeedConfigFullRefreshOff,
)
from dbt.tests.adapter.simple_seed.test_seed import (
    TestSeedConfigFullRefreshOn as CoreTestSeedConfigFullRefreshOn,
)
from dbt.tests.adapter.simple_seed.test_seed import (
    TestSeedCustomSchema as CoreTestSeedCustomSchema,
)
from dbt.tests.adapter.simple_seed.test_seed import (
    TestSeedParsing as CoreTestSeedParsing,
)
from dbt.tests.adapter.simple_seed.test_seed import (
    TestSeedSpecificFormats as CoreTestSeedSpecificFormats,
)
from dbt.tests.adapter.simple_seed.test_seed import (
    TestSeedWithEmptyDelimiter as CoreTestSeedWithEmptyDelimiter,
)
from dbt.tests.adapter.simple_seed.test_seed import (
    TestSeedWithUniqueDelimiter as CoreTestSeedWithUniqueDelimiter,
)
from dbt.tests.adapter.simple_seed.test_seed import (
    TestSeedWithWrongDelimiter as CoreTestSeedWithWrongDelimiter,
)
from dbt.tests.adapter.simple_seed.test_seed import (
    TestSimpleSeedEnabledViaConfig as CoreTestSimpleSeedEnabledViaConfig,
)
from dbt.tests.adapter.simple_seed.test_seed import (
    TestSimpleSeedWithBOM as CoreTestSimpleSeedWithBOM,
)
from dbt.tests.util import copy_file, run_dbt

from tests.functional.adapter.simple_seed.seeds import (
    trino_seeds__expected_sql_create_table,
    trino_seeds__expected_sql_insert_into,
)


class TrinoSetUpFixture:
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        """Create table for ensuring seeds and models used in tests build correctly"""
        project.run_sql(trino_seeds__expected_sql_create_table)
        project.run_sql(trino_seeds__expected_sql_insert_into)


class TestTrinoBasicSeedTests(TrinoSetUpFixture, CoreTestBasicSeedTests):
    # TODO Trino currently does not support DROP TABLE CASCADE.
    #  Dropping seed won't drop downstream models automatically.
    @pytest.mark.skip
    def test_simple_seed_full_refresh_flag(self, project):
        pass


# TODO Trino currently does not support DROP TABLE CASCADE.
#  Dropping seed won't drop downstream models automatically.
@pytest.mark.skip
class TestTrinoSeedConfigFullRefreshOn(TrinoSetUpFixture, CoreTestSeedConfigFullRefreshOn):
    pass


class TestTrinoSeedConfigFullRefreshOff(TrinoSetUpFixture, CoreTestSeedConfigFullRefreshOff):
    pass


class TestTrinoSeedCustomSchema(TrinoSetUpFixture, CoreTestSeedCustomSchema):
    pass


class TestTrinoSeedWithUniqueDelimiter(TrinoSetUpFixture, CoreTestSeedWithUniqueDelimiter):
    pass


class TestTrinoSeedWithWrongDelimiter(TrinoSetUpFixture, CoreTestSeedWithWrongDelimiter):
    def test_seed_with_wrong_delimiter(self, project):
        """Testing failure of running dbt seed with a wrongly configured delimiter"""
        seed_result = run_dbt(["seed"], expect_pass=False)
        assert "syntax_error" in seed_result.results[0].message.lower()


class TestTrinoSeedWithEmptyDelimiter(TrinoSetUpFixture, CoreTestSeedWithEmptyDelimiter):
    pass


class TestTrinoSimpleSeedEnabledViaConfig(CoreTestSimpleSeedEnabledViaConfig):
    pass


class TestTrinoSeedParsing(TrinoSetUpFixture, CoreTestSeedParsing):
    pass


class TestTrinoSimpleSeedWithBOM(CoreTestSimpleSeedWithBOM):
    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        """Create table for ensuring seeds and models used in tests build correctly"""
        project.run_sql(trino_seeds__expected_sql_create_table)
        project.run_sql(trino_seeds__expected_sql_insert_into)
        copy_file(
            project.test_dir,
            "seed_bom.csv",
            project.project_root / Path("seeds") / "seed_bom.csv",
            "",
        )


class TestTrinoSeedSpecificFormats(CoreTestSeedSpecificFormats):
    pass
