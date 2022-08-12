import pytest
from dbt.tests.adapter.basic.expected_catalog import base_expected_catalog, no_stats
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_docs_generate import BaseDocsGenerate
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral,
)
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_validate_connection import BaseValidateConnection


class TestAdapterMethods(BaseAdapterMethod):
    pass


class TestSimpleMaterializationsTrino(BaseSimpleMaterializations):
    pass


class TestSingularTestsTrino(BaseSingularTests):
    pass


class TestSingularTestsEphemeralTrino(BaseSingularTestsEphemeral):
    pass


class TestEmptyTrino(BaseEmpty):
    pass


class TestEphemeralTrino(BaseEphemeral):
    pass


class TestIncrementalTrino(BaseIncremental):
    pass


class TestGenericTestsTrino(BaseGenericTests):
    pass


@pytest.mark.xfail(reason="Snapshot not supported in dbt-trino")
class TestSnapshotCheckColsTrino(BaseSnapshotCheckCols):
    pass


@pytest.mark.xfail(reason="Snapshot not supported in dbt-trino")
class TestSnapshotTimestampTrino(BaseSnapshotTimestamp):
    pass


class TestTrinoValidateConnection(BaseValidateConnection):
    pass


class TestDocsGenerateTrino(BaseDocsGenerate):
    @pytest.fixture(scope="class")
    def expected_catalog(self, project, profile_user):
        return base_expected_catalog(
            project,
            role=None,
            id_type="integer",
            text_type="varchar",
            time_type="timestamp(3)",
            view_type="VIEW",
            table_type="BASE TABLE",
            model_stats=no_stats(),
        )
