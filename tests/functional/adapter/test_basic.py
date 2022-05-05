import pytest

from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import BaseSingularTestsEphemeral
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp


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
