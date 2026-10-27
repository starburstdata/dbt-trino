from dbt.tests.adapter.empty.test_empty import (
    BaseTestEmpty,
    BaseTestEmptyInlineSourceRef,
    BaseTestEmptySeedFlag,
)
from dbt.tests.util import run_dbt


class TestTrinoEmpty(BaseTestEmpty):
    pass


class TestTrinoEmptyInlineSourceRef(BaseTestEmptyInlineSourceRef):
    pass


class TestTrinoEmptySeedFlag(BaseTestEmptySeedFlag):
    def test_seed_empty_full_refresh_replaces_rows(self, project):
        run_dbt(["seed"])
        self.assert_row_count(project, "raw_seed", 2)

        run_dbt(["seed", "--empty", "--full-refresh"])
        self.assert_row_count(project, "raw_seed", 0)
