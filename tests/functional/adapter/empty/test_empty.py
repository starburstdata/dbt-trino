from dbt.tests.adapter.empty.test_empty import (
    BaseTestEmpty,
    BaseTestEmptyInlineSourceRef,
)


class TestTrinoEmpty(BaseTestEmpty):
    pass


class TestTrinoEmptyInlineSourceRef(BaseTestEmptyInlineSourceRef):
    pass
