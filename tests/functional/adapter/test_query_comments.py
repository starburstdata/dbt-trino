from dbt.tests.adapter.query_comment.test_query_comment import (
    BaseEmptyQueryComments,
    BaseMacroArgsQueryComments,
    BaseMacroInvalidQueryComments,
    BaseMacroQueryComments,
    BaseNullQueryComments,
    BaseQueryComments,
)


class TestQueryCommentsTrino(BaseQueryComments):
    pass


class TestMacroQueryCommentsTrino(BaseMacroQueryComments):
    pass


class TestMacroArgsQueryCommentsTrino(BaseMacroArgsQueryComments):
    pass


class TestMacroInvalidQueryCommentsTrino(BaseMacroInvalidQueryComments):
    pass


class TestNullQueryCommentsTrino(BaseNullQueryComments):
    pass


class TestEmptyQueryCommentsTrino(BaseEmptyQueryComments):
    pass
