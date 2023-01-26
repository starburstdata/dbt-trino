import pytest
from dbt.tests.adapter.incremental.test_incremental_predicates import (
    BaseIncrementalPredicates,
)


@pytest.mark.iceberg
class TestIcebergPredicatesDeleteInsertTrino(BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+predicates": ["id != 2"], "+incremental_strategy": "delete+insert"}}


@pytest.mark.delta
class TestDeltaPredicatesDeleteInsertTrino(BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+predicates": ["id != 2"], "+incremental_strategy": "delete+insert"}}


@pytest.mark.iceberg
class TestIcebergIncrementalPredicatesMergeTrino(BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+incremental_predicates": ["dbt_internal_dest.id != 2"],
                "+incremental_strategy": "merge",
            }
        }


@pytest.mark.delta
class TestDeltaIncrementalPredicatesMergeTrino(BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+incremental_predicates": ["dbt_internal_dest.id != 2"],
                "+incremental_strategy": "merge",
            }
        }


@pytest.mark.iceberg
class TestIcebergPredicatesMergeTrino(BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+predicates": ["dbt_internal_dest.id != 2"],
                "+incremental_strategy": "merge",
            }
        }


@pytest.mark.delta
class TestDeltaPredicatesMergeTrino(BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+predicates": ["dbt_internal_dest.id != 2"],
                "+incremental_strategy": "merge",
            }
        }
