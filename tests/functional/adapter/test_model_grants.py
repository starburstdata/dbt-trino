import pytest
from dbt.context.base import BaseContext  # diff_of_two_dicts only
from dbt.tests.adapter.grants.test_model_grants import BaseModelGrants


@pytest.mark.hive
# TODO: setup Galaxy and Starbust tests
#   See https://github.com/starburstdata/dbt-trino/issues/147
#   and also https://github.com/starburstdata/dbt-trino/issues/146
@pytest.mark.skip_profile("starburst_galaxy")
# To run this test locally add following env vars:
# DBT_TEST_USER_1=user1
# DBT_TEST_USER_2=user2
# DBT_TEST_USER_3=user3
class TestModelGrants(BaseModelGrants):
    def assert_expected_grants_match_actual(self, project, relation_name, expected_grants):
        actual_grants = self.get_grants_on_relation(project, relation_name)
        # Remove the creation user
        try:
            for privilege in ["delete", "update", "insert", "select"]:
                if privilege in actual_grants:
                    actual_grants[privilege].remove("admin")
                    if len(actual_grants[privilege]) == 0:
                        del actual_grants[privilege]
        except ValueError:
            pass

        # need a case-insensitive comparison
        # so just a simple "assert expected == actual_grants" won't work
        diff_a = BaseContext.diff_of_two_dicts(actual_grants, expected_grants)
        diff_b = BaseContext.diff_of_two_dicts(expected_grants, actual_grants)
        assert diff_a == diff_b == {}
