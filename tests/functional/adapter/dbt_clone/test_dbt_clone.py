import pytest
from dbt.tests.adapter.dbt_clone.fixtures import (
    clone_model_with_grants_schema_yml,
    custom_can_clone_tables_false_macros_sql,
    get_schema_name_sql,
    infinite_macros_sql,
    macros_sql,
    table_model_with_grants_sql,
)
from dbt.tests.adapter.dbt_clone.test_dbt_clone import BaseClone, BaseCloneNotPossible
from dbt.tests.adapter.grants.base_grants import BaseGrants
from dbt.tests.util import run_dbt

iceberg_macro_override_sql = """
{% macro trino__current_timestamp() -%}
    current_timestamp(6)
{%- endmacro %}
"""


class TestTrinoCloneNotPossible(BaseCloneNotPossible):
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "macros.sql": macros_sql,
            "my_can_clone_tables.sql": custom_can_clone_tables_false_macros_sql,
            "infinite_macros.sql": infinite_macros_sql,
            "get_schema_name.sql": get_schema_name_sql,
            "iceberg.sql": iceberg_macro_override_sql,
        }

    # TODO: below method probably should be implemented in base class (on dbt-core side)
    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=f"{project.test_schema}_seeds"
            )
            project.adapter.drop_schema(relation)

            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)


@pytest.mark.hive
@pytest.mark.skip_profile("starburst_galaxy")
class TestTrinoClonePreservesGrants(BaseGrants, BaseClone):
    """Trino can't clone tables, so `dbt clone` creates a view; its grants must still apply."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": table_model_with_grants_sql,
            "schema.yml": self.interpolate_name_overrides(clone_model_with_grants_schema_yml),
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {}

    @pytest.fixture(autouse=True)
    def clean_up(self, project, other_schema):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=other_schema
            )
            project.adapter.drop_schema(relation)

    def test_clone_preserves_grants(self, project, other_schema, get_test_users):
        if len(get_test_users) == 0:
            pytest.skip("DBT_TEST_USER_1 environment variable not set")

        project.create_test_schema(other_schema)
        select_privilege = self.privilege_grantee_name_overrides()["select"]

        results = run_dbt(["run"])
        assert len(results) == 1
        source_grants = self.get_grants_on_relation(project, f"{project.test_schema}.my_model")
        assert get_test_users[0] in source_grants[select_privilege]

        self.copy_state(project.project_root)
        clone_args = ["clone", "--state", "state", "--target", "otherschema", "--full-refresh"]
        results = run_dbt(clone_args)
        assert len(results) == 1

        cloned_grants = self.get_grants_on_relation(project, f"{other_schema}.my_model")
        assert get_test_users[0] in cloned_grants[select_privilege]
