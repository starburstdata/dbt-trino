import pytest
from dbt.tests.adapter.persist_docs.test_persist_docs import (
    BasePersistDocs,
    BasePersistDocsAllColumnsMissing,
    BasePersistDocsBase,
    BasePersistDocsColumnMissing,
    BasePersistDocsCommentOnQuotedColumn,
    BasePersistDocsQuotedColumnCaseSensitive,
    BasePersistDocsQuotedDescriptionNotAppliedOnMismatch,
)
from dbt.tests.util import run_dbt, run_dbt_and_capture, run_sql_with_adapter

from tests.functional.adapter.persist_docs.fixtures import (
    incremental_model,
    incremental_profile_yml,
    quoted_mixed_case_model,
    quoted_mixed_case_yml,
    seed_csv,
    table_model,
    table_profile_yml,
    view_model,
    view_profile_yml,
)


@pytest.mark.iceberg
class TestPersistDocsBase:
    """
    Testing persist_docs functionality
    """

    @property
    def schema(self):
        return "default"

    # everything that goes in the "seeds" directory
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": seed_csv,
        }


class TestPersistDocsTable(TestPersistDocsBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "persist_docs_tests",
            "models": {"+persist_docs": {"relation": True, "columns": True}},
            "seeds": {
                "+column_types": {"date": "timestamp(6)"},
                "+persist_docs": {"relation": True, "columns": True},
            },
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model.sql": table_model,
            "table_persist_docs.yml": table_profile_yml,
        }

    def test_run_seed_test(self, project):
        # seed seeds
        results = run_dbt(["seed"], expect_pass=True)
        assert len(results) == 1
        results = run_dbt(["run"], expect_pass=True)
        assert len(results) == 1
        # test tests
        results = run_dbt(["test"], expect_pass=True)
        assert len(results) == 4


class TestPersistDocsView(TestPersistDocsBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "persist_docs_tests",
            "models": {
                "+persist_docs": {"relation": True, "columns": True},
                "+materialized": "view",
                "+view_security": "definer",
            },
            "seeds": {
                "+column_types": {"date": "timestamp(6)"},
                "+persist_docs": {"relation": True, "columns": True},
            },
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "view_model.sql": view_model,
            "view_persist_docs.yml": view_profile_yml,
        }

    def test_run_seed_test(self, project):
        # seed seeds
        results = run_dbt(["seed"], expect_pass=True)
        assert len(results) == 1
        results = run_dbt(["run"], expect_pass=True)
        assert len(results) == 1
        # test tests
        results = run_dbt(["test"], expect_pass=True)
        assert len(results) == 4


class TestPersistDocsIncremental(TestPersistDocsBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "persist_docs_tests",
            "models": {"+persist_docs": {"relation": True, "columns": True}},
            "seeds": {
                "+column_types": {"date": "timestamp(6)"},
                "+persist_docs": {"relation": True, "columns": True},
            },
        }

    # everything that goes in the "models" directory
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_model.sql": incremental_model,
            "incremental_persist_docs.yml": incremental_profile_yml,
        }

    def test_run_seed_test(self, project):
        # seed seeds
        results = run_dbt(["seed"], expect_pass=True)
        assert len(results) == 1
        results = run_dbt(["run"], expect_pass=True)
        assert len(results) == 1
        # test tests
        results = run_dbt(["test"], expect_pass=True)
        assert len(results) == 4


# TODO: Trying to COMMENT ON COLUMN with a comment that includes '*/'
# results in an error. This happens only for views.
@pytest.mark.skip_profile("starburst_galaxy")
class TestPersistDocs(BasePersistDocs):
    pass


class TestPersistDocsColumnMissing(BasePersistDocsColumnMissing):
    pass


class TestPersistDocsAllColumnsMissing(BasePersistDocsAllColumnsMissing):
    pass


# Trino lowercases quoted identifiers too, so a quoted "MyCol" and mycol are the same
# column and the two upstream case-sensitivity tests below don't apply.
class TestPersistDocsQuotedColumnCaseSensitive(BasePersistDocsQuotedColumnCaseSensitive):
    @pytest.mark.skip(reason="Trino identifiers are case-insensitive, even when quoted")
    def test_quoted_column_case_sensitive_warning(self, project):
        pass


class TestPersistDocsQuotedDescriptionNotAppliedOnMismatch(
    BasePersistDocsQuotedDescriptionNotAppliedOnMismatch
):
    @pytest.mark.skip(reason="Trino identifiers are case-insensitive, even when quoted")
    def test_quoted_description_not_applied_on_case_mismatch(self, project):
        pass


class TestPersistDocsQuotedMixedCaseColumn(BasePersistDocsBase):
    """A quoted mixed-case column still gets its comment, since Trino stores it lowercased."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"test": {"+persist_docs": {"columns": True}}}}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {}

    @pytest.fixture(scope="class")
    def models(self):
        return {"quoted_mixed_case.sql": quoted_mixed_case_model}

    @pytest.fixture(scope="class")
    def properties(self):
        return {"schema.yml": quoted_mixed_case_yml}

    @pytest.fixture(scope="class", autouse=True)
    def setUp(self, project):
        pass

    def test_quoted_mixed_case_column_comment_applied(self, project):
        _, logs = run_dbt_and_capture(["run"])
        assert "are not present in the database" not in logs

        result = run_sql_with_adapter(
            project.adapter,
            f"""
            select comment from {project.database}.information_schema.columns
            where table_schema = '{project.test_schema}'
              and table_name = 'quoted_mixed_case'
              and column_name = 'mycol'
            """,
            fetch="one",
        )
        assert result[0] == "quoted mixed-case column description"


class TestPersistDocsCommentOnQuotedColumn(BasePersistDocsCommentOnQuotedColumn):
    pass


class BasePersistDocsDisabled(BasePersistDocsBase):
    def test_persist_docs_disabled(self, project):
        sql = f"""select * from system.metadata.table_comments
        where catalog_name = '{project.database}'
        and schema_name = '{project.test_schema}'
        and table_name = 'table_model'
        and comment is not null
        """
        result = run_sql_with_adapter(project.adapter, sql, fetch="all")
        assert len(result) == 0


class TestPersistDocsDisabledByDefault(BasePersistDocsDisabled):
    """
    Without providing `persist_docs` config, table comments shouldn't be added by default.
    """

    pass


# TODO: Trying to COMMENT ON COLUMN with a comment that includes '*/'
# results in an error. This happens only for views.
@pytest.mark.skip_profile("starburst_galaxy")
class TestPersistDocsRelationSetToFalse(BasePersistDocsDisabled):
    """
    With `persist_docs.relation` config set to False, table comments shouldn't be added.
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+persist_docs": {
                        "relation": False,
                        "columns": True,
                    },
                }
            }
        }


# TODO: Trying to COMMENT ON COLUMN with a comment that includes '*/'
# results in an error. This happens only for views.
@pytest.mark.skip_profile("starburst_galaxy")
class TestPersistDocsRelationNotSet(BasePersistDocsDisabled):
    """
    Without providing `persist_docs.relation` config, table comments shouldn't be added by default.
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+persist_docs": {
                        "columns": True,
                    },
                }
            }
        }
