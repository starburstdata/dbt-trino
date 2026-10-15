import pytest
from dbt.adapters.events.types import SQLQuery
from dbt.artifacts.schemas.results import RunStatus
from dbt.events.types import JinjaLogWarning, RunResultError
from dbt.tests.adapter.functions import test_udfs as base
from dbt.tests.util import run_dbt, write_file
from dbt_common.events.event_catcher import EventCatcher
from dbt_common.events.event_manager_client import get_event_manager

from tests.functional.adapter.functions import fixtures

# Trino only stores catalog functions in the Hive and Memory connectors, so on Trino and Starburst
# Enterprise these tests run in the default `memory` catalog (or `hive` where marked). Starburst
# Galaxy only stores them in the built-in `galaxy.functions` schema, which is shared by the whole
# account: there the functions are routed to it and removed again after every test class.
skip_galaxy = pytest.mark.skip_profile("starburst_galaxy")

GALAXY_PROFILE = "starburst_galaxy"
GALAXY_FUNCTIONS = ("galaxy", "functions")

# Every function name the tests below create. On Galaxy they are the only functions the tests
# may look at or remove, because other users' functions live in the same schema.
UDF_NAMES = ("price_for_xlarge", "describe_amount", "meaning_of_life")


def on_galaxy(request):
    return request.config.getoption("--profile") == GALAXY_PROFILE


def show_functions(project, database, schema):
    """(name, argument types, description) of every function in a schema."""
    relation = project.adapter.Relation.create(database=database, schema=schema)
    with project.adapter.connection_named("_test_show_functions"):
        _, table = project.adapter.execute(f"show functions from {relation}", fetch=True)
    return [(row[0], row[2], row[5]) for row in table.rows]


def drop_functions(project, database, schema):
    for name, argument_types, _ in show_functions(project, database, schema):
        if name not in UDF_NAMES:
            continue
        with project.adapter.connection_named("_test_drop_functions"):
            project.adapter.execute(
                f"drop function if exists {database}.{schema}.{name}({argument_types})"
            )


def expect_build_error():
    catcher = EventCatcher(RunResultError)
    result = run_dbt(["build", "--debug"], expect_pass=False, callbacks=[catcher.catch])
    assert len(result.results) == 1
    assert result.results[0].status == RunStatus.Error
    assert len(catcher.caught_events) == 1
    return catcher.caught_events[0].data.msg


class UDFTest:
    """Where the functions are stored, and the cleanup that goes with it, for every profile."""

    # Settings applied to every function, such as {"+volatility": "deterministic"}
    function_config: dict = {}
    # Set to False to leave the functions in the target's catalog, even on Galaxy
    store_in_galaxy_functions = True

    @pytest.fixture(scope="class")
    def project_config_update(self, request):
        config = dict(self.function_config)
        if on_galaxy(request) and self.store_in_galaxy_functions:
            config.update({"+database": GALAXY_FUNCTIONS[0], "+schema": GALAXY_FUNCTIONS[1]})
        return {"functions": config}

    @pytest.fixture(scope="class")
    def function_location(self, project, request):
        if on_galaxy(request) and self.store_in_galaxy_functions:
            return GALAXY_FUNCTIONS
        return project.database, project.test_schema

    @pytest.fixture(scope="class", autouse=True)
    def clean_up_galaxy_functions(self, project, request):
        if not (on_galaxy(request) and self.store_in_galaxy_functions):
            yield
            return
        drop_functions(project, *GALAXY_FUNCTIONS)
        yield
        drop_functions(project, *GALAXY_FUNCTIONS)


class TrinoUDFs(UDFTest):
    """Replaces the upstream fixtures, which use `float` and a `SELECT` body that Trino rejects."""

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": fixtures.MY_UDF_SQL,
            "price_for_xlarge.yml": fixtures.MY_UDF_YML,
        }


class TestUDFsBasic(TrinoUDFs, base.UDFsBasic):
    def check_function_volatility(self, sql: str):
        assert "DETERMINISTIC" not in sql


class TestDeterministicUDF(TrinoUDFs, base.DeterministicUDF):
    function_config = {"+volatility": "deterministic"}

    def check_function_volatility(self, sql: str):
        assert "NOT DETERMINISTIC" not in sql
        assert "DETERMINISTIC" in sql


class TestNonDeterministicUDF(TrinoUDFs, base.NonDeterministicUDF):
    function_config = {"+volatility": "non-deterministic"}

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": fixtures.NON_DETERMINISTIC_UDF_SQL,
            "price_for_xlarge.yml": fixtures.MY_UDF_YML,
        }

    def check_function_volatility(self, sql: str):
        assert "NOT DETERMINISTIC" in sql


class TestVolatilityMustMatchBody(TrinoUDFs, base.NonDeterministicUDF):
    """Trino validates the declared volatility against the body when the function is created."""

    function_config = {"+volatility": "non-deterministic"}

    def test_udfs(self, project, sql_event_catcher):
        msg = expect_build_error()
        assert "Deterministic function declared NOT DETERMINISTIC" in msg


class TestNonDeterministicBodyRequiresVolatility(TrinoUDFs):
    """Without a volatility, Trino assumes DETERMINISTIC, so a body calling random() is rejected."""

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": fixtures.NON_DETERMINISTIC_UDF_SQL,
            "price_for_xlarge.yml": fixtures.MY_UDF_YML,
        }

    def test_default_volatility(self, project):
        msg = expect_build_error()
        assert "Non-deterministic function declared DETERMINISTIC" in msg


class TestStableUDF(TrinoUDFs, base.StableUDF):
    function_config = {"+volatility": "stable"}

    def check_function_volatility(self, sql: str):
        assert "STABLE" not in sql
        assert "DETERMINISTIC" not in sql

    def test_udfs(self, project, sql_event_catcher):
        warning_catcher = EventCatcher(JinjaLogWarning)
        run_dbt(["build", "--debug"], callbacks=[sql_event_catcher.catch, warning_catcher.catch])
        assert len(sql_event_catcher.caught_events) == 1
        self.check_function_volatility(sql_event_catcher.caught_events[0].data.sql)
        assert any(
            "stable" in event.data.msg and "not supported by trino" in event.data.msg
            for event in warning_catcher.caught_events
        )


class TestErrorForUnsupportedType(TrinoUDFs, base.ErrorForUnsupportedType):
    function_config = {"+type": "table"}


class TestPythonUDFNotSupported(UDFTest, base.PythonUDFNotSupported):
    pass


class TestSqlUDFDefaultArgSupport(TrinoUDFs, base.SqlUDFDefaultArgSupport):
    expect_default_arg_support = False

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": fixtures.MY_UDF_SQL,
            "price_for_xlarge.yml": fixtures.MY_UDF_WITH_DEFAULT_ARG_YML,
        }


class TestUDFRebuildReplacesBody(TrinoUDFs):
    def test_rebuild(self, project):
        run_dbt(["build"])
        result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(100)"])
        assert int(result.results[0].agate_table.rows[0].values()[0]) == 200

        write_file("price * 3", project.project_root, "functions", "price_for_xlarge.sql")
        run_dbt(["build"])
        result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(100)"])
        assert int(result.results[0].agate_table.rows[0].values()[0]) == 300


class TestUDFUsedByModel(TrinoUDFs):
    @pytest.fixture(scope="class")
    def models(self):
        return {"doubled.sql": fixtures.MODEL_USING_UDF_SQL}

    def test_build_order(self, project):
        results = run_dbt(["build"])
        assert [r.node.name for r in results.results] == ["price_for_xlarge", "doubled"]
        assert all(r.status == RunStatus.Success for r in results.results)
        rows = project.run_sql("select doubled from doubled", fetch="all")
        assert float(rows[0][0]) == 42.0


class TestUDFDescriptionIsEscapedIntoComment(TrinoUDFs):
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": fixtures.MY_UDF_SQL,
            "price_for_xlarge.yml": fixtures.MY_UDF_QUOTED_DESCRIPTION_YML,
        }

    def test_comment(self, project, function_location):
        run_dbt(["build"])
        assert [f for f in show_functions(project, *function_location) if f[0] in UDF_NAMES] == [
            ("price_for_xlarge", "double", "Calculate the xlarge price for a customer's item")
        ]


class TestUDFSignatures(UDFTest):
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "describe_amount.sql": fixtures.MULTI_ARG_UDF_SQL,
            "describe_amount.yml": fixtures.MULTI_ARG_UDF_YML,
            "meaning_of_life.sql": fixtures.NO_ARG_UDF_SQL,
            "meaning_of_life.yml": fixtures.NO_ARG_UDF_YML,
        }

    def test_signatures(self, project):
        results = run_dbt(["build"])
        assert all(r.status == RunStatus.Success for r in results.results)

        result = run_dbt(
            [
                "show",
                "--inline",
                "SELECT {{ function('describe_amount') }}('a', cast(1.5 as decimal(10,2)), true), "
                "{{ function('meaning_of_life') }}()",
            ]
        )
        values = result.results[0].agate_table.rows[0].values()
        assert values[0] == "a:1.50"
        assert int(values[1]) == 42


class TestUDFBodyMustBeAnExpression(UDFTest):
    """Trino rejects queries in functions at creation time; dbt-trino adds no validation of its own."""

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": fixtures.MY_UDF_WITH_SELECT_SQL,
            "price_for_xlarge.yml": fixtures.MY_UDF_YML,
        }

    def test_select_is_rejected(self, project, function_location):
        msg = expect_build_error()
        assert "mismatched input 'SELECT'" in msg
        assert [f for f in show_functions(project, *function_location) if f[0] in UDF_NAMES] == []


class TestUDFSubqueryBodyIsRejected(TestUDFBodyMustBeAnExpression):
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": fixtures.MY_UDF_WITH_SUBQUERY_SQL,
            "price_for_xlarge.yml": fixtures.MY_UDF_YML,
        }

    def test_select_is_rejected(self, project, function_location):
        msg = expect_build_error()
        assert "Queries are not allowed in functions" in msg
        assert [f for f in show_functions(project, *function_location) if f[0] in UDF_NAMES] == []


class TestUDFFloatTypeIsNotMapped(UDFTest):
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": fixtures.MY_UDF_SQL,
            "price_for_xlarge.yml": fixtures.MY_UDF_FLOAT_YML,
        }

    def test_float_is_rejected(self, project):
        msg = expect_build_error()
        assert "Unknown type: float" in msg


class TestUDFGrantsAreIgnoredWithWarning(TrinoUDFs):
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": fixtures.MY_UDF_SQL,
            "price_for_xlarge.yml": fixtures.MY_UDF_WITH_GRANTS_YML,
        }

    def test_grants(self, project):
        warning_catcher = EventCatcher(JinjaLogWarning)
        sql_catcher = EventCatcher(SQLQuery)
        results = run_dbt(
            ["build", "--debug"], callbacks=[warning_catcher.catch, sql_catcher.catch]
        )
        assert results.results[0].status == RunStatus.Success
        assert any(
            "Grants are not supported on functions" in e.data.msg
            for e in warning_catcher.caught_events
        )
        statements = " ".join(e.data.sql.lower() for e in sql_catcher.caught_events)
        assert "comment on" not in statements
        assert "grant " not in statements


class TestUDFOverloadsAreNotReplaced(TrinoUDFs):
    """Trino identifies a function by its full signature, so a new argument type is a new function."""

    def test_overload(self, project, function_location):
        run_dbt(["build"])
        write_file(
            fixtures.MY_UDF_BIGINT_YML, project.project_root, "functions", "price_for_xlarge.yml"
        )
        run_dbt(["build"])
        signatures = [
            (name, args)
            for name, args, _ in show_functions(project, *function_location)
            if name in UDF_NAMES
        ]
        assert sorted(signatures) == [
            ("price_for_xlarge", "bigint"),
            ("price_for_xlarge", "double"),
        ]


# Dropping a schema drops every function in it. That is only safe in a schema the test owns,
# which `galaxy.functions` is not.
@skip_galaxy
class TestDropSchemaRemovesFunctions(TrinoUDFs):
    """Memory keeps functions after their schema is dropped; Hive refuses to drop the schema."""

    def test_drop_schema(self, project):
        run_dbt(["build"])
        write_file(
            fixtures.MY_UDF_BIGINT_YML, project.project_root, "functions", "price_for_xlarge.yml"
        )
        run_dbt(["build"])
        assert len(show_functions(project, project.database, project.test_schema)) == 2

        relation = project.adapter.Relation.create(
            database=project.database, schema=project.test_schema
        )
        project.adapter.drop_schema(relation)

        assert show_functions(project, project.database, project.test_schema) == []
        with project.adapter.connection_named("_test_schema_exists"):
            assert not project.adapter.check_schema_exists(project.database, project.test_schema)


@pytest.mark.hive
@skip_galaxy
class TestDropSchemaRemovesFunctionsOnHive(TestDropSchemaRemovesFunctions):
    pass


@skip_galaxy
class TestDropSchemaRemovesFunctionsWithUnusualNames(UDFTest):
    """SHOW FUNCTIONS returns names unquoted, so a name with a space must be quoted to drop it."""

    @pytest.mark.parametrize("schema_suffix", ["", "-quoted"])
    def test_drop_schema(self, project, schema_suffix):
        database, schema = project.database, project.test_schema + schema_suffix
        relation = project.adapter.Relation.create(database=database, schema=schema)
        project.adapter.create_schema(relation)
        with project.adapter.connection_named("_test_create_function"):
            project.adapter.execute(
                f'create or replace function {relation}."Weird Name"(x integer) '
                "returns integer return x"
            )
        assert [f[0] for f in show_functions(project, database, schema)] == ["weird name"]

        project.adapter.drop_schema(relation)

        assert show_functions(project, database, schema) == []
        with project.adapter.connection_named("_test_schema_exists"):
            assert not project.adapter.check_schema_exists(database, schema)


# Galaxy cannot store functions in a Hive catalog; see TestUDFsOnUnsupportedConnector
@pytest.mark.hive
@skip_galaxy
class TestUDFsOnHive(TrinoUDFs):
    def test_hive(self, project):
        results = run_dbt(["build"])
        assert results.results[0].status == RunStatus.Success
        result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(100)"])
        assert int(result.results[0].agate_table.rows[0].values()[0]) == 200


@pytest.mark.iceberg
class TestUDFsOnUnsupportedConnector(TrinoUDFs):
    """Iceberg cannot store functions; on Galaxy only `galaxy.functions` can."""

    store_in_galaxy_functions = False

    def test_iceberg(self, project, request):
        msg = expect_build_error()
        if on_galaxy(request):
            assert "functions can only be created in the built-in 'galaxy.functions'" in msg
        else:
            assert "does not support creating functions" in msg


class TestCreateSchema:
    """`trino__create_schema` skips Galaxy's built-in `galaxy.functions` and creates everything else."""

    @staticmethod
    def create_schema(project, database, schema):
        """The statements dbt sends for creating the schema, and the error if it was rejected."""
        catcher = EventCatcher(SQLQuery)
        manager = get_event_manager()
        manager.add_callback(catcher.catch)
        error = None
        try:
            project.adapter.create_schema(
                project.adapter.Relation.create(database=database, schema=schema)
            )
        except Exception as e:
            error = e
        finally:
            manager.callbacks.remove(catcher.catch)
        return [e.data.sql.lower() for e in catcher.caught_events], error

    def test_galaxy_functions_schema_is_not_created(self, project):
        for database, schema in [("galaxy", "functions"), ("GALAXY", "Functions")]:
            statements, error = self.create_schema(project, database, schema)
            assert error is None
            assert not any("create schema" in sql for sql in statements)
            assert any("select 1" in sql for sql in statements)

    def test_other_schemas_are_created(self, project):
        schema = f"{project.test_schema}_created"
        try:
            statements, error = self.create_schema(project, project.database, schema)
            assert error is None
            assert any(
                f'create schema if not exists "{project.database}"."{schema}"' in s
                for s in statements
            )
            with project.adapter.connection_named("_test_schema_exists"):
                assert project.adapter.check_schema_exists(project.database, schema)
        finally:
            project.adapter.drop_schema(
                project.adapter.Relation.create(database=project.database, schema=schema)
            )

    def test_other_schemas_in_the_galaxy_catalog_are_still_attempted(self, project):
        # Rejected on every engine - there is no `galaxy` catalog on Trino and SEP, and on Galaxy
        # only `functions` exists - but the statement has to be sent rather than silently skipped.
        statements, error = self.create_schema(project, "galaxy", f"{project.test_schema}_other")
        assert error is not None
        assert any('create schema if not exists "galaxy"' in sql for sql in statements)

    def test_functions_schema_in_another_catalog_is_still_created(self, project):
        with project.adapter.connection_named("_test_schema_exists"):
            existed = project.adapter.check_schema_exists(project.database, "functions")
        statements, error = self.create_schema(project, project.database, "functions")
        try:
            assert error is None
            assert any(
                f'create schema if not exists "{project.database}"."functions"' in s
                for s in statements
            )
        finally:
            if not existed:
                project.adapter.drop_schema(
                    project.adapter.Relation.create(database=project.database, schema="functions")
                )


class TestGenerateSchemaName:
    """Only `galaxy.functions` keeps its custom schema unprefixed; all else gets the target's."""

    LOCATIONS = {
        "in_galaxy": ("galaxy", "functions"),
        "in_galaxy_mixed_case": ("GALAXY", "Functions"),
        "other_schema": ("galaxy", "other"),
        "other_catalog": ("memory", "functions"),
    }

    @pytest.fixture(scope="class")
    def functions(self):
        files = {}
        for name, (database, schema) in self.LOCATIONS.items():
            files[f"{name}.sql"] = fixtures.MY_UDF_SQL
            files[f"{name}.yml"] = (
                fixtures.MY_UDF_YML.replace("price_for_xlarge", name)
                + f"    config:\n      database: {database}\n      schema: {schema}\n"
            )
        return files

    def test_schema_names(self, project):
        manifest = run_dbt(["parse"])
        schemas = {
            n.name: n.schema for n in manifest.functions.values() if n.name in self.LOCATIONS
        }
        assert schemas == {
            "in_galaxy": "functions",
            "in_galaxy_mixed_case": "Functions",
            "other_schema": f"{project.test_schema}_other",
            "other_catalog": f"{project.test_schema}_functions",
        }
