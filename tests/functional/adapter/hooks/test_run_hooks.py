import pytest
from dbt.tests.adapter.hooks.test_run_hooks import (
    BaseAfterRunHooks,
    BasePrePostRunHooks,
)


class TestPrePostRunHooksTrino(BasePrePostRunHooks):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            # The create and drop table statements here validate that these hooks run
            # in the same order that they are defined. Drop before create is an error.
            # Also check that the table does not exist below.
            "on-run-start": [
                "{{ custom_run_hook('start', target, run_started_at, invocation_id) }}",
                "create table {{ target.schema }}.start_hook_order_test ( id int )",
                "drop table {{ target.schema }}.start_hook_order_test",
                "{{ log(env_var('TERM_TEST'), info=True) }}",
            ],
            "on-run-end": [
                "{{ custom_run_hook('end', target, run_started_at, invocation_id) }}",
                "create table {{ target.schema }}.end_hook_order_test ( id int )",
                "drop table {{ target.schema }}.end_hook_order_test",
                "create table {{ target.schema }}.schemas ( schema varchar )",
                "insert into {{ target.schema }}.schemas (schema) values {% for schema in schemas %}( '{{ schema }}' ){% if not loop.last %},{% endif %}{% endfor %}",
                "create table {{ target.schema }}.db_schemas ( db varchar, schema varchar )",
                "insert into {{ target.schema }}.db_schemas (db, schema) values {% for db, schema in database_schemas %}('{{ db }}', '{{ schema }}' ){% if not loop.last %},{% endif %}{% endfor %}",
            ],
            "seeds": {
                "quote_columns": False,
            },
        }

    def check_hooks(self, state, project, host):
        ctx = self.get_ctx_vars(state, project)

        assert ctx["test_state"] == state
        assert ctx["target_dbname"] == ""
        assert ctx["target_host"] == host
        assert ctx["target_name"] == "default"
        assert ctx["target_schema"] == project.test_schema
        assert ctx["target_threads"] == 4
        assert ctx["target_type"] == project.adapter_type
        assert "admin" in ctx["target_user"]
        assert ctx["target_pass"] == ""

        assert (
            ctx["run_started_at"] is not None and len(ctx["run_started_at"]) > 0
        ), "run_started_at was not set"
        assert (
            ctx["invocation_id"] is not None and len(ctx["invocation_id"]) > 0
        ), "invocation_id was not set"


class TestAfterRunHooksTrino(BaseAfterRunHooks):
    pass
