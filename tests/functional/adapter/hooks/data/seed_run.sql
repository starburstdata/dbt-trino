drop table if exists {schema}.on_run_hook;

create table {schema}.on_run_hook (
    test_state       VARCHAR, -- start|end
    target_dbname    VARCHAR,
    target_host      VARCHAR,
    target_name      VARCHAR,
    target_schema    VARCHAR,
    target_type      VARCHAR,
    target_user      VARCHAR,
    target_pass      VARCHAR,
    target_threads   INTEGER,
    run_started_at   VARCHAR,
    invocation_id    VARCHAR
);
