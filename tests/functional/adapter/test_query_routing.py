"""Per-model `client_tags` / `http_headers`, and the query routing they drive.

Starburst Galaxy picks a cluster per statement from the request headers, so these
tests assert on the headers dbt actually put on the wire, and — against Galaxy —
that two models configured with different client tags were executed by different
clusters.
"""
import os
from collections import namedtuple
from unittest.mock import patch

import pytest
import trino
from dbt.tests.util import run_dbt

Statement = namedtuple("Statement", ["sql", "headers", "next_uri"])

CLIENT_TAGS_HEADER = "X-Trino-Client-Tags"
ROUTE_HEADER = "X-Dbt-Test-Route"


@pytest.fixture
def recorded_statements():
    """Record every statement dbt submits, with the headers it was sent with.

    The headers are read back off the prepared request, so they are what the
    server actually received, including anything requests cached on the session.
    """
    statements = []
    original_post = trino.client.TrinoRequest.post

    def recording_post(self, sql, additional_http_headers=None):
        response = original_post(self, sql, additional_http_headers)
        try:
            next_uri = response.json().get("nextUri")
        except ValueError:
            next_uri = None
        # only what the tests assert on: the request also carries `Authorization`,
        # and pytest renders the whole dict into an `in` assertion's failure output
        headers = {
            name: response.request.headers[name]
            for name in (CLIENT_TAGS_HEADER, ROUTE_HEADER)
            if name in response.request.headers
        }
        statements.append(Statement(sql=sql, headers=headers, next_uri=next_uri))
        return response

    with patch.object(trino.client.TrinoRequest, "post", recording_post):
        yield statements


def statements_for(statements, model_name):
    return [statement for statement in statements if model_name in statement.sql]


@pytest.mark.single_thread
class TestModelQueryHeaders:
    """dbt sends a model's own tags and headers, and only for that model.

    Runs on a single thread so that all three models share one connection:
    that is what makes a header leaking from one model to the next visible.
    """

    @pytest.fixture(scope="class")
    def models(self):
        alpha = """
            {{ config(
                materialized='table',
                client_tags=['dbt-alpha'],
                http_headers={'X-Dbt-Test-Route': 'alpha'}
            ) }}
            select 1 as id
        """
        beta = """
            {{ config(materialized='table', client_tags=['dbt-beta']) }}
            select 2 as id
        """
        return {"alpha.sql": alpha, "beta.sql": beta, "plain.sql": "select 3 as id"}

    def test_headers_are_scoped_to_the_model(self, project, recorded_statements):
        results = run_dbt(["run"])
        assert len(results) == 3

        alpha = statements_for(recorded_statements, "alpha")
        assert alpha, "no statements recorded for the alpha model"
        for statement in alpha:
            assert statement.headers[CLIENT_TAGS_HEADER] == "dbt-alpha"
            assert statement.headers[ROUTE_HEADER] == "alpha"

        beta = statements_for(recorded_statements, "beta")
        assert beta, "no statements recorded for the beta model"
        for statement in beta:
            assert statement.headers[CLIENT_TAGS_HEADER] == "dbt-beta"
            # the header alpha set must not leak into another model
            assert ROUTE_HEADER not in statement.headers

        plain = statements_for(recorded_statements, "plain")
        assert plain, "no statements recorded for the plain model"
        for statement in plain:
            assert ROUTE_HEADER not in statement.headers
            assert statement.headers.get(CLIENT_TAGS_HEADER) not in ("dbt-alpha", "dbt-beta")


@pytest.mark.skip_profile("trino_starburst")
@pytest.mark.query_routing
class TestGalaxyQueryRouting:
    """Models with different client tags are run by different Galaxy clusters.

    Requires a Galaxy account with smart routing enabled, reached through its
    routing endpoint, and routing rules that send each of the configured tags to
    a *different* cluster, plus a catch-all rule on a third. See
    `galaxy_routing_target` in tests/conftest.py.

    Each model records the coordinator that executed it, which is how the test
    tells the clusters apart; the dispatched host is reported on failure. The
    third model configures no tags, so it has to land on the catch-all cluster:
    that is what proves a model does not inherit the routing of the one before
    it, at the engine rather than the header level.
    """

    @pytest.fixture(scope="class")
    def models(self):
        first = """
            {{ config(
                materialized='table',
                client_tags=[env_var('DBT_TESTS_STARBURST_GALAXY_ROUTING_TAG_A')]
            ) }}
            select node_id from system.runtime.nodes where coordinator
        """
        second = """
            {{ config(
                materialized='table',
                client_tags=[env_var('DBT_TESTS_STARBURST_GALAXY_ROUTING_TAG_B')]
            ) }}
            select node_id from system.runtime.nodes where coordinator
        """
        untagged = """
            {{ config(materialized='table') }}
            select node_id from system.runtime.nodes where coordinator
        """
        return {
            "routed_first.sql": first,
            "routed_second.sql": second,
            "routed_untagged.sql": untagged,
        }

    def test_models_are_routed_to_different_clusters(self, project, recorded_statements):
        tag_a = os.environ["DBT_TESTS_STARBURST_GALAXY_ROUTING_TAG_A"]
        tag_b = os.environ["DBT_TESTS_STARBURST_GALAXY_ROUTING_TAG_B"]

        results = run_dbt(["run"])
        assert len(results) == 3

        # dbt sent each model's tag, which is what Galaxy routes on
        for model, tag in (("routed_first", tag_a), ("routed_second", tag_b)):
            statements = statements_for(recorded_statements, model)
            assert statements, f"no statements recorded for {model}"
            for statement in statements:
                assert statement.headers[CLIENT_TAGS_HEADER] == tag

        # the model that configures no tags keeps the profile's
        untagged_statements = statements_for(recorded_statements, "routed_untagged")
        assert untagged_statements, "no statements recorded for routed_untagged"
        for statement in untagged_statements:
            assert statement.headers.get(CLIENT_TAGS_HEADER) not in (tag_a, tag_b)

        # ...and Galaxy dispatched all three to different clusters
        coordinators = {
            model: project.run_sql(
                f"select node_id from {project.test_schema}.{model}", fetch="one"
            )[0]
            for model in ("routed_first", "routed_second", "routed_untagged")
        }
        dispatched = sorted({s.next_uri for s in recorded_statements if s.next_uri})
        assert len(set(coordinators.values())) == 3, (
            f"expected three clusters, got {coordinators}. Tags '{tag_a}' and "
            f"'{tag_b}' must route to different clusters, and an untagged query "
            f"to a third. Dispatched to: {dispatched}"
        )
