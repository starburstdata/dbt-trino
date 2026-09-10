#!/usr/bin/env python
"""Set up and verify a Starburst Control Plane (Portal) for the dbt-trino query
routing tests, against `docker-compose-starburst-routing.yml`.

Registers the two SEP clusters as backends, each in its own routing group, then
creates a routing rule per tag pointing at the matching group - both through
Portal's public API (`/public/api/v1/backend` and `/public/api/v1/routingRule`)
- and finally proves it worked by running a tagged query per configured tag
and checking that they land on different clusters.

gateway.routing.type is set to AUTO_TAGS, not CLIENT_TAGS, even though we are
matching on the client's literal X-Trino-Client-Tags header rather than an
auto-tagging rule: in 482-e.1, CLIENT_TAGS routing rules are UI-only, with no
REST API. AUTO_TAGS is the mode that exposes /public/api/v1/routingRule, and
the client's tags are available to it under the "clientTag" tag key
regardless of mode (RequestMetadataTagsProvider surfaces them the same way
either way) - so a rule matching "clientTag" still does exactly what a
CLIENT_TAGS rule would.

A default rule (lowest priority) routes anything without one of the two
tags - including the untagged queries dbt sends outside of a routed model -
to one cluster, matching on the "source" tag, which is always present
because dbt-trino always sets `source=dbt-trino-<version>` on every
connection. Without it, an unrouted query gets no backend at all ("No
active, healthy backends found").

    docker compose -f docker-compose-starburst-routing.yml up -d
    python scripts/starburst_portal_test_setup.py register
    python scripts/starburst_portal_test_setup.py probe

Reads the same environment variables as the tests; see test.env.example.
"""
import argparse
import base64
import json
import os
import pathlib
import sys
import time
import urllib.error
import urllib.request

import trino

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from tests.env_file import ENV_FILE, load_test_env  # noqa: E402

COORDINATOR_QUERY = "select node_id from system.runtime.nodes where coordinator"

PORTAL_URL = "http://localhost:8090"
PORTAL_ADMIN_USER = "admin"
BACKENDS = [
    {"name": "sep1", "group": "cluster-a"},
    {"name": "sep2", "group": "cluster-b"},
]
# The tag key that surfaces the client's X-Trino-Client-Tags header, as
# opposed to a custom tag an auto-tagging rule would derive.
CLIENT_TAG_KEY = "clientTag"
# Always present (see module docstring): what the default rule matches on.
SOURCE_TAG_KEY = "source"

ROUTING_VARS = [
    "DBT_TESTS_STARBURST_ROUTING_TAG_A",
    "DBT_TESTS_STARBURST_ROUTING_TAG_B",
]


def env(name):
    return os.environ.get(name)


def require(names):
    missing = [name for name in names if not env(name)]
    if not missing:
        return
    message = "Missing environment variables: " + ", ".join(missing)
    if ENV_FILE.exists():
        message += f"\nRead {ENV_FILE} but the routing tags are not set there."
    else:
        message += f"\nNo {ENV_FILE}; copy test.env.example to it."
    sys.exit(message)


def portal_request(method, path, body=None, ignore_conflict=False):
    data = json.dumps(body).encode() if body is not None else None
    credentials = base64.b64encode(f"{PORTAL_ADMIN_USER}:".encode()).decode()
    request = urllib.request.Request(
        f"{PORTAL_URL}{path}",
        data=data,
        method=method,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Basic {credentials}",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            payload = response.read()
            return json.loads(payload) if payload else None
    except urllib.error.HTTPError as error:
        # `register` runs again every time `make dbt-starburst-routing-tests`
        # does, even after a manual `register` already created these - a 409
        # here just means there is nothing to do.
        if ignore_conflict and error.code == 409:
            return None
        raise


def wait_for_portal(timeout=300):
    print(f"Waiting for Portal at {PORTAL_URL}...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(f"{PORTAL_URL}/status/livez", timeout=5)
            return
        except (urllib.error.URLError, TimeoutError):
            time.sleep(5)
    sys.exit(f"Portal did not become ready within {timeout}s")


def create_routing_rule(existing_rules, name, priority, group, tag_key, operator, value):
    """Create a routing rule sending queries matching a tag to a routing group.

    A routing group on its own does not route anything - it just labels a
    backend as a possible destination. This is what maps a tag to one.

    `register` runs again every time `make dbt-starburst-routing-tests` does,
    even after a manual `register` already created these, so this skips names
    that already exist rather than retrying the POST: a duplicate priority
    comes back as an unhandled 500 (a duplicate-key error from Portal's own
    database), not a clean 409, so checking by name upfront is more reliable
    than branching on the response status.

    A rule that already exists under this name is only reused as-is, never
    updated - so it must still match what we would have created, or a value
    changed since the last run (e.g. a different routing tag) would silently
    keep routing on the stale one.
    """
    existing = existing_rules.get(name)
    if existing is not None:
        expression = (
            existing.get("condition", {}).get("orClauses", [{}])[0].get("expressions", [{}])[0]
        )
        actual = (
            expression.get("tagKey"),
            expression.get("value"),
            existing.get("action", {}).get("routingGroupId"),
        )
        expected = (tag_key, value, group)
        if actual != expected:
            sys.exit(
                f"Routing rule '{name}' already exists but does not match this configuration "
                f"(expected {expected}, found {actual}). Run "
                "`./docker/remove_starburst_routing.bash && make start-starburst-routing` "
                "to start from a clean Portal, or DELETE "
                f"/public/api/v1/routingRule/{existing.get('id')} directly - then re-run register."
            )
        return
    portal_request(
        "POST",
        "/public/api/v1/routingRule",
        {
            "name": name,
            "priority": priority,
            "condition": {
                "orClauses": [
                    {"expressions": [{"tagKey": tag_key, "operator": operator, "value": value}]}
                ]
            },
            "action": {"type": "routeToGroup", "routingGroupId": group},
        },
    )


def command_register(args):
    """Register sep1 and sep2 as backends and add a routing rule per tag."""
    require(ROUTING_VARS)
    wait_for_portal()

    for backend in BACKENDS:
        print(f"Registering {backend['name']} in routing group '{backend['group']}'...")
        portal_request(
            "POST",
            "/public/api/v1/backend",
            {
                "name": backend["name"],
                "routingGroup": backend["group"],
                "sharedSecret": "internal-shared-secret",
                "active": True,
                "useDiscovery": True,
            },
            ignore_conflict=True,
        )
    print("\nRegistered backends:")
    registered_backends = {
        backend["name"]: backend
        for backend in portal_request("GET", "/public/api/v1/backend") or []
    }
    for expected in BACKENDS:
        actual = registered_backends.get(expected["name"])
        if actual is None or actual.get("routingGroup") != expected["group"]:
            sys.exit(
                f"Backend '{expected['name']}' already exists but is not in routing group "
                f"'{expected['group']}' (found {actual!r}). Run "
                "`./docker/remove_starburst_routing.bash && make start-starburst-routing` "
                "to start from a clean Portal, then re-run register."
            )
        print(f"  {actual.get('name'):<10} routingGroup={actual.get('routingGroup')}")

    tags = (
        env("DBT_TESTS_STARBURST_ROUTING_TAG_A"),
        env("DBT_TESTS_STARBURST_ROUTING_TAG_B"),
    )
    existing_rules = {
        rule["name"]: rule
        for rule in (portal_request("GET", "/public/api/v1/routingRule") or {}).get("result", [])
    }
    print("\nCreating routing rules:")
    for priority, tag, backend in zip((10, 20), tags, BACKENDS):
        print(f"  '{tag}' -> routing group '{backend['group']}'")
        create_routing_rule(
            existing_rules,
            f"dbt-trino-{backend['group']}",
            priority,
            backend["group"],
            CLIENT_TAG_KEY,
            "EQUALS",
            tag,
        )

    # Lowest priority: anything without tag A or B (including dbt's own
    # untagged queries) still needs a cluster to land on.
    default_group = BACKENDS[0]["group"]
    print(f"  (default, untagged) -> routing group '{default_group}'")
    create_routing_rule(
        existing_rules,
        "dbt-trino-default",
        100,
        default_group,
        SOURCE_TAG_KEY,
        "MATCHESPATTERN",
        ".*",
    )

    wait_until_routable()


def wait_until_routable(timeout=120):
    """Wait until Portal actually routes a query, not just until it accepts backends.

    Registering a backend and creating a rule both return as soon as Portal's
    database is updated, before Portal's own health check has polled the
    backend - a query run in that gap gets a `503: service unavailable`
    ("no active, healthy backends found"), even though `register` reported
    success.
    """
    print("\nWaiting for Portal to start routing to the registered backends...")
    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        try:
            run_query(())
            print("Routing is live.")
            return
        except Exception as error:  # noqa: BLE001 - keep retrying regardless of shape
            last_error = error
            time.sleep(2)
    sys.exit(f"Portal did not start routing to backends within {timeout}s: {last_error}")


def run_query(tags):
    """Run a query against the Portal's Trino endpoint with the given client tags.

    Returns the coordinator node id, which is how the test tells the two SEP
    clusters apart.
    """
    connection = trino.dbapi.connect(
        host="localhost",
        port=8090,
        user="admin",
        catalog="system",
        client_tags=list(tags),
    )
    cursor = connection.cursor()
    cursor.execute(COORDINATOR_QUERY)
    return cursor.fetchone()[0]


def command_probe(args):
    require(ROUTING_VARS)
    tag_a = env("DBT_TESTS_STARBURST_ROUTING_TAG_A")
    tag_b = env("DBT_TESTS_STARBURST_ROUTING_TAG_B")

    wait_for_portal()
    print(f"Probing {PORTAL_URL}\n")
    results = {}
    for label, tags in (("A", [tag_a]), ("B", [tag_b])):
        try:
            coordinator = run_query(tags)
        except Exception as error:  # noqa: BLE001 - report whatever Portal said
            print(f"  {label} tag '{tags[0]}': FAILED - {error}")
            results[label] = None
            continue
        print(f"  {label} tag '{tags[0]}': served by {coordinator}")
        results[label] = coordinator

    print()
    if not all(results.values()):
        sys.exit("Routing is not set up yet; see the failures above.")
    if results["A"] == results["B"]:
        sys.exit(
            f"Tags '{tag_a}' and '{tag_b}' both landed on {results['A']}. They must "
            "be routed to different clusters (routing groups) for the routing test "
            "to mean anything."
        )
    print("Routing looks good: the two tags are served by different clusters.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "register",
        help="register sep1 and sep2 as Portal backends, with a routing rule per tag",
    )
    commands.add_parser("probe", help="verify routing sends each tag to its cluster")

    args = parser.parse_args()
    load_test_env()
    {"register": command_register, "probe": command_probe}[args.command](args)


if __name__ == "__main__":
    main()
