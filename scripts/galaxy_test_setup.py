#!/usr/bin/env python
"""Set up and verify a Starburst Galaxy account for the dbt-trino integration tests.

Galaxy's public API covers clusters and service accounts, but not routing rules -
those have to be created in the UI, under Admin > Routing rules. What this script
does is tell you what is missing and then prove, by running a tagged query per
configured tag, that routing sends them to different clusters.

    python scripts/galaxy_test_setup.py status
    python scripts/galaxy_test_setup.py probe
    python scripts/galaxy_test_setup.py clone-cluster --from my-cluster --name my-cluster-2

Reads the same environment variables as the tests; see test.env.example.
"""
import argparse
import os
import pathlib
import sys

import trino

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from dbt.adapters.trino.starburst.api_client import (  # noqa: E402
    StarburstDiscoveryClient,
)
from tests.env_file import ENV_FILE, load_test_env  # noqa: E402

COORDINATOR_QUERY = "select node_id from system.runtime.nodes where coordinator"

CONNECTION_VARS = [
    "DBT_TESTS_STARBURST_GALAXY_HOST",
    "DBT_TESTS_STARBURST_GALAXY_USER",
    "DBT_TESTS_STARBURST_GALAXY_PASSWORD",
]
API_VARS = [
    "DBT_TESTS_STARBURST_GALAXY_API_URL",
    "DBT_TESTS_STARBURST_GALAXY_CLIENT_ID",
    "DBT_TESTS_STARBURST_GALAXY_SECRET_KEY",
]
ROUTING_VARS = [
    "DBT_TESTS_STARBURST_GALAXY_ROUTING_HOST",
    "DBT_TESTS_STARBURST_GALAXY_ROUTING_TAG_A",
    "DBT_TESTS_STARBURST_GALAXY_ROUTING_TAG_B",
]
# Optional: only needed when the account's catch-all is a tag rule rather than a
# rule on the connecting role.
OPTIONAL_ROUTING_VARS = ["DBT_TESTS_STARBURST_GALAXY_ROUTING_DEFAULT_TAG"]


def env(name):
    return os.environ.get(name)


def require(names):
    missing = [name for name in names if not env(name)]
    if not missing:
        return
    message = "Missing environment variables: " + ", ".join(missing)
    if ENV_FILE.exists():
        found = sum(1 for name in os.environ if name.startswith("DBT_TESTS_STARBURST"))
        message += f"\nRead {ENV_FILE} and took {found} setting(s) from it."
    else:
        message += f"\nNo {ENV_FILE}; copy test.env.example to it."
    sys.exit(message)


def api_client():
    require(API_VARS)
    return StarburstDiscoveryClient(
        env("DBT_TESTS_STARBURST_GALAXY_API_URL"),
        env("DBT_TESTS_STARBURST_GALAXY_CLIENT_ID"),
        env("DBT_TESTS_STARBURST_GALAXY_SECRET_KEY"),
    )


def clusters(client):
    return client._api_request("get", "/cluster")


def run_query(host, tags, query=COORDINATOR_QUERY):
    """Run a query as the test user, with the given client tags.

    Returns the single result value and the host that served the query, which is
    the cluster Galaxy dispatched to rather than the endpoint we connected to.
    No catalog is set, so this works on an account whatever its catalogs are.
    """
    connection = trino.dbapi.connect(
        host=host,
        port=443,
        http_scheme="https",
        auth=trino.auth.BasicAuthentication(
            env("DBT_TESTS_STARBURST_GALAXY_USER"),
            env("DBT_TESTS_STARBURST_GALAXY_PASSWORD"),
        ),
        client_tags=list(tags),
    )
    cursor = connection.cursor()
    cursor.execute(query)
    # Read this before fetching: it is cleared once the query completes.
    next_uri = cursor._request.next_uri or ""
    value = cursor.fetchone()[0]
    return value, next_uri.split("/")[2] if "//" in next_uri else "unknown"


def command_status(args):
    print("Environment")
    for group, names in (
        ("connection", CONNECTION_VARS),
        ("api", API_VARS),
        ("routing", ROUTING_VARS),
        ("routing (optional)", OPTIONAL_ROUTING_VARS),
    ):
        for name in names:
            print(f"  [{'x' if env(name) else ' '}] {name}")

    require(API_VARS)
    print("\nClusters")
    for cluster in clusters(api_client()):
        print(
            f"  {cluster['name']:<30} {cluster['clusterState']:<12} "
            f"{cluster.get('trinoUri', '')}"
        )
    print(
        "\nRouting rules cannot be read or created through the public API. "
        "Check them in the Galaxy UI under Admin > Routing rules."
    )


def command_probe(args):
    require(CONNECTION_VARS + ROUTING_VARS)
    host = env("DBT_TESTS_STARBURST_GALAXY_ROUTING_HOST")
    default_tag = env("DBT_TESTS_STARBURST_GALAXY_ROUTING_DEFAULT_TAG")
    tag_a = env("DBT_TESTS_STARBURST_GALAXY_ROUTING_TAG_A")
    tag_b = env("DBT_TESTS_STARBURST_GALAXY_ROUTING_TAG_B")

    # The untagged probe stands in for the statements dbt runs outside a model.
    probes = [("A", [tag_a]), ("B", [tag_b])]
    probes.insert(0, ("default", [default_tag] if default_tag else []))

    print(f"Probing {host}\n")
    results = {}
    for label, tags in probes:
        tag = ", ".join(tags) or "<none>"
        try:
            coordinator, served_by = run_query(host, tags)
        except Exception as error:  # noqa: BLE001 - report whatever Galaxy said
            message = str(error)
            print(f"  {label} tag '{tag}': FAILED - {message}")
            if "didn't match any cluster" in message:
                print(
                    "      No routing rule matches this tag. Every tag the tests use "
                    "needs a rule, including the default one."
                )
            results[label] = None
            continue
        print(f"  {label} tag '{tag}': served by {coordinator} ({served_by})")
        results[label] = coordinator

    print()
    if not all(results.values()):
        sys.exit("Routing is not set up yet; see the failures above.")
    if results["A"] == results["B"]:
        sys.exit(
            f"Tags '{tag_a}' and '{tag_b}' both landed on {results['A']}. They must "
            "be routed to different clusters for the routing test to mean anything."
        )
    print("Routing looks good: the two tags are served by different clusters.")


def roles_by_id(client):
    return {role["roleId"]: role["roleName"] for role in client._api_request("get", "/role")}


def command_service_accounts(args):
    """List the account's service accounts, with the exact name to log in as."""
    client = api_client()
    roles = roles_by_id(client)
    accounts = client._api_request("get", "/serviceAccount")
    if not accounts:
        print("No service accounts. Create one with create-service-account.")
        return
    print(f"{'username':<40} default role")
    for account in accounts:
        role = roles.get(account.get("roleId"), account.get("roleId", ""))
        print(f"  {account['userName']:<38} {role}")
    print(
        "\nDBT_TESTS_STARBURST_GALAXY_USER takes one of these names, optionally with "
        "a role appended as 'name/role' to log in under a role other than the default."
    )


def command_create_service_account(args):
    """Create a service account with a password and print the settings to use."""
    client = api_client()
    roles = roles_by_id(client)
    wanted = {name: role_id for role_id, name in roles.items()}
    if args.role not in wanted:
        sys.exit(f"No role named '{args.role}'. Found: {', '.join(sorted(wanted))}")

    created = client._api_request(
        "post",
        "/serviceAccount",
        {
            "username": args.name,
            "roleId": wanted[args.role],
            "additionalRoleIds": [],
            "withInitialPassword": True,
        },
    )
    account = created[0] if isinstance(created, list) else created
    passwords = account.get("passwords") or []
    password = passwords[0].get("password") if passwords else None

    print(f"Created service account '{account['userName']}'\n")
    print(f"DBT_TESTS_STARBURST_GALAXY_USER={account['userName']}/{args.role}")
    if password:
        print(f"DBT_TESTS_STARBURST_GALAXY_PASSWORD={password}")
    else:
        print(
            "No password came back. Issue one from the Galaxy UI, or with "
            f"POST /serviceAccount/{account['serviceAccountId']}/serviceAccountPassword"
        )
    print("\nThe password is shown once. Put these in test.env now.")


def find_service_account(client, name):
    accounts = client._api_request("get", "/serviceAccount")
    match = next((a for a in accounts if a["userName"] == name), None)
    if match is None:
        known = ", ".join(sorted(a["userName"] for a in accounts)) or "none"
        sys.exit(f"No service account named '{name}'. The account has: {known}")
    return match


def command_issue_password(args):
    """Issue a fresh password for an existing service account."""
    client = api_client()
    name = args.name or (env("DBT_TESTS_STARBURST_GALAXY_USER") or "").rsplit("/", 1)[0]
    if not name:
        sys.exit("Pass --name, or set DBT_TESTS_STARBURST_GALAXY_USER.")

    account = find_service_account(client, name)
    existing = account.get("passwords") or []
    if len(existing) >= 2:
        print(
            f"'{name}' already has {len(existing)} passwords, which is the limit. "
            "Delete one in the Galaxy UI if this fails."
        )

    issued = client._api_request(
        "post",
        f"/serviceAccount/{account['serviceAccountId']}/serviceAccountPassword",
        {"description": "dbt-trino integration tests"},
    )
    issued = issued[0] if isinstance(issued, list) else issued
    password = issued.get("password")
    if not password:
        sys.exit(f"Galaxy did not return a password: {issued}")

    role = roles_by_id(client).get(account.get("roleId"), "")
    print("Put these in test.env; the password is not retrievable again.\n")
    print(f"DBT_TESTS_STARBURST_GALAXY_USER={name}{'/' + role if role else ''}")
    print(f"DBT_TESTS_STARBURST_GALAXY_PASSWORD={password}")


def command_clone_cluster(args):
    client = api_client()
    existing = clusters(client)
    by_name = {cluster["name"]: cluster for cluster in existing}

    if args.name in by_name:
        print(f"Cluster '{args.name}' already exists.")
        return
    if args.source not in by_name:
        sys.exit(f"No cluster named '{args.source}'. Found: {', '.join(sorted(by_name))}")

    source = by_name[args.source]
    body = {
        "name": args.name,
        "cloudRegionId": source["cloudRegionId"],
        "catalogRefs": source.get("catalogRefs", []),
        "minWorkers": source.get("minWorkers", 1),
        "maxWorkers": source.get("maxWorkers", 1),
        "idleStopMinutes": source.get("idleStopMinutes", 5),
        "resultCacheEnabled": source.get("resultCacheEnabled", False),
        "warpResiliencyEnabled": source.get("warpResiliencyEnabled", False),
        "privateLinkCluster": source.get("privateLinkCluster", False),
    }
    created = client._api_request("post", "/cluster", body)
    print(f"Created cluster '{args.name}': {created}")
    print("Now add a routing rule pointing one of your tags at it, in the Galaxy UI.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    commands.add_parser("status", help="show environment and cluster inventory")
    commands.add_parser("probe", help="verify routing sends each tag to its cluster")
    commands.add_parser("service-accounts", help="list service accounts and their roles")

    create = commands.add_parser(
        "create-service-account", help="create a service account with a password"
    )
    create.add_argument("--name", required=True, help="service account name")
    create.add_argument("--role", default="accountadmin", help="default role to assign")

    password = commands.add_parser(
        "issue-password", help="issue a new password for an existing service account"
    )
    password.add_argument(
        "--name", help="service account username (defaults to the one in the environment)"
    )

    clone = commands.add_parser("clone-cluster", help="copy an existing cluster's config")
    clone.add_argument("--from", dest="source", required=True, help="cluster to copy")
    clone.add_argument("--name", required=True, help="name for the new cluster")

    args = parser.parse_args()
    load_test_env()
    {
        "status": command_status,
        "probe": command_probe,
        "service-accounts": command_service_accounts,
        "create-service-account": command_create_service_account,
        "issue-password": command_issue_password,
        "clone-cluster": command_clone_cluster,
    }[args.command](args)


if __name__ == "__main__":
    main()
