import os
import time

import pytest
from dbt.tests.util import run_dbt

from dbt.adapters.trino.starburst.api_client import (
    STARBURST_API_TIMEOUT,
    StarburstDiscoveryClient,
    _extract_results,
)
from tests.conftest import get_galaxy_target
from tests.functional.adapter.persist_docs.fixtures import (
    STARBURST_SYNC_ID_DESCRIPTION,
    STARBURST_SYNC_NAME_DESCRIPTION,
    STARBURST_SYNC_TABLE_DESCRIPTION,
    seed_csv,
    starburst_sync_model,
    starburst_sync_profile_yml,
)

STARBURST_API_URL = os.environ.get("DBT_TESTS_STARBURST_GALAXY_API_URL")
STARBURST_CLIENT_ID = os.environ.get("DBT_TESTS_STARBURST_GALAXY_CLIENT_ID")
STARBURST_SECRET_KEY = os.environ.get("DBT_TESTS_STARBURST_GALAXY_SECRET_KEY")

TABLE_NAME = "starburst_sync_model"

# A description write may not be readable back the instant persist_docs returns.
_VERIFY_TIMEOUT_SECONDS = 60
_VERIFY_POLL_INTERVAL_SECONDS = 3


def _table_description(client: StarburstDiscoveryClient, table_path: str):
    """Read a table's Data Discovery description back via the REST API."""
    resp = client._session.get(f"{client.base_url}{table_path}", timeout=STARBURST_API_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    # The table resource is returned either bare or wrapped in a "result" object.
    if isinstance(data, dict) and "result" in data:
        data = data["result"]
    if isinstance(data, list):
        data = data[0] if data else {}
    return data.get("description") if isinstance(data, dict) else None


def _column_descriptions(client: StarburstDiscoveryClient, column_path: str) -> dict:
    """Read column descriptions back via the REST API, keyed by column name.

    Data Discovery returns each column as {"columnId": <name>, "description": ...}.
    """
    resp = client._session.get(f"{client.base_url}{column_path}", timeout=STARBURST_API_TIMEOUT)
    resp.raise_for_status()
    columns = _extract_results(resp.json())
    return {
        col.get("columnId"): col.get("description") for col in columns if isinstance(col, dict)
    }


@pytest.mark.skip_profile("trino_starburst")
class TestStarburstDocsSync:
    """
    End-to-end check against a real Starburst Galaxy instance: with persist_docs enabled
    and starburst_url set, running a model should push its table and column descriptions
    to Galaxy's Data Discovery catalog - not just emit COMMENT ON to Trino.

    Runs only on the starburst_galaxy profile and verifies by reading the descriptions
    back from the Data Discovery REST API.
    """

    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        # Enable Data Discovery sync only for this test by setting starburst_url on the
        # Galaxy target - the sync is opt-in, so it stays off for every other test.
        target = get_galaxy_target()
        target.update(
            {
                "starburst_url": STARBURST_API_URL,
                "starburst_client_id": STARBURST_CLIENT_ID,
                "starburst_secret_key": STARBURST_SECRET_KEY,
            }
        )
        return target

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "starburst_docs_sync_tests",
            "models": {"+persist_docs": {"relation": True, "columns": True}},
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            f"{TABLE_NAME}.sql": starburst_sync_model,
            f"{TABLE_NAME}.yml": starburst_sync_profile_yml,
        }

    def test_persist_docs_syncs_to_starburst(self, project):
        assert STARBURST_API_URL and STARBURST_CLIENT_ID and STARBURST_SECRET_KEY, (
            "Set DBT_TESTS_STARBURST_GALAXY_API_URL, DBT_TESTS_STARBURST_GALAXY_CLIENT_ID "
            "and DBT_TESTS_STARBURST_GALAXY_SECRET_KEY to run the Data Discovery sync test."
        )

        run_dbt(["seed"], expect_pass=True)
        results = run_dbt(["run"], expect_pass=True)
        assert len(results) == 1

        catalog_name = project.database
        schema_name = project.test_schema

        credentials = project.adapter.config.credentials
        client = StarburstDiscoveryClient(
            credentials.starburst_url,
            credentials.starburst_client_id,
            credentials.starburst_secret_key,
        )
        assert client._ensure_token(), "failed to obtain a Data Discovery OAuth token"

        catalog_id = client._get_catalog_id(catalog_name)
        assert catalog_id is not None, f"catalog '{catalog_name}' not found in Data Discovery"

        table_path = f"/catalog/{catalog_id}/schema/{schema_name}/table/{TABLE_NAME}"
        column_path = f"{table_path}/column"

        # Poll briefly: the write may not be immediately readable.
        deadline = time.time() + _VERIFY_TIMEOUT_SECONDS
        table_desc = None
        column_descs: dict = {}
        while time.time() < deadline:
            table_desc = _table_description(client, table_path)
            column_descs = _column_descriptions(client, column_path)
            if (
                table_desc == STARBURST_SYNC_TABLE_DESCRIPTION
                and column_descs.get("id") == STARBURST_SYNC_ID_DESCRIPTION
                and column_descs.get("name") == STARBURST_SYNC_NAME_DESCRIPTION
            ):
                break
            time.sleep(_VERIFY_POLL_INTERVAL_SECONDS)

        assert table_desc == STARBURST_SYNC_TABLE_DESCRIPTION
        assert column_descs.get("id") == STARBURST_SYNC_ID_DESCRIPTION
        assert column_descs.get("name") == STARBURST_SYNC_NAME_DESCRIPTION
