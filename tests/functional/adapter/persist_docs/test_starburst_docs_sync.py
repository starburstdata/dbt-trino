import json

import pytest
import responses
from dbt.tests.util import run_dbt

from tests.conftest import get_trino_starburst_target
from tests.functional.adapter.persist_docs.fixtures import (
    STARBURST_SYNC_ID_DESCRIPTION,
    STARBURST_SYNC_NAME_DESCRIPTION,
    STARBURST_SYNC_TABLE_DESCRIPTION,
    seed_csv,
    starburst_sync_model,
    starburst_sync_profile_yml,
)

STARBURST_URL = "https://fake-starburst.test"
STARBURST_CATALOG_ID = "fake-catalog-id"


@pytest.mark.skip_profile("starburst_galaxy")
class TestStarburstDocsSync:
    """
    Verify that enabling persist_docs, on a connection with starburst_url set, sends the
    model's table and column descriptions to the Starburst Data Discovery API with the
    correct payloads - not just to Trino via COMMENT ON.
    """

    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        target = get_trino_starburst_target()
        target.update(
            {
                "starburst_url": STARBURST_URL,
                "starburst_client_id": "fake-client-id",
                "starburst_secret_key": "fake-secret-key",
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
            "starburst_sync_model.sql": starburst_sync_model,
            "starburst_sync_model.yml": starburst_sync_profile_yml,
        }

    def test_persist_docs_syncs_to_starburst(self, project):
        catalog_name = project.database
        schema_name = project.test_schema
        table_path = (
            f"/public/api/v1/catalog/{STARBURST_CATALOG_ID}"
            f"/schema/{schema_name}/table/starburst_sync_model"
        )
        column_path = f"{table_path}/column"

        with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
            # Only intercept calls to the fake Starburst API - let real Trino traffic
            # (also sent via `requests`, through the trino-python-client) pass through.
            rsps.add_passthru(f"http://{project.adapter.config.credentials.host}")
            rsps.add(
                responses.POST,
                f"{STARBURST_URL}/oauth/v2/token",
                json={"access_token": "fake-token", "expires_in": 600},
                status=200,
            )
            rsps.add(
                responses.GET,
                f"{STARBURST_URL}/public/api/v1/catalog",
                json={
                    "result": [{"catalogName": catalog_name, "catalogId": STARBURST_CATALOG_ID}]
                },
                status=200,
            )
            rsps.add(responses.PATCH, f"{STARBURST_URL}{table_path}", status=204)
            rsps.add(responses.PATCH, f"{STARBURST_URL}{column_path}", status=204)

            run_dbt(["seed"], expect_pass=True)
            results = run_dbt(["run"], expect_pass=True)
            assert len(results) == 1

            table_patches = [
                call
                for call in rsps.calls
                if call.request.method == "PATCH" and call.request.url.endswith(table_path)
            ]
            column_patches = [
                call
                for call in rsps.calls
                if call.request.method == "PATCH" and call.request.url.endswith(column_path)
            ]

        assert len(table_patches) == 1
        table_body = json.loads(table_patches[0].request.body)
        assert table_body == {"description": STARBURST_SYNC_TABLE_DESCRIPTION}

        assert len(column_patches) == 1
        column_body = json.loads(column_patches[0].request.body)
        assert column_body == {
            "descriptions": {
                "id": STARBURST_SYNC_ID_DESCRIPTION,
                "name": STARBURST_SYNC_NAME_DESCRIPTION,
            }
        }
