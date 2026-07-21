from __future__ import annotations

import threading
from typing import Optional

import requests
from dbt.adapters.events.logging import AdapterLogger
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.trino.starburst.api_client import (
    DEFAULT_MAX_COLUMN_DESCRIPTIONS_PER_REQUEST,
    StarburstDiscoveryClient,
)

logger = AdapterLogger("Trino")

FAIL_FAST = "fail_fast"
CONTINUE_ON_ERROR = "continue_on_error"
VALID_FAILURE_STRATEGIES = frozenset({FAIL_FAST, CONTINUE_ON_ERROR})


class StarburstCatalogSync:
    def __init__(self, credentials):
        self._starburst_url = credentials.starburst_url
        self._client_id: Optional[str] = getattr(credentials, "starburst_client_id", None)
        self._secret_key: Optional[str] = getattr(credentials, "starburst_secret_key", None)
        self._failure_strategy = (
            getattr(credentials, "starburst_metadata_failure_strategy", None) or CONTINUE_ON_ERROR
        )
        self._max_column_batch_size = (
            getattr(credentials, "starburst_max_column_batch_size", None)
            or DEFAULT_MAX_COLUMN_DESCRIPTIONS_PER_REQUEST
        )
        self._client: Optional[StarburstDiscoveryClient] = None
        self._client_unavailable = False
        self._client_lock = threading.Lock()

    @property
    def client(self) -> Optional[StarburstDiscoveryClient]:
        if self._client is None and not self._client_unavailable:
            with self._client_lock:
                if self._client is None and not self._client_unavailable:
                    if not self._client_id or not self._secret_key:
                        logger.warning(
                            "Starburst description sync requires starburst_client_id and "
                            "starburst_secret_key in the profile. Skipping Starburst sync."
                        )
                        self._client_unavailable = True
                        return None
                    self._client = StarburstDiscoveryClient(
                        starburst_url=self._starburst_url,
                        client_id=self._client_id,
                        secret_key=self._secret_key,
                        max_column_batch_size=self._max_column_batch_size,
                    )
        return self._client

    def _handle_failure(self, message: str, error: Exception) -> None:
        """Apply the configured failure strategy to an API error."""
        if self._failure_strategy == FAIL_FAST:
            raise DbtRuntimeError(f"{message}: {error}")
        logger.warning(f"{message}: {error}")

    def sync_relation_description(
        self, catalog_name: str, schema: str, table: str, description: str
    ) -> None:
        client = self.client
        if client is None:
            return
        try:
            client.update_table_description(catalog_name, schema, table, description)
        except requests.RequestException as e:
            self._handle_failure(
                f"Failed to update Starburst table description for "
                f"{catalog_name}.{schema}.{table}",
                e,
            )

    def sync_column_descriptions(
        self, catalog_name: str, schema: str, table: str, columns: dict
    ) -> None:
        client = self.client
        if client is None:
            return
        descriptions = {
            col_name: col_info["description"]
            for col_name, col_info in columns.items()
            if isinstance(col_info, dict) and col_info.get("description")
        }
        if not descriptions:
            return
        try:
            client.update_column_descriptions(catalog_name, schema, table, descriptions)
        except requests.RequestException as e:
            self._handle_failure(
                f"Failed to update Starburst column descriptions for "
                f"{catalog_name}.{schema}.{table}",
                e,
            )
