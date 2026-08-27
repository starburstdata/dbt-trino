import threading
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional

import agate
import trino.constants as trino_constants
from dbt.adapters.base.impl import AdapterConfig, ConstraintSupport
from dbt.adapters.base.meta import available
from dbt.adapters.capability import (
    Capability,
    CapabilityDict,
    CapabilitySupport,
    Support,
)
from dbt.adapters.catalogs import CatalogRelation
from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.sql import SQLAdapter
from dbt_common.behavior_flags import BehaviorFlag
from dbt_common.contracts.constraints import ConstraintType
from dbt_common.exceptions import DbtConfigError, DbtDatabaseError

from dbt.adapters.trino import (
    TrinoColumn,
    TrinoConnectionManager,
    TrinoRelation,
    constants,
    parse_model,
)
from dbt.adapters.trino.catalogs import TrinoCatalogIntegration
from dbt.adapters.trino.connections import RESERVED_HTTP_HEADERS
from dbt.adapters.trino.starburst.catalog_sync import StarburstCatalogSync


@dataclass
class TrinoConfig(AdapterConfig):
    properties: Optional[Dict[str, str]] = None
    view_security: Optional[str] = "definer"
    client_tags: Optional[List[str]] = None
    http_headers: Optional[Dict[str, str]] = None


class TrinoAdapter(SQLAdapter):
    Relation = TrinoRelation
    Column = TrinoColumn
    ConnectionManager = TrinoConnectionManager
    connections: TrinoConnectionManager
    AdapterSpecificConfigs = TrinoConfig

    CATALOG_INTEGRATIONS = [
        TrinoCatalogIntegration,
    ]

    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.not_null: ConstraintSupport.ENFORCED,
        ConstraintType.unique: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.primary_key: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.foreign_key: ConstraintSupport.NOT_SUPPORTED,
    }

    _capabilities: CapabilityDict = CapabilityDict(
        {
            Capability.SchemaMetadataByRelations: CapabilitySupport(support=Support.Full),
            # No information about last table modification in information_schema.tables
            Capability.TableLastModifiedMetadata: CapabilitySupport(support=Support.Unsupported),
            Capability.TableLastModifiedMetadataBatch: CapabilitySupport(
                support=Support.Unsupported
            ),
            Capability.MicrobatchConcurrency: CapabilitySupport(support=Support.Full),
        }
    )

    def __init__(self, config, mp_context) -> None:
        super().__init__(config, mp_context)
        self.connections = self.ConnectionManager(config, mp_context, self.behavior)
        self.add_catalog_integration(constants.DEFAULT_TRINO_CATALOG)
        self._starburst_sync: Optional[StarburstCatalogSync] = None
        self._starburst_sync_lock = threading.Lock()

    @property
    def _behavior_flags(self) -> List[BehaviorFlag]:
        return [
            {  # type: ignore
                "name": "require_certificate_validation",
                "default": False,
                "description": (
                    "SSL certificate validation is disabled by default. "
                    "It is legacy behavior which will be changed in future releases. "
                    "It is strongly advised to enable `require_certificate_validation` flag "
                    "or explicitly set `cert` configuration to `True` for security reasons. "
                    "You may receive an error after that if your SSL setup is incorrect."
                ),
            }
        ]

    def pre_model_hook(self, config: Mapping[str, Any]) -> Optional[bool]:
        """Route this node's statements using its `client_tags` / `http_headers`.

        Trino routers such as Starburst Galaxy pick a cluster per statement based
        on the request headers, so overriding them for the duration of a node
        sends it to a different cluster than the rest of the run.
        """
        client_tags = config.get("client_tags")
        http_headers = config.get("http_headers")
        if client_tags is None and http_headers is None:
            return None

        credentials = self.config.credentials
        tags = self._validated_client_tags(client_tags, credentials)
        headers = self._validated_http_headers(http_headers, credentials)

        self.connections.set_query_overrides(tags, headers)
        return True

    def post_model_hook(self, config: Mapping[str, Any], context: Optional[bool]) -> None:
        if context:
            self.connections.clear_query_overrides()

    @staticmethod
    def _validated_client_tags(client_tags, credentials) -> List[str]:
        if client_tags is None:
            return list(credentials.client_tags or [])
        if isinstance(client_tags, str) or not isinstance(client_tags, (list, tuple)):
            raise DbtConfigError(f"client_tags must be a list of strings, got {client_tags!r}")
        if not all(isinstance(tag, str) for tag in client_tags):
            raise DbtConfigError(f"client_tags must be a list of strings, got {client_tags!r}")
        if any("," in tag for tag in client_tags):
            raise DbtConfigError(f"client_tags entries cannot contain commas, got {client_tags!r}")
        return list(client_tags)

    @staticmethod
    def _validated_http_headers(http_headers, credentials) -> Dict[str, str]:
        profile_headers = dict(credentials.http_headers or {})
        if http_headers is None:
            return profile_headers
        if not isinstance(http_headers, dict):
            raise DbtConfigError(
                f"http_headers must be a mapping of header name to value, got {http_headers!r}"
            )

        reserved = {header.lower() for header in RESERVED_HTTP_HEADERS}
        for name, value in http_headers.items():
            if not isinstance(name, str) or not isinstance(value, str):
                raise DbtConfigError(
                    f"http_headers must be a mapping of header name to value, got {http_headers!r}"
                )
            if name.lower() == trino_constants.HEADER_CLIENT_TAGS.lower():
                raise DbtConfigError(
                    f"{name} is set by Trino itself; use the client_tags config instead"
                )
            if name.lower() in reserved:
                raise DbtConfigError(f"{name} is a reserved Trino header and cannot be overridden")
            if any(char in name or char in value for char in "\r\n"):
                raise DbtConfigError(
                    f"http_headers entries cannot contain newlines, got {name!r}: {value!r}"
                )

        profile_headers.update(http_headers)
        return profile_headers

    @classmethod
    def date_function(cls):
        return "datenow()"

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        return "VARCHAR"

    @classmethod
    def convert_number_type(cls, agate_table, col_idx):
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "DOUBLE" if decimals else "INTEGER"

    @classmethod
    def convert_datetime_type(cls, agate_table, col_idx):
        return "TIMESTAMP"

    @classmethod
    def convert_date_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "DATE"

    def timestamp_add_sql(self, add_to: str, number: int = 1, interval: str = "hour") -> str:
        return f"{add_to} + interval '{number}' {interval}"

    def get_columns_in_relation(self, relation):
        try:
            return super().get_columns_in_relation(relation)
        except DbtDatabaseError as exc:
            if "does not exist" in str(exc):
                return []
            else:
                raise

    def valid_incremental_strategies(self):
        return ["append", "merge", "delete+insert", "microbatch"]

    @available
    def persist_starburst_docs(self, relation, model_dict, for_relation=True, for_columns=True):
        """Sync model/column descriptions to Starburst Data Discovery API."""
        credentials = self.connections.get_thread_connection().credentials
        if not getattr(credentials, "starburst_url", None):
            return

        if self._starburst_sync is None:
            with self._starburst_sync_lock:
                if self._starburst_sync is None:
                    self._starburst_sync = StarburstCatalogSync(credentials)

        catalog_name = relation.database
        schema_name = relation.schema
        table_name = relation.identifier

        if for_relation:
            description = model_dict.get("description", "")
            if description:
                self._starburst_sync.sync_relation_description(
                    catalog_name, schema_name, table_name, description
                )

        if for_columns:
            columns = model_dict.get("columns", {})
            if columns:
                self._starburst_sync.sync_column_descriptions(
                    catalog_name, schema_name, table_name, columns
                )

    @available
    def build_catalog_relation(self, model: RelationConfig) -> Optional[CatalogRelation]:
        """
        Builds a relation for a given configuration.

        This method uses the provided configuration to determine the appropriate catalog
        integration and config parser for building the relation. It defaults to the trino
        catalog if none is provided in the configuration for backward compatibility.

        Args:
            model (RelationConfig): `config.model` (not `model`) from the jinja context

        Returns:
            Any: The constructed relation object generated through the catalog integration and parser
        """
        if catalog := parse_model.catalog_name(model):
            catalog_integration = self.get_catalog_integration(catalog)
            return catalog_integration.build_relation(model)
        return None
