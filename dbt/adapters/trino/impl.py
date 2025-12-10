from dataclasses import dataclass
from typing import Dict, List, Optional

import agate
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
from dbt_common.exceptions import DbtDatabaseError

from dbt.adapters.trino import (
    TrinoColumn,
    TrinoConnectionManager,
    TrinoRelation,
    constants,
    parse_model,
)
from dbt.adapters.trino.catalogs import TrinoCatalogIntegration


@dataclass
class TrinoConfig(AdapterConfig):
    properties: Optional[Dict[str, str]] = None
    view_security: Optional[str] = "definer"


class TrinoAdapter(SQLAdapter):
    Relation = TrinoRelation
    Column = TrinoColumn
    ConnectionManager = TrinoConnectionManager
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
        }
    )

    def __init__(self, config, mp_context) -> None:
        super().__init__(config, mp_context)
        self.connections = self.ConnectionManager(config, mp_context, self.behavior)
        self.add_catalog_integration(constants.DEFAULT_TRINO_CATALOG)

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
