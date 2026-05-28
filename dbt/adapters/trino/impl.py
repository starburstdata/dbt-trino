from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

import agate
from dbt.adapters.base.impl import AdapterConfig, ConstraintSupport
from dbt.adapters.base.meta import available
from dbt.adapters.events.logging import AdapterLogger
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
from dbt.adapters.trino.row_type_utils import (
    RowTypeDiff,
    diff_row_types,
    filter_handled_type_changes,
    is_row_type,
    row_columns_from_type_changes,
)

logger = AdapterLogger("Trino")

NESTED_SCHEMA_EVOLUTION_CATALOGS = frozenset({"iceberg"})


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
            Capability.MicrobatchConcurrency: CapabilitySupport(support=Support.Full),
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

    @classmethod
    def supports_nested_schema_evolution(cls, catalog: Optional[str]) -> bool:
        if catalog is None:
            return False
        return catalog.lower() in NESTED_SCHEMA_EVOLUTION_CATALOGS

    def _format_nested_column_path(self, path: str) -> str:
        return ".".join(self.quote(segment) for segment in path.split("."))

    def _collect_row_type_diffs(
        self,
        source_columns: Dict[str, str],
        target_columns: Dict[str, str],
        row_columns: Set[str],
    ) -> RowTypeDiff:
        additions: List[Tuple[str, str]] = []
        removals: List[Tuple[str, str]] = []
        type_changes: List[Tuple[str, str]] = []

        for column_name in sorted(row_columns):
            source_type = source_columns.get(column_name)
            target_type = target_columns.get(column_name)
            if not source_type or not target_type:
                continue
            if not is_row_type(source_type) and not is_row_type(target_type):
                continue

            column_diff = diff_row_types(source_type, target_type, column_name)
            additions.extend(column_diff.additions)
            removals.extend(column_diff.removals)
            type_changes.extend(column_diff.type_changes)

        return RowTypeDiff(
            additions=tuple(additions),
            removals=tuple(removals),
            type_changes=tuple(type_changes),
        )

    def _apply_nested_schema_changes(
        self,
        relation: TrinoRelation,
        additions: Tuple[Tuple[str, str], ...],
        removals: Tuple[Tuple[str, str], ...],
        type_changes: Tuple[Tuple[str, str], ...],
    ) -> None:
        relation_name = relation.render()
        relation_type = relation.type.replace("_", " ")

        for path, _field_type in removals:
            drop_sql = (
                f"alter {relation_type} {relation_name} "
                f"drop column {self._format_nested_column_path(path)}"
            )
            logger.debug('Dropping nested column `%s` from table "%s".', path, relation_name)
            self.execute(drop_sql, fetch=False)

        for path, new_type in type_changes:
            alter_sql = (
                f"alter {relation_type} {relation_name} "
                f"alter column {self._format_nested_column_path(path)} "
                f"set data type {new_type}"
            )
            logger.debug(
                'Changing nested column `%s` type to %s on table "%s".',
                path,
                new_type,
                relation_name,
            )
            self.execute(alter_sql, fetch=False)

        for path, field_type in additions:
            add_sql = (
                f"alter {relation_type} {relation_name} "
                f"add column {self._format_nested_column_path(path)} {field_type}"
            )
            logger.debug('Adding nested column `%s` to table "%s".', path, relation_name)
            self.execute(add_sql, fetch=False)

    @available.parse(lambda *a, **k: {})
    def sync_row_columns(
        self,
        on_schema_change: str,
        source_relation: TrinoRelation,
        target_relation: TrinoRelation,
        schema_changes_dict: Dict[str, Any],
    ) -> Dict[str, Any]:
        if on_schema_change not in ("append_new_columns", "sync_all_columns"):
            return schema_changes_dict

        catalog = target_relation.database
        if not self.supports_nested_schema_evolution(catalog):
            row_type_changes = row_columns_from_type_changes(
                schema_changes_dict.get("new_target_types", [])
            )
            if row_type_changes:
                logger.warning(
                    "Nested ROW schema changes detected on catalog '%s', but nested schema "
                    "evolution is currently supported only for: %s. Consider using "
                    "full_refresh or manually updating the table schema.",
                    catalog,
                    ", ".join(sorted(NESTED_SCHEMA_EVOLUTION_CATALOGS)),
                )
            return schema_changes_dict

        source_columns = {
            column.name: column.data_type
            for column in schema_changes_dict.get("source_columns", [])
        }
        target_columns = {
            column.name: column.data_type
            for column in schema_changes_dict.get("target_columns", [])
        }

        row_columns = row_columns_from_type_changes(
            schema_changes_dict.get("new_target_types", [])
        )
        for column_name in set(source_columns).intersection(target_columns):
            if is_row_type(source_columns[column_name]) or is_row_type(
                target_columns[column_name]
            ):
                row_columns.add(column_name)

        if not row_columns:
            return schema_changes_dict

        row_diff = self._collect_row_type_diffs(source_columns, target_columns, row_columns)
        additions = row_diff.additions
        removals = row_diff.removals if on_schema_change == "sync_all_columns" else ()
        type_changes = row_diff.type_changes if on_schema_change == "sync_all_columns" else ()

        if not additions and not removals and not type_changes:
            return schema_changes_dict

        logger.debug(
            "Trino ROW sync invoked: mode=%s target=%s additions=%s removals=%s type_changes=%s",
            on_schema_change,
            target_relation.render(),
            additions,
            removals,
            type_changes,
        )

        self._apply_nested_schema_changes(target_relation, additions, removals, type_changes)

        handled_columns = set(row_columns)
        if schema_changes_dict.get("source_not_in_target"):
            schema_changes_dict["source_not_in_target"] = [
                column
                for column in schema_changes_dict["source_not_in_target"]
                if column.name.split(".", 1)[0] not in handled_columns
            ]

        if schema_changes_dict.get("target_not_in_source"):
            schema_changes_dict["target_not_in_source"] = [
                column
                for column in schema_changes_dict["target_not_in_source"]
                if column.name.split(".", 1)[0] not in handled_columns
            ]

        if schema_changes_dict.get("new_target_types"):
            schema_changes_dict["new_target_types"] = filter_handled_type_changes(
                schema_changes_dict["new_target_types"],
                handled_columns,
            )

        schema_changes_dict["target_columns"] = self.get_columns_in_relation(target_relation)

        if (
            not schema_changes_dict.get("source_not_in_target")
            and not schema_changes_dict.get("target_not_in_source")
            and not schema_changes_dict.get("new_target_types")
        ):
            schema_changes_dict["schema_changed"] = False

        return schema_changes_dict

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
