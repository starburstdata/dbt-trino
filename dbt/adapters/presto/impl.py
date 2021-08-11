from dataclasses import dataclass
from typing import Dict, Optional
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.presto import PrestoConnectionManager, PrestoColumn
from dbt.adapters.base.impl import AdapterConfig

import agate


@dataclass
class PrestoConfig(AdapterConfig):
    properties: Optional[Dict[str, str]] = None


class PrestoAdapter(SQLAdapter):
    Column = PrestoColumn
    ConnectionManager = PrestoConnectionManager
    AdapterSpecificConfigs = PrestoConfig

    @classmethod
    def date_function(cls):
        return 'datenow()'

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
