from dataclasses import dataclass
from typing import Dict, Optional
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.trino import TrinoConnectionManager, TrinoColumn
from dbt.adapters.base.impl import AdapterConfig

import agate


@dataclass
class TrinoConfig(AdapterConfig):
    properties: Optional[Dict[str, str]] = None


class TrinoAdapter(SQLAdapter):
    Column = TrinoColumn
    ConnectionManager = TrinoConnectionManager
    AdapterSpecificConfigs = TrinoConfig

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
