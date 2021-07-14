from dbt.adapters.sql import SQLAdapter
from dbt.adapters.trino import TrinoConnectionManager, TrinoColumn

import agate


class TrinoAdapter(SQLAdapter):
    Column = TrinoColumn
    ConnectionManager = TrinoConnectionManager

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
