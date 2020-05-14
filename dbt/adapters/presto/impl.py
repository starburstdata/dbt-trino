from dbt.adapters.sql import SQLAdapter
from dbt.adapters.presto import PrestoConnectionManager, PrestoColumn

import agate


class PrestoAdapter(SQLAdapter):
    Column = PrestoColumn
    ConnectionManager = PrestoConnectionManager

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
