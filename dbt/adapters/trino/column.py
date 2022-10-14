from dataclasses import dataclass
from typing import ClassVar, Dict

from dbt.adapters.base.column import Column


@dataclass
class TrinoColumn(Column):
    TYPE_LABELS: ClassVar[Dict[str, str]] = {
        "STRING": "VARCHAR",
        "TIMESTAMP": "TIMESTAMP",
        "FLOAT": "DOUBLE",
        "INTEGER": "INT",
    }

    @classmethod
    def string_type(cls, size: int) -> str:
        return "varchar({})".format(size)

    @classmethod
    def from_description(cls, name: str, raw_data_type: str) -> "Column":
        # some of the Trino data types specify a type and not a precision
        if raw_data_type.startswith(("array", "map", "row")):
            return cls(name, raw_data_type)
        return super(TrinoColumn, cls).from_description(name, raw_data_type)
