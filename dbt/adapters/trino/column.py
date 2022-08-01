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
