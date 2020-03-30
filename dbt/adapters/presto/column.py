from dataclasses import dataclass
from dbt.adapters.base.column import Column


@dataclass
class PrestoColumn(Column):
    @classmethod
    def string_type(cls, size: int) -> str:
        return "varchar({})".format(size)
