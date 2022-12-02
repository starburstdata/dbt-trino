import re
from dataclasses import dataclass
from typing import ClassVar, Dict

from dbt.adapters.base.column import Column
from dbt.exceptions import RuntimeException


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
        match = re.match(
            r"(?P<type>[^(]+)(?P<size>\([^)]+\))?(?P<type_suffix>[\w ]+)?", raw_data_type
        )
        if match is None:
            raise RuntimeException(f'Could not interpret data type "{raw_data_type}"')
        data_type = match.group("type")
        size_info = match.group("size")
        data_type_suffix = match.group("type_suffix")
        if data_type_suffix:
            data_type += data_type_suffix
        char_size = None
        numeric_precision = None
        numeric_scale = None
        if size_info is not None:
            # strip out the parentheses
            size_info = size_info[1:-1]
            parts = size_info.split(",")
            if len(parts) == 1:
                try:
                    char_size = int(parts[0])
                except ValueError:
                    raise RuntimeException(
                        f'Could not interpret data_type "{raw_data_type}": '
                        f'could not convert "{parts[0]}" to an integer'
                    )
            elif len(parts) == 2:
                try:
                    numeric_precision = int(parts[0])
                except ValueError:
                    raise RuntimeException(
                        f'Could not interpret data_type "{raw_data_type}": '
                        f'could not convert "{parts[0]}" to an integer'
                    )
                try:
                    numeric_scale = int(parts[1])
                except ValueError:
                    raise RuntimeException(
                        f'Could not interpret data_type "{raw_data_type}": '
                        f'could not convert "{parts[1]}" to an integer'
                    )

        return cls(name, data_type, char_size, numeric_precision, numeric_scale)
