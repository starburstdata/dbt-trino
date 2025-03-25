import re
from dataclasses import dataclass
from typing import ClassVar, Dict

from dbt.adapters.base.column import Column
from dbt_common.exceptions import DbtRuntimeError

# Taken from the MAX_LENGTH variable in
# https://github.com/trinodb/trino/blob/master/core/trino-spi/src/main/java/io/trino/spi/type/VarcharType.java
TRINO_VARCHAR_MAX_LENGTH = 2147483646


@dataclass
class TrinoColumn(Column):
    TYPE_LABELS: ClassVar[Dict[str, str]] = {
        "STRING": "VARCHAR",
        "FLOAT": "DOUBLE",
    }

    @property
    def data_type(self):
        # when varchar has no defined size, default to unbound varchar
        # the super().data_type defaults to varchar(256)
        if self.dtype.lower() == "varchar" and self.char_size is None:
            return self.dtype

        return super().data_type

    def is_string(self) -> bool:
        return self.dtype.lower() in ["varchar", "char", "varbinary", "json"]

    def is_float(self) -> bool:
        return self.dtype.lower() in [
            "real",
            "double precision",
            "double",
        ]

    def is_integer(self) -> bool:
        return self.dtype.lower() in [
            "tinyint",
            "smallint",
            "integer",
            "int",
            "bigint",
        ]

    def is_numeric(self) -> bool:
        return self.dtype.lower() == "decimal"

    @classmethod
    def string_type(cls, size: int) -> str:
        return "varchar({})".format(size)

    def string_size(self) -> int:
        # override the string_size function to handle the unbound varchar case
        if self.dtype.lower() == "varchar" and self.char_size is None:
            return TRINO_VARCHAR_MAX_LENGTH

        return super().string_size()

    @classmethod
    def from_description(cls, name: str, raw_data_type: str) -> "Column":
        # Most of the Trino data types specify a type and not a precision/scale/charsize
        if not raw_data_type.lower().startswith(("varchar", "char", "decimal")):
            return cls(name, raw_data_type)
        # Trino data types that do specify a precision/scale/charsize:
        match = re.match(
            r"(?P<type>[^(]+)(?P<size>\([^)]+\))?(?P<type_suffix>[\w ]+)?", raw_data_type
        )
        if match is None:
            raise DbtRuntimeError(f'Could not interpret data type "{raw_data_type}"')
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
                    raise DbtRuntimeError(
                        f'Could not interpret data_type "{raw_data_type}": '
                        f'could not convert "{parts[0]}" to an integer'
                    )
            elif len(parts) == 2:
                try:
                    numeric_precision = int(parts[0])
                except ValueError:
                    raise DbtRuntimeError(
                        f'Could not interpret data_type "{raw_data_type}": '
                        f'could not convert "{parts[0]}" to an integer'
                    )
                try:
                    numeric_scale = int(parts[1])
                except ValueError:
                    raise DbtRuntimeError(
                        f'Could not interpret data_type "{raw_data_type}": '
                        f'could not convert "{parts[1]}" to an integer'
                    )

        return cls(name, data_type, char_size, numeric_precision, numeric_scale)
