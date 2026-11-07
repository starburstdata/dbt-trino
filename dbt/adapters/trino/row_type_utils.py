from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, List, Set, Tuple

ROW_PREFIX = "row("


def normalize_type(type_str: str) -> str:
    return re.sub(r"\s+", " ", type_str.strip().lower())


def is_row_type(type_str: str) -> bool:
    return normalize_type(type_str).startswith(ROW_PREFIX)


def _skip_whitespace(value: str, index: int) -> int:
    while index < len(value) and value[index].isspace():
        index += 1
    return index


def _parse_identifier(value: str, index: int) -> Tuple[str, int]:
    index = _skip_whitespace(value, index)
    if index >= len(value):
        raise ValueError(f"Expected identifier at position {index}")

    if value[index] == '"':
        end = index + 1
        chars: List[str] = []
        while end < len(value):
            if value[end] == '"':
                if end + 1 < len(value) and value[end + 1] == '"':
                    chars.append('"')
                    end += 2
                    continue
                return "".join(chars), end + 1
            chars.append(value[end])
            end += 1
        raise ValueError(f"Unterminated quoted identifier in {value!r}")

    match = re.match(r"[a-zA-Z_][\w$]*", value[index:])
    if not match:
        raise ValueError(f"Invalid identifier at position {index} in {value!r}")
    identifier = match.group(0)
    return identifier, index + len(identifier)


def _parse_type(value: str, index: int) -> Tuple[str, int]:
    index = _skip_whitespace(value, index)
    if index >= len(value):
        raise ValueError(f"Expected type at position {index}")

    if value[index : index + 4].lower() == ROW_PREFIX:
        depth = 0
        start = index
        for cursor in range(index, len(value)):
            if value[cursor] == "(":
                depth += 1
            elif value[cursor] == ")":
                depth -= 1
                if depth == 0:
                    return value[start : cursor + 1], cursor + 1
        raise ValueError(f"Unterminated row type in {value!r}")

    start = index
    paren_depth = 0
    while index < len(value):
        char = value[index]
        if char == "(":
            paren_depth += 1
        elif char == ")":
            if paren_depth == 0:
                break
            paren_depth -= 1
        elif char == "," and paren_depth == 0:
            break
        index += 1

    return value[start:index].strip(), index


def parse_row_fields(row_type: str) -> Dict[str, str]:
    normalized = row_type.strip()
    if not is_row_type(normalized):
        raise ValueError(f"Expected row type, got {row_type!r}")

    inner = normalized[normalized.index("(") + 1 : normalized.rfind(")")]
    fields: Dict[str, str] = {}
    index = 0

    while index < len(inner):
        index = _skip_whitespace(inner, index)
        if index >= len(inner):
            break

        field_name, index = _parse_identifier(inner, index)
        index = _skip_whitespace(inner, index)
        field_type, index = _parse_type(inner, index)
        fields[field_name] = field_type

        index = _skip_whitespace(inner, index)
        if index < len(inner):
            if inner[index] != ",":
                raise ValueError(f"Expected comma in row definition {row_type!r}")
            index += 1

    return fields


@dataclass(frozen=True)
class RowTypeDiff:
    additions: Tuple[Tuple[str, str], ...]
    removals: Tuple[Tuple[str, str], ...]
    type_changes: Tuple[Tuple[str, str], ...]


def diff_row_types(
    source_type: str,
    target_type: str,
    column_prefix: str,
) -> RowTypeDiff:
    source_fields = parse_row_fields(source_type)
    target_fields = parse_row_fields(target_type)

    additions: List[Tuple[str, str]] = []
    removals: List[Tuple[str, str]] = []
    type_changes: List[Tuple[str, str]] = []

    for field_name, source_field_type in source_fields.items():
        path = f"{column_prefix}.{field_name}"
        target_field_type = target_fields.get(field_name)
        if target_field_type is None:
            # Trino needs every parent of a nested ADD COLUMN path to exist, so a
            # new ROW field is added as a whole rather than one leaf at a time.
            additions.append((path, source_field_type.strip()))
        elif is_row_type(source_field_type) and is_row_type(target_field_type):
            nested_diff = diff_row_types(source_field_type, target_field_type, path)
            additions.extend(nested_diff.additions)
            removals.extend(nested_diff.removals)
            type_changes.extend(nested_diff.type_changes)
        elif normalize_type(source_field_type) != normalize_type(target_field_type):
            type_changes.append((path, source_field_type.strip()))

    for field_name, target_field_type in target_fields.items():
        if field_name not in source_fields:
            removals.append((f"{column_prefix}.{field_name}", target_field_type.strip()))

    return RowTypeDiff(
        additions=tuple(sorted(additions)),
        removals=tuple(sorted(removals)),
        type_changes=tuple(sorted(type_changes)),
    )


def row_columns_from_type_changes(
    new_target_types: List[dict],
) -> Set[str]:
    columns: Set[str] = set()
    for change in new_target_types:
        column_name = change.get("column_name")
        new_type = change.get("new_type", "")
        if column_name and is_row_type(new_type):
            columns.add(column_name.split(".", 1)[0])
    return columns


def filter_handled_type_changes(
    new_target_types: List[dict],
    handled_columns: Set[str],
) -> List[dict]:
    filtered: List[dict] = []
    for change in new_target_types:
        column_name = change.get("column_name")
        if not column_name:
            filtered.append(change)
            continue
        root_column = column_name.split(".", 1)[0]
        if root_column not in handled_columns:
            filtered.append(change)
    return filtered
