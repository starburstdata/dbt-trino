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
        while end < len(value):
            if value[end] == '"' and value[end - 1] != "\\":
                return value[index + 1 : end], end + 1
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


def collect_field_paths(type_str: str, prefix: str = "") -> Dict[str, str]:
    collected: Dict[str, str] = {}
    if not is_row_type(type_str):
        if prefix:
            collected[prefix] = type_str.strip()
        return collected

    for field_name, field_type in parse_row_fields(type_str).items():
        path = f"{prefix}.{field_name}" if prefix else field_name
        if is_row_type(field_type):
            collected.update(collect_field_paths(field_type, path))
        else:
            collected[path] = field_type.strip()

    return collected


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
    source_paths = collect_field_paths(source_type, column_prefix)
    target_paths = collect_field_paths(target_type, column_prefix)

    additions = sorted(
        (path, source_paths[path])
        for path in source_paths
        if path not in target_paths
    )
    removals = sorted(
        (path, target_paths[path])
        for path in target_paths
        if path not in source_paths
    )
    type_changes = sorted(
        (path, source_paths[path])
        for path in source_paths
        if path in target_paths
        and normalize_type(source_paths[path]) != normalize_type(target_paths[path])
    )

    return RowTypeDiff(
        additions=tuple(additions),
        removals=tuple(removals),
        type_changes=tuple(type_changes),
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
