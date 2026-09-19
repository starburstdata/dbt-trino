import unittest

from dbt.adapters.trino.row_type_utils import (
    collect_field_paths,
    diff_row_types,
    filter_handled_type_changes,
    is_row_type,
    normalize_type,
    parse_row_fields,
    row_columns_from_type_changes,
)


class TestRowTypeUtils(unittest.TestCase):
    def test_is_row_type(self):
        self.assertTrue(is_row_type("row(nested_field varchar)"))
        self.assertFalse(is_row_type("varchar"))

    def test_parse_row_fields_simple(self):
        self.assertEqual(
            parse_row_fields("row(nested_field varchar, extra_field varchar)"),
            {
                "nested_field": "varchar",
                "extra_field": "varchar",
            },
        )

    def test_parse_row_fields_quoted_identifier(self):
        fields = parse_row_fields('row("a""b" varchar)')
        self.assertEqual(fields, {'a"b': "varchar"})

    def test_parse_row_fields_nested(self):
        fields = parse_row_fields(
            "row(l1 varchar, level2 row(l2 varchar, level3 row(l3 varchar)))"
        )
        self.assertEqual(fields["l1"], "varchar")
        self.assertEqual(fields["level2"], "row(l2 varchar, level3 row(l3 varchar))")

    def test_collect_field_paths(self):
        paths = collect_field_paths(
            "row(l1 varchar, level2 row(l2 varchar, level3 row(l3 varchar)))",
            "payload",
        )
        self.assertEqual(
            paths,
            {
                "payload.l1": "varchar",
                "payload.level2.l2": "varchar",
                "payload.level2.level3.l3": "varchar",
            },
        )

    def test_diff_row_types_additions(self):
        source = "row(nested_field varchar, extra_field varchar)"
        target = "row(nested_field varchar)"
        diff = diff_row_types(source, target, "payload")

        self.assertEqual(diff.additions, (("payload.extra_field", "varchar"),))
        self.assertEqual(diff.removals, ())
        self.assertEqual(diff.type_changes, ())

    def test_diff_row_types_removals(self):
        source = "row(nested_field varchar)"
        target = "row(nested_field varchar, extra_field varchar)"
        diff = diff_row_types(source, target, "payload")

        self.assertEqual(diff.removals, (("payload.extra_field", "varchar"),))

    def test_diff_row_types_type_changes(self):
        source = "row(nested_field bigint)"
        target = "row(nested_field integer)"
        diff = diff_row_types(source, target, "payload")

        self.assertEqual(diff.type_changes, (("payload.nested_field", "bigint"),))

    def test_diff_row_types_nested_changes(self):
        source = "row(payload row(nested_field bigint, added_field varchar))"
        target = "row(payload row(nested_field integer, removed_field boolean))"
        diff = diff_row_types(source, target, "record")

        self.assertEqual(diff.additions, (("record.payload.added_field", "varchar"),))
        self.assertEqual(diff.removals, (("record.payload.removed_field", "boolean"),))
        self.assertEqual(diff.type_changes, (("record.payload.nested_field", "bigint"),))

    def test_row_columns_from_type_changes(self):
        changes = [
            {
                "column_name": "payload.nested",
                "new_type": "row(nested_field varchar, extra_field varchar)",
            },
            {"column_name": "field1", "new_type": "varchar"},
        ]

        self.assertEqual(row_columns_from_type_changes(changes), {"payload"})

    def test_filter_handled_type_changes(self):
        changes = [
            {
                "column_name": "payload.nested",
                "new_type": "row(nested_field varchar, extra_field varchar)",
            },
            {"column_name": "field1", "new_type": "varchar"},
        ]

        filtered = filter_handled_type_changes(changes, {"payload"})

        self.assertEqual(
            filtered,
            [{"column_name": "field1", "new_type": "varchar"}],
        )

    def test_filter_handled_type_changes_dotted_nested_path(self):
        changes = [
            {"column_name": "payload.nested_field", "new_type": "bigint"},
            {"column_name": "field1", "new_type": "varchar"},
        ]

        filtered = filter_handled_type_changes(changes, {"payload"})

        self.assertEqual(
            filtered,
            [{"column_name": "field1", "new_type": "varchar"}],
        )

    def test_diff_row_types_ignores_case_only_type_differences(self):
        source = "row(nested_field VARCHAR)"
        target = "row(nested_field varchar)"
        diff = diff_row_types(source, target, "payload")

        self.assertEqual(diff.type_changes, ())

    def test_parse_row_fields_decimal_with_precision(self):
        fields = parse_row_fields("row(amount decimal(18,2), name varchar)")
        self.assertEqual(fields["amount"], "decimal(18,2)")
        self.assertEqual(fields["name"], "varchar")

    def test_filter_handled_type_changes_missing_column_name(self):
        changes = [
            {"new_type": "varchar"},
            {"column_name": "field1", "new_type": "varchar"},
        ]

        filtered = filter_handled_type_changes(changes, {"payload"})

        self.assertEqual(
            filtered,
            [
                {"new_type": "varchar"},
                {"column_name": "field1", "new_type": "varchar"},
            ],
        )

    def test_normalize_type_collapses_whitespace_and_case(self):
        self.assertEqual(normalize_type("  VARCHAR  "), "varchar")


if __name__ == "__main__":
    unittest.main()
