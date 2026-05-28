import unittest

from dbt.adapters.trino.row_type_utils import (
    collect_field_paths,
    diff_row_types,
    is_row_type,
    parse_row_fields,
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


if __name__ == "__main__":
    unittest.main()
