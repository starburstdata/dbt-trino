import dataclasses
import unittest

from dbt.adapters.trino.connections import (
    TrinoCertificateCredentials,
    TrinoGssapiCredentials,
    TrinoJwtCredentials,
    TrinoKerberosCredentials,
    TrinoLdapCredentials,
    TrinoNoneCredentials,
    TrinoOauthConsoleCredentials,
    TrinoOauthCredentials,
    with_starburst_fields,
)

STARBURST_FIELD_NAMES = {
    "starburst_url",
    "starburst_client_id",
    "starburst_secret_key",
    "starburst_metadata_failure_strategy",
    "starburst_max_column_batch_size",
}

CREDENTIALS_CLASSES = (
    TrinoNoneCredentials,
    TrinoLdapCredentials,
    TrinoKerberosCredentials,
    TrinoGssapiCredentials,
    TrinoCertificateCredentials,
    TrinoJwtCredentials,
    TrinoOauthCredentials,
    TrinoOauthConsoleCredentials,
)


class TestWithStarburstFields(unittest.TestCase):
    """Regression coverage for with_starburst_fields' annotation handling.

    with_starburst_fields used to read a decorated class's own annotations via
    cls.__dict__.get("__annotations__", {}). Under Python 3.14's deferred annotation
    evaluation (PEP 649/749), a class's __dict__ no longer eagerly contains an
    "__annotations__" entry, so that lookup silently returned {} -- dropping every
    field the decorated class declared itself (e.g. session_properties) before
    dataclass() processed the class, which then raised TypeError. These tests assert
    the decorator's actual output (the annotations and fields dataclass() ends up
    with), so they catch that regression on whichever interpreter they run under
    rather than only on 3.14 itself.
    """

    def test_real_credentials_classes_keep_their_own_fields(self):
        for cls in CREDENTIALS_CLASSES:
            with self.subTest(cls=cls.__name__):
                field_names = {f.name for f in dataclasses.fields(cls)}
                self.assertIn(
                    "session_properties",
                    field_names,
                    f"{cls.__name__} lost its own session_properties field",
                )
                self.assertTrue(
                    STARBURST_FIELD_NAMES.issubset(field_names),
                    f"{cls.__name__} is missing starburst fields: "
                    f"{STARBURST_FIELD_NAMES - field_names}",
                )

    def test_decorator_preserves_a_freshly_defined_classs_own_annotation(self):
        # A throwaway class exercises with_starburst_fields in isolation, independent
        # of TrinoCredentials/Credentials and any of their other behavior.
        @with_starburst_fields
        class Dummy:
            widget: int = dataclasses.field(default=0)

        field_names = {f.name for f in dataclasses.fields(Dummy)}
        self.assertIn("widget", field_names)
        self.assertTrue(STARBURST_FIELD_NAMES.issubset(field_names))

        instance = Dummy(widget=5)
        self.assertEqual(instance.widget, 5)
        self.assertEqual(instance.starburst_max_column_batch_size, 100)
        self.assertEqual(instance.starburst_metadata_failure_strategy, "continue_on_error")


if __name__ == "__main__":
    unittest.main()
