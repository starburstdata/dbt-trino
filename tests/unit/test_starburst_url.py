import unittest
from dataclasses import fields

from dbt_common.exceptions import DbtConfigError

from dbt.adapters.trino.connections import (
    TrinoCertificateCredentials,
    TrinoGssapiCredentials,
    TrinoJwtCredentials,
    TrinoKerberosCredentials,
    TrinoLdapCredentials,
    TrinoNoneCredentials,
    TrinoOauthConsoleCredentials,
    TrinoOauthCredentials,
)

ALL_CREDENTIAL_CLASSES = [
    TrinoNoneCredentials,
    TrinoCertificateCredentials,
    TrinoLdapCredentials,
    TrinoKerberosCredentials,
    TrinoGssapiCredentials,
    TrinoJwtCredentials,
    TrinoOauthCredentials,
    TrinoOauthConsoleCredentials,
]

STARBURST_FIELDS = [
    "starburst_url",
    "starburst_client_id",
    "starburst_secret_key",
    "starburst_metadata_failure_strategy",
    "starburst_max_column_batch_size",
]


class TestStarburstCredentialFields(unittest.TestCase):
    def test_starburst_fields_present_on_all_credentials(self):
        for cls in ALL_CREDENTIAL_CLASSES:
            field_names = [f.name for f in fields(cls)]
            for name in STARBURST_FIELDS:
                self.assertIn(name, field_names, f"{name} missing from {cls.__name__}")

    def test_no_galaxy_fields_remain(self):
        for cls in ALL_CREDENTIAL_CLASSES:
            field_names = [f.name for f in fields(cls)]
            for stale in ("galaxy_domain_name", "galaxy_client_id", "galaxy_secret_key"):
                self.assertNotIn(stale, field_names, f"{stale} still on {cls.__name__}")

    def test_field_defaults(self):
        for cls in ALL_CREDENTIAL_CLASSES:
            field_map = {f.name: f for f in fields(cls)}
            self.assertIsNone(field_map["starburst_url"].default)
            self.assertIsNone(field_map["starburst_client_id"].default)
            self.assertIsNone(field_map["starburst_secret_key"].default)
            self.assertEqual(
                field_map["starburst_metadata_failure_strategy"].default,
                "continue_on_error",
            )
            self.assertEqual(field_map["starburst_max_column_batch_size"].default, 100)

    def test_connection_keys_include_url_and_strategy_but_not_secrets(self):
        creds = TrinoNoneCredentials.from_dict(
            {
                "host": "localhost",
                "port": 8080,
                "user": "trino",
                "database": "test_db",
                "schema": "test_schema",
            }
        )
        keys = creds._connection_keys()
        self.assertIn("starburst_url", keys)
        self.assertIn("starburst_metadata_failure_strategy", keys)
        self.assertIn("starburst_max_column_batch_size", keys)
        self.assertNotIn("starburst_client_id", keys)
        self.assertNotIn("starburst_secret_key", keys)

    def test_from_dict_default(self):
        creds = TrinoNoneCredentials.from_dict(
            {
                "host": "localhost",
                "port": 8080,
                "user": "trino",
                "database": "test_db",
                "schema": "test_schema",
            }
        )
        self.assertIsNone(creds.starburst_url)
        self.assertEqual(creds.starburst_metadata_failure_strategy, "continue_on_error")

    def test_from_dict_set_ldap(self):
        creds = TrinoLdapCredentials.from_dict(
            {
                "host": "my-cluster.galaxy.starburst.io",
                "port": 443,
                "user": "test_user",
                "password": "test_pass",
                "database": "my_catalog",
                "schema": "my_schema",
                "starburst_url": "https://my-tenant.galaxy.starburst.io",
                "starburst_client_id": "my-client-id",
                "starburst_secret_key": "my-secret-key",
                "starburst_metadata_failure_strategy": "fail_fast",
                "starburst_max_column_batch_size": 250,
            }
        )
        self.assertEqual(creds.starburst_url, "https://my-tenant.galaxy.starburst.io")
        self.assertEqual(creds.starburst_client_id, "my-client-id")
        self.assertEqual(creds.starburst_secret_key, "my-secret-key")
        self.assertEqual(creds.starburst_metadata_failure_strategy, "fail_fast")
        self.assertEqual(creds.starburst_max_column_batch_size, 250)

    def test_rejects_zero_max_column_batch_size(self):
        with self.assertRaises(DbtConfigError):
            TrinoNoneCredentials.from_dict(
                {
                    "host": "localhost",
                    "port": 8080,
                    "user": "trino",
                    "database": "test_db",
                    "schema": "test_schema",
                    "starburst_max_column_batch_size": 0,
                }
            )

    def test_rejects_negative_max_column_batch_size(self):
        with self.assertRaises(DbtConfigError):
            TrinoNoneCredentials.from_dict(
                {
                    "host": "localhost",
                    "port": 8080,
                    "user": "trino",
                    "database": "test_db",
                    "schema": "test_schema",
                    "starburst_max_column_batch_size": -1,
                }
            )

    def test_rejects_invalid_failure_strategy(self):
        with self.assertRaises(DbtConfigError):
            TrinoNoneCredentials.from_dict(
                {
                    "host": "localhost",
                    "port": 8080,
                    "user": "trino",
                    "database": "test_db",
                    "schema": "test_schema",
                    "starburst_metadata_failure_strategy": "failfast",
                }
            )

    def test_from_dict_set_gssapi(self):
        creds = TrinoGssapiCredentials.from_dict(
            {
                "host": "my-cluster.example.com",
                "port": 443,
                "user": "test_user",
                "database": "my_catalog",
                "schema": "my_schema",
                "starburst_url": "https://my-tenant.example.com",
            }
        )
        self.assertEqual(creds.starburst_url, "https://my-tenant.example.com")


if __name__ == "__main__":
    unittest.main()
