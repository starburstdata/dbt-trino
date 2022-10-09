import string
import unittest
from unittest import TestCase

import agate
import dbt.flags as flags
import trino
from dbt.clients import agate_helper

from dbt.adapters.trino import TrinoAdapter
from dbt.adapters.trino.connections import (
    HttpScheme,
    TrinoCertificateCredentials,
    TrinoJwtCredentials,
    TrinoKerberosCredentials,
    TrinoLdapCredentials,
    TrinoNoneCredentials,
    TrinoOauthCredentials,
)

from .utils import config_from_parts_or_dicts, mock_connection


class TestTrinoAdapter(unittest.TestCase):
    def setUp(self):
        flags.STRICT_MODE = True

        profile_cfg = {
            "outputs": {
                "test": {
                    "type": "trino",
                    "catalog": "trinodb",
                    "host": "database",
                    "port": 5439,
                    "schema": "dbt_test_schema",
                    "method": "none",
                    "user": "trino_user",
                    "cert": "/path/to/cert",
                    "http_headers": {"X-Trino-Client-Info": "dbt-trino"},
                    "http_scheme": "http",
                    "session_properties": {
                        "query_max_run_time": "5d",
                        "exchange_compression": True,
                    },
                }
            },
            "target": "test",
        }

        project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "quoting": {
                "identifier": False,
                "schema": True,
            },
            "config-version": 2,
        }

        self.config = config_from_parts_or_dicts(project_cfg, profile_cfg)

    @property
    def adapter(self):
        self._adapter = TrinoAdapter(self.config)
        return self._adapter

    def test_acquire_connection(self):
        connection = self.adapter.acquire_connection("dummy")
        connection.handle

        self.assertEqual(connection.state, "open")
        self.assertIsNotNone(connection.handle)

    def test_cancel_open_connections_empty(self):
        self.assertEqual(len(list(self.adapter.cancel_open_connections())), 0)

    def test_cancel_open_connections_master(self):
        key = self.adapter.connections.get_thread_identifier()
        self.adapter.connections.thread_connections[key] = mock_connection("master")
        self.assertEqual(len(list(self.adapter.cancel_open_connections())), 0)


class TestTrinoAdapterAuthenticationMethods(unittest.TestCase):
    def setUp(self):
        flags.STRICT_MODE = True

    def acquire_connetion_with_profile(self, profile):
        profile_cfg = {
            "outputs": {"test": profile},
            "target": "test",
        }

        project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "quoting": {
                "identifier": False,
                "schema": True,
            },
            "config-version": 2,
        }

        config = config_from_parts_or_dicts(project_cfg, profile_cfg)

        return TrinoAdapter(config).acquire_connection("dummy")

    def assert_default_connection_credentials(self, credentials):
        self.assertEqual(credentials.type, "trino")
        self.assertEqual(credentials.database, "trinodb")
        self.assertEqual(credentials.host, "database")
        self.assertEqual(credentials.port, 5439)
        self.assertEqual(credentials.schema, "dbt_test_schema")
        self.assertEqual(credentials.http_headers, {"X-Trino-Client-Info": "dbt-trino"})
        self.assertEqual(
            credentials.session_properties,
            {"query_max_run_time": "5d", "exchange_compression": True},
        )
        self.assertEqual(credentials.prepared_statements_enabled, True)
        self.assertEqual(credentials.retries, trino.constants.DEFAULT_MAX_ATTEMPTS)

    def test_none_authentication(self):
        connection = self.acquire_connetion_with_profile(
            {
                "type": "trino",
                "catalog": "trinodb",
                "host": "database",
                "port": 5439,
                "schema": "dbt_test_schema",
                "user": "trino_user",
                "cert": "/path/to/cert",
                "http_headers": {"X-Trino-Client-Info": "dbt-trino"},
                "http_scheme": "https",
                "session_properties": {
                    "query_max_run_time": "5d",
                    "exchange_compression": True,
                },
            }
        )
        credentials = connection.credentials
        self.assert_default_connection_credentials(credentials)
        self.assertIsInstance(credentials, TrinoNoneCredentials)
        self.assertEqual(credentials.http_scheme, HttpScheme.HTTPS)
        self.assertEqual(credentials.cert, "/path/to/cert")

    def test_none_authentication_with_method(self):
        connection = self.acquire_connetion_with_profile(
            {
                "type": "trino",
                "catalog": "trinodb",
                "host": "database",
                "port": 5439,
                "method": "none",
                "schema": "dbt_test_schema",
                "user": "trino_user",
                "cert": "/path/to/cert",
                "http_headers": {"X-Trino-Client-Info": "dbt-trino"},
                "http_scheme": "https",
                "session_properties": {
                    "query_max_run_time": "5d",
                    "exchange_compression": True,
                },
            }
        )
        credentials = connection.credentials
        self.assert_default_connection_credentials(credentials)
        self.assertIsInstance(credentials, TrinoNoneCredentials)
        self.assertEqual(credentials.http_scheme, HttpScheme.HTTPS)
        self.assertEqual(credentials.cert, "/path/to/cert")

    def test_none_authentication_without_http_scheme(self):
        connection = self.acquire_connetion_with_profile(
            {
                "type": "trino",
                "catalog": "trinodb",
                "host": "database",
                "port": 5439,
                "method": "none",
                "schema": "dbt_test_schema",
                "user": "trino_user",
                "cert": "/path/to/cert",
                "http_headers": {"X-Trino-Client-Info": "dbt-trino"},
                "session_properties": {
                    "query_max_run_time": "5d",
                    "exchange_compression": True,
                },
            }
        )
        credentials = connection.credentials
        self.assert_default_connection_credentials(credentials)
        self.assertIsInstance(credentials, TrinoNoneCredentials)
        self.assertEqual(credentials.http_scheme, HttpScheme.HTTP)
        self.assertEqual(credentials.cert, "/path/to/cert")

    def test_ldap_authentication(self):
        connection = self.acquire_connetion_with_profile(
            {
                "type": "trino",
                "catalog": "trinodb",
                "host": "database",
                "port": 5439,
                "method": "ldap",
                "schema": "dbt_test_schema",
                "user": "trino_user",
                "password": "trino_password",
                "cert": "/path/to/cert",
                "http_headers": {"X-Trino-Client-Info": "dbt-trino"},
                "session_properties": {
                    "query_max_run_time": "5d",
                    "exchange_compression": True,
                },
            }
        )
        credentials = connection.credentials
        self.assertIsInstance(credentials, TrinoLdapCredentials)
        self.assert_default_connection_credentials(credentials)
        self.assertEqual(credentials.http_scheme, HttpScheme.HTTPS)
        self.assertEqual(credentials.cert, "/path/to/cert")

    def test_kerberos_authentication(self):
        connection = self.acquire_connetion_with_profile(
            {
                "type": "trino",
                "catalog": "trinodb",
                "host": "database",
                "port": 5439,
                "method": "kerberos",
                "schema": "dbt_test_schema",
                "user": "trino_user",
                "password": "trino_password",
                "cert": "/path/to/cert",
                "http_headers": {"X-Trino-Client-Info": "dbt-trino"},
                "session_properties": {
                    "query_max_run_time": "5d",
                    "exchange_compression": True,
                },
            }
        )
        credentials = connection.credentials
        self.assertIsInstance(credentials, TrinoKerberosCredentials)
        self.assert_default_connection_credentials(credentials)
        self.assertEqual(credentials.http_scheme, HttpScheme.HTTPS)
        self.assertEqual(credentials.cert, "/path/to/cert")

    def test_certificate_authentication(self):
        connection = self.acquire_connetion_with_profile(
            {
                "type": "trino",
                "catalog": "trinodb",
                "host": "database",
                "port": 5439,
                "method": "certificate",
                "schema": "dbt_test_schema",
                "cert": "/path/to/cert",
                "http_headers": {"X-Trino-Client-Info": "dbt-trino"},
                "client_certificate": "/path/to/client_cert",
                "client_private_key": "password",
                "session_properties": {
                    "query_max_run_time": "5d",
                    "exchange_compression": True,
                },
            }
        )
        credentials = connection.credentials
        self.assertIsInstance(credentials, TrinoCertificateCredentials)
        self.assertIsInstance(credentials.trino_auth(), trino.auth.CertificateAuthentication)
        self.assertEqual(
            credentials.trino_auth(),
            trino.auth.CertificateAuthentication("/path/to/client_cert", "password"),
        )
        self.assert_default_connection_credentials(credentials)
        self.assertEqual(credentials.http_scheme, HttpScheme.HTTPS)
        self.assertEqual(credentials.cert, "/path/to/cert")

    def test_jwt_authentication(self):
        connection = self.acquire_connetion_with_profile(
            {
                "type": "trino",
                "catalog": "trinodb",
                "host": "database",
                "port": 5439,
                "method": "jwt",
                "schema": "dbt_test_schema",
                "cert": "/path/to/cert",
                "jwt_token": "aabbccddeeff",
                "http_headers": {"X-Trino-Client-Info": "dbt-trino"},
                "session_properties": {
                    "query_max_run_time": "5d",
                    "exchange_compression": True,
                },
            }
        )
        credentials = connection.credentials
        self.assertIsInstance(credentials, TrinoJwtCredentials)
        self.assert_default_connection_credentials(credentials)
        self.assertEqual(credentials.http_scheme, HttpScheme.HTTPS)
        self.assertEqual(credentials.cert, "/path/to/cert")

    def test_oauth_authentication(self):
        connection = self.acquire_connetion_with_profile(
            {
                "type": "trino",
                "catalog": "trinodb",
                "host": "database",
                "port": 5439,
                "method": "oauth",
                "schema": "dbt_test_schema",
                "cert": "/path/to/cert",
                "http_headers": {"X-Trino-Client-Info": "dbt-trino"},
                "session_properties": {
                    "query_max_run_time": "5d",
                    "exchange_compression": True,
                },
            }
        )
        credentials = connection.credentials
        self.assertIsInstance(credentials, TrinoOauthCredentials)
        self.assert_default_connection_credentials(credentials)
        self.assertEqual(credentials.http_scheme, HttpScheme.HTTPS)
        self.assertEqual(credentials.cert, "/path/to/cert")
        self.assertEqual(connection.credentials.prepared_statements_enabled, True)


class TestPreparedStatementsEnabled(TestCase):
    def setup_profile(self, credentials):
        profile_cfg = {
            "outputs": {"test": credentials},
            "target": "test",
        }

        project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "quoting": {
                "identifier": False,
                "schema": True,
            },
            "config-version": 2,
        }

        config = config_from_parts_or_dicts(project_cfg, profile_cfg)
        adapter = TrinoAdapter(config)
        connection = adapter.acquire_connection("dummy")
        return connection

    def test_default(self):
        connection = self.setup_profile(
            {
                "type": "trino",
                "catalog": "trinodb",
                "host": "database",
                "port": 5439,
                "schema": "dbt_test_schema",
                "method": "none",
                "user": "trino_user",
                "http_scheme": "http",
            }
        )
        self.assertEqual(connection.credentials.prepared_statements_enabled, True)

    def test_false(self):
        connection = self.setup_profile(
            {
                "type": "trino",
                "catalog": "trinodb",
                "host": "database",
                "port": 5439,
                "schema": "dbt_test_schema",
                "method": "none",
                "user": "trino_user",
                "http_scheme": "http",
                "prepared_statements_enabled": False,
            }
        )
        self.assertEqual(connection.credentials.prepared_statements_enabled, False)

    def test_true(self):
        connection = self.setup_profile(
            {
                "type": "trino",
                "catalog": "trinodb",
                "host": "database",
                "port": 5439,
                "schema": "dbt_test_schema",
                "method": "none",
                "user": "trino_user",
                "http_scheme": "http",
                "prepared_statements_enabled": True,
            }
        )
        self.assertEqual(connection.credentials.prepared_statements_enabled, True)


class TestAdapterConversions(TestCase):
    def _get_tester_for(self, column_type):
        from dbt.clients import agate_helper

        if column_type is agate.TimeDelta:  # dbt never makes this!
            return agate.TimeDelta()

        for instance in agate_helper.DEFAULT_TYPE_TESTER._possible_types:
            if isinstance(instance, column_type):
                return instance

        raise ValueError(f"no tester for {column_type}")

    def _make_table_of(self, rows, column_types):
        column_names = list(string.ascii_letters[: len(rows[0])])
        if isinstance(column_types, type):
            column_types = [self._get_tester_for(column_types) for _ in column_names]
        else:
            column_types = [self._get_tester_for(typ) for typ in column_types]
        table = agate.Table(rows, column_names=column_names, column_types=column_types)
        return table


class TestTrinoAdapterConversions(TestAdapterConversions):
    def test_convert_text_type(self):
        rows = [
            ["", "a1", "stringval1"],
            ["", "a2", "stringvalasdfasdfasdfa"],
            ["", "a3", "stringval3"],
        ]
        agate_table = self._make_table_of(rows, agate.Text)
        expected = ["VARCHAR", "VARCHAR", "VARCHAR"]
        for col_idx, expect in enumerate(expected):
            assert TrinoAdapter.convert_text_type(agate_table, col_idx) == expect

    def test_convert_number_type(self):
        rows = [
            ["", "23.98", "-1"],
            ["", "12.78", "-2"],
            ["", "79.41", "-3"],
        ]
        agate_table = self._make_table_of(rows, agate.Number)
        expected = ["INTEGER", "DOUBLE", "INTEGER"]
        for col_idx, expect in enumerate(expected):
            assert TrinoAdapter.convert_number_type(agate_table, col_idx) == expect

    def test_convert_boolean_type(self):
        rows = [
            ["", "false", "true"],
            ["", "false", "false"],
            ["", "false", "true"],
        ]
        agate_table = self._make_table_of(rows, agate.Boolean)
        expected = ["boolean", "boolean", "boolean"]
        for col_idx, expect in enumerate(expected):
            assert TrinoAdapter.convert_boolean_type(agate_table, col_idx) == expect

    def test_convert_datetime_type(self):
        rows = [
            ["", "20190101T01:01:01Z", "2019-01-01 01:01:01"],
            ["", "20190102T01:01:01Z", "2019-01-01 01:01:01"],
            ["", "20190103T01:01:01Z", "2019-01-01 01:01:01"],
        ]
        agate_table = self._make_table_of(
            rows, [agate.DateTime, agate_helper.ISODateTime, agate.DateTime]
        )
        expected = ["TIMESTAMP", "TIMESTAMP", "TIMESTAMP"]
        for col_idx, expect in enumerate(expected):
            assert TrinoAdapter.convert_datetime_type(agate_table, col_idx) == expect

    def test_convert_date_type(self):
        rows = [
            ["", "2019-01-01", "2019-01-04"],
            ["", "2019-01-02", "2019-01-04"],
            ["", "2019-01-03", "2019-01-04"],
        ]
        agate_table = self._make_table_of(rows, agate.Date)
        expected = ["DATE", "DATE", "DATE"]
        for col_idx, expect in enumerate(expected):
            assert TrinoAdapter.convert_date_type(agate_table, col_idx) == expect
