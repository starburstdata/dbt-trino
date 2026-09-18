import string
import unittest
from multiprocessing import get_context
from unittest import TestCase
from unittest.mock import MagicMock, Mock, patch

import agate
import dbt.flags as flags
import trino
from dbt.adapters.exceptions.connection import FailedToConnectError
from dbt_common.clients import agate_helper
from dbt_common.exceptions import DbtConfigError, DbtDatabaseError, DbtRuntimeError

from dbt.adapters.trino import TrinoAdapter
from dbt.adapters.trino.column import TRINO_VARCHAR_MAX_LENGTH, TrinoColumn
from dbt.adapters.trino.connections import (
    HttpScheme,
    TrinoCertificateCredentials,
    TrinoGssapiCredentials,
    TrinoJwtCredentials,
    TrinoKerberosCredentials,
    TrinoLdapCredentials,
    TrinoNoneCredentials,
    TrinoOauthConsoleCredentials,
    TrinoOauthCredentials,
)

from .utils import config_from_parts_or_dicts, mock_connection

from dbt.adapters.trino.connections import TrinoConnectionManager

class TestTrinoConnectionManagerCancel(TestCase):
    @patch("dbt.adapters.trino.connections.trino.dbapi.connect", return_value=MagicMock())
    def test_cancel_query_executes_kill_and_closes(self, mock_trino_connect):
        manager = TrinoConnectionManager(profile=None, mp_context=get_context("spawn"))
        fake_query_id = "20240601_123456_00001_abcde"

        mock_cursor = MagicMock()
        mock_handle = MagicMock()
        mock_handle.query_id = [fake_query_id]
        mock_handle.cursor.return_value = mock_cursor

        connection = MagicMock()
        connection.handle = mock_handle

        mock_kill_cursor = MagicMock()
        mock_kill_handle = MagicMock()
        mock_kill_handle.cursor.return_value = mock_kill_cursor
        mock_kill_connection = MagicMock()
        mock_kill_connection.handle = mock_kill_handle

        with patch.object(manager, "get_thread_connection", return_value=mock_kill_connection), \
             patch("dbt.adapters.trino.connections.logger") as mock_logger:
            manager.cancel(connection)

            kill_sql = f"CALL system.runtime.kill_query('{fake_query_id}', 'Cancelled by dbt')"
            mock_kill_cursor.execute.assert_called_with(kill_sql)
            mock_kill_cursor.close.assert_called_once()
            mock_kill_handle.close.assert_called_once()
            mock_handle.cancel.assert_called()
            mock_logger.info.assert_any_call(f"Attempting to cancel Trino query using SQL: {fake_query_id}")
            mock_logger.info.assert_any_call(f"Successfully cancelled Trino query: {fake_query_id}")


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
                        "query_max_run_time": "4h",
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
            "query-comment": "dbt",
            "config-version": 2,
        }

        self.config = config_from_parts_or_dicts(project_cfg, profile_cfg)
        self.assertEqual(self.config.query_comment.comment, "dbt")
        self.assertEqual(self.config.query_comment.append, None)

    @property
    def adapter(self):
        self._adapter = TrinoAdapter(self.config, get_context("spawn"))
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

    @patch("dbt.adapters.trino.TrinoAdapter.ConnectionManager.get_thread_connection")
    def test_database_exception(self, get_thread_connection):
        self._setup_mock_exception(
            get_thread_connection, trino.exceptions.ProgrammingError("Syntax error")
        )
        with self.assertRaises(DbtDatabaseError):
            self.adapter.execute("select 1")

    @patch("dbt.adapters.trino.TrinoAdapter.ConnectionManager.get_thread_connection")
    def test_failed_to_connect_exception(self, get_thread_connection):
        self._setup_mock_exception(
            get_thread_connection,
            trino.exceptions.OperationalError("Failed to establish a new connection"),
        )
        with self.assertRaises(FailedToConnectError):
            self.adapter.execute("select 1")

    @patch("dbt.adapters.trino.TrinoAdapter.ConnectionManager.get_thread_connection")
    def test_dbt_exception(self, get_thread_connection):
        self._setup_mock_exception(get_thread_connection, Exception("Unexpected error"))
        with self.assertRaises(DbtRuntimeError):
            self.adapter.execute("select 1")

    def _setup_mock_exception(self, get_thread_connection, exception):
        connection = mock_connection("master")
        connection.handle = MagicMock()
        cursor = MagicMock()
        cursor.execute = Mock(side_effect=exception)
        connection.handle.cursor = MagicMock(return_value=cursor)
        get_thread_connection.return_value = connection


class TestTrinoAdapterAuthenticationMethods(unittest.TestCase):
    def setUp(self):
        flags.STRICT_MODE = True

    def acquire_connection_with_profile(self, profile):
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

        return TrinoAdapter(config, get_context("spawn")).acquire_connection("dummy")

    def assert_default_connection_credentials(self, credentials):
        self.assertEqual(credentials.type, "trino")
        self.assertEqual(credentials.database, "trinodb")
        self.assertEqual(credentials.host, "database")
        self.assertEqual(credentials.port, 5439)
        self.assertEqual(credentials.schema, "dbt_test_schema")
        self.assertEqual(credentials.http_headers, {"X-Trino-Client-Info": "dbt-trino"})
        self.assertEqual(
            credentials.session_properties,
            {"query_max_run_time": "4h", "exchange_compression": True},
        )
        self.assertEqual(credentials.prepared_statements_enabled, True)
        self.assertEqual(credentials.retries, trino.constants.DEFAULT_MAX_ATTEMPTS)

    def test_none_authentication(self):
        connection = self.acquire_connection_with_profile(
            {
                "type": "trino",
                "catalog": "trinodb",
                "host": "database",
                "port": 5439,
                "schema": "dbt_test_schema",
                "user": "trino_user",
                "cert": "/path/to/cert",
                "client_tags": ["dev", "none"],
                "http_headers": {"X-Trino-Client-Info": "dbt-trino"},
                "http_scheme": "https",
                "session_properties": {
                    "query_max_run_time": "4h",
                    "exchange_compression": True,
                },
                "timezone": "UTC",
                "suppress_cert_warning": False,
            }
        )
        credentials = connection.credentials
        self.assert_default_connection_credentials(credentials)
        self.assertIsInstance(credentials, TrinoNoneCredentials)
        self.assertEqual(credentials.http_scheme, HttpScheme.HTTPS)
        self.assertEqual(credentials.cert, "/path/to/cert")
        self.assertEqual(credentials.client_tags, ["dev", "none"])
        self.assertEqual(credentials.timezone, "UTC")
        self.assertEqual(credentials.suppress_cert_warning, False)

    def test_none_authentication_with_method(self):
        connection = self.acquire_connection_with_profile(
            {
                "type": "trino",
                "catalog": "trinodb",
                "host": "database",
                "port": 5439,
                "method": "none",
                "schema": "dbt_test_schema",
                "user": "trino_user",
                "cert": "/path/to/cert",
                "client_tags": ["dev", "none_with_method"],
                "http_headers": {"X-Trino-Client-Info": "dbt-trino"},
                "http_scheme": "https",
                "session_properties": {
                    "query_max_run_time": "4h",
                    "exchange_compression": True,
                },
                "timezone": "UTC",
                "suppress_cert_warning": False,
            }
        )
        credentials = connection.credentials
        self.assert_default_connection_credentials(credentials)
        self.assertIsInstance(credentials, TrinoNoneCredentials)
        self.assertEqual(credentials.http_scheme, HttpScheme.HTTPS)
        self.assertEqual(credentials.cert, "/path/to/cert")
        self.assertEqual(credentials.client_tags, ["dev", "none_with_method"])
        self.assertEqual(credentials.timezone, "UTC")
        self.assertEqual(credentials.suppress_cert_warning, False)

    def test_none_authentication_without_http_scheme(self):
        connection = self.acquire_connection_with_profile(
            {
                "type": "trino",
                "catalog": "trinodb",
                "host": "database",
                "port": 5439,
                "method": "none",
                "schema": "dbt_test_schema",
                "user": "trino_user",
                "cert": True,
                "client_tags": ["dev", "without_http_scheme"],
                "http_headers": {"X-Trino-Client-Info": "dbt-trino"},
                "session_properties": {
                    "query_max_run_time": "4h",
                    "exchange_compression": True,
                },
                "timezone": "UTC",
                "suppress_cert_warning": False,
            }
        )
        credentials = connection.credentials
        self.assert_default_connection_credentials(credentials)
        self.assertIsInstance(credentials, TrinoNoneCredentials)
        self.assertEqual(credentials.http_scheme, HttpScheme.HTTP)
        self.assertEqual(credentials.cert, True)
        self.assertEqual(credentials.client_tags, ["dev", "without_http_scheme"])
        self.assertEqual(credentials.timezone, "UTC")
        self.assertEqual(credentials.suppress_cert_warning, False)

    def test_ldap_authentication(self):
        test_cases = [(False, "trino_user"), (True, "impersonated_user")]
        for is_impersonation, expected_user in test_cases:
            connection = self.acquire_connection_with_profile(
                {
                    "type": "trino",
                    "catalog": "trinodb",
                    "host": "database",
                    "port": 5439,
                    "method": "ldap",
                    "schema": "dbt_test_schema",
                    "user": "trino_user",
                    "impersonation_user": "impersonated_user" if is_impersonation else None,
                    "password": "trino_password",
                    "cert": False,
                    "client_tags": ["dev", "ldap"],
                    "http_headers": {"X-Trino-Client-Info": "dbt-trino"},
                    "session_properties": {
                        "query_max_run_time": "4h",
                        "exchange_compression": True,
                    },
                    "timezone": "UTC",
                    "suppress_cert_warning": True,
                }
            )
            credentials = connection.credentials
            connection.handle
            self.assertIsInstance(credentials, TrinoLdapCredentials)
            self.assert_default_connection_credentials(credentials)
            self.assertEqual(credentials.http_scheme, HttpScheme.HTTPS)
            self.assertEqual(credentials.cert, False)
            self.assertEqual(connection.handle.handle.user, expected_user)
            self.assertEqual(credentials.client_tags, ["dev", "ldap"])
            self.assertEqual(credentials.timezone, "UTC")
            self.assertEqual(credentials.suppress_cert_warning, True)

    def test_kerberos_authentication(self):
        connection = self.acquire_connection_with_profile(
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
                "client_tags": ["dev", "kerberos"],
                "http_headers": {"X-Trino-Client-Info": "dbt-trino"},
                "session_properties": {
                    "query_max_run_time": "4h",
                    "exchange_compression": True,
                },
                "timezone": "UTC",
                "suppress_cert_warning": False,
            }
        )
        credentials = connection.credentials
        self.assertIsInstance(credentials, TrinoKerberosCredentials)
        self.assert_default_connection_credentials(credentials)
        self.assertEqual(credentials.http_scheme, HttpScheme.HTTPS)
        self.assertEqual(credentials.cert, "/path/to/cert")
        self.assertEqual(credentials.client_tags, ["dev", "kerberos"])
        self.assertEqual(credentials.timezone, "UTC")
        self.assertEqual(credentials.suppress_cert_warning, False)

    def test_gssapi_authentication(self):
        connection = self.acquire_connection_with_profile(
            {
                "type": "trino",
                "catalog": "trinodb",
                "host": "database",
                "port": 5439,
                "method": "gssapi",
                "schema": "dbt_test_schema",
                "user": "trino_user",
                "principal": "trino_user@EXAMPLE.COM",
                "krb5_config": "/etc/krb5.conf",
                "service_name": "trino",
                "hostname_override": "database.example.com",
                "mutual_authentication": "OPTIONAL",
                "force_preemptive": True,
                "delegate": True,
                "cert": "/path/to/cert",
                "client_tags": ["dev", "gssapi"],
                "http_headers": {"X-Trino-Client-Info": "dbt-trino"},
                "session_properties": {
                    "query_max_run_time": "4h",
                    "exchange_compression": True,
                },
                "timezone": "UTC",
                "suppress_cert_warning": False,
            }
        )
        credentials = connection.credentials
        self.assertIsInstance(credentials, TrinoGssapiCredentials)
        self.assert_default_connection_credentials(credentials)
        self.assertEqual(credentials.http_scheme, HttpScheme.HTTPS)
        self.assertEqual(credentials.cert, "/path/to/cert")
        self.assertEqual(credentials.client_tags, ["dev", "gssapi"])
        self.assertEqual(credentials.principal, "trino_user@EXAMPLE.COM")
        self.assertEqual(credentials.service_name, "trino")
        self.assertEqual(credentials.hostname_override, "database.example.com")
        self.assertEqual(credentials.mutual_authentication, "OPTIONAL")
        self.assertEqual(credentials.force_preemptive, True)
        self.assertEqual(credentials.delegate, True)
        self.assertEqual(credentials.timezone, "UTC")
        self.assertEqual(credentials.suppress_cert_warning, False)
        self.assertEqual(
            credentials.trino_auth(),
            trino.auth.GSSAPIAuthentication(
                config="/etc/krb5.conf",
                service_name="trino",
                mutual_authentication=trino.auth.GSSAPIAuthentication.MUTUAL_OPTIONAL,
                force_preemptive=True,
                hostname_override="database.example.com",
                sanitize_mutual_error_response=True,
                principal="trino_user@EXAMPLE.COM",
                delegate=True,
                ca_bundle="/path/to/cert",
            ),
        )

    def test_gssapi_authentication_default_mutual_authentication(self):
        connection = self.acquire_connection_with_profile(
            {
                "type": "trino",
                "catalog": "trinodb",
                "host": "database",
                "port": 5439,
                "method": "gssapi",
                "schema": "dbt_test_schema",
                "user": "trino_user",
            }
        )
        credentials = connection.credentials
        self.assertIsInstance(credentials, TrinoGssapiCredentials)
        self.assertEqual(credentials.mutual_authentication, "DISABLED")
        self.assertEqual(
            credentials.trino_auth(),
            trino.auth.GSSAPIAuthentication(),
        )

    def test_gssapi_authentication_invalid_mutual_authentication(self):
        connection = self.acquire_connection_with_profile(
            {
                "type": "trino",
                "catalog": "trinodb",
                "host": "database",
                "port": 5439,
                "method": "gssapi",
                "schema": "dbt_test_schema",
                "user": "trino_user",
                "mutual_authentication": "bogus",
            }
        )
        with self.assertRaises(DbtConfigError):
            connection.credentials._resolve_mutual_authentication()

    def test_certificate_authentication(self):
        connection = self.acquire_connection_with_profile(
            {
                "type": "trino",
                "catalog": "trinodb",
                "host": "database",
                "port": 5439,
                "method": "certificate",
                "schema": "dbt_test_schema",
                "cert": "/path/to/cert",
                "client_tags": ["dev", "certificate"],
                "http_headers": {"X-Trino-Client-Info": "dbt-trino"},
                "client_certificate": "/path/to/client_cert",
                "client_private_key": "password",
                "session_properties": {
                    "query_max_run_time": "4h",
                    "exchange_compression": True,
                },
                "timezone": "UTC",
                "suppress_cert_warning": False,
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
        self.assertEqual(credentials.client_tags, ["dev", "certificate"])
        self.assertEqual(credentials.timezone, "UTC")
        self.assertEqual(credentials.suppress_cert_warning, False)

    def test_jwt_authentication(self):
        connection = self.acquire_connection_with_profile(
            {
                "type": "trino",
                "catalog": "trinodb",
                "host": "database",
                "port": 5439,
                "method": "jwt",
                "schema": "dbt_test_schema",
                "cert": "/path/to/cert",
                "jwt_token": "aabbccddeeff",
                "client_tags": ["dev", "jwt"],
                "http_headers": {"X-Trino-Client-Info": "dbt-trino"},
                "session_properties": {
                    "query_max_run_time": "4h",
                    "exchange_compression": True,
                },
                "timezone": "UTC",
                "suppress_cert_warning": False,
            }
        )
        credentials = connection.credentials
        self.assertIsInstance(credentials, TrinoJwtCredentials)
        self.assert_default_connection_credentials(credentials)
        self.assertEqual(credentials.http_scheme, HttpScheme.HTTPS)
        self.assertEqual(credentials.cert, "/path/to/cert")
        self.assertEqual(credentials.client_tags, ["dev", "jwt"])
        self.assertEqual(credentials.timezone, "UTC")
        self.assertEqual(credentials.suppress_cert_warning, False)

    def test_oauth_authentication(self):
        connection = self.acquire_connection_with_profile(
            {
                "type": "trino",
                "catalog": "trinodb",
                "host": "database",
                "port": 5439,
                "method": "oauth",
                "schema": "dbt_test_schema",
                "cert": "/path/to/cert",
                "client_tags": ["dev", "oauth"],
                "http_headers": {"X-Trino-Client-Info": "dbt-trino"},
                "session_properties": {
                    "query_max_run_time": "4h",
                    "exchange_compression": True,
                },
                "timezone": "UTC",
                "suppress_cert_warning": False,
            }
        )
        credentials = connection.credentials
        self.assertIsInstance(credentials, TrinoOauthCredentials)
        self.assert_default_connection_credentials(credentials)
        self.assertEqual(credentials.http_scheme, HttpScheme.HTTPS)
        self.assertEqual(credentials.cert, "/path/to/cert")
        self.assertEqual(connection.credentials.prepared_statements_enabled, True)
        self.assertEqual(credentials.client_tags, ["dev", "oauth"])
        self.assertEqual(credentials.timezone, "UTC")
        self.assertEqual(credentials.suppress_cert_warning, False)

    def test_oauth_console_authentication(self):
        connection = self.acquire_connection_with_profile(
            {
                "type": "trino",
                "catalog": "trinodb",
                "host": "database",
                "port": 5439,
                "method": "oauth_console",
                "schema": "dbt_test_schema",
                "cert": "/path/to/cert",
                "client_tags": ["dev", "oauth_console"],
                "http_headers": {"X-Trino-Client-Info": "dbt-trino"},
                "session_properties": {
                    "query_max_run_time": "4h",
                    "exchange_compression": True,
                },
                "timezone": "UTC",
                "suppress_cert_warning": False,
            }
        )
        credentials = connection.credentials
        self.assertIsInstance(credentials, TrinoOauthConsoleCredentials)
        self.assert_default_connection_credentials(credentials)
        self.assertEqual(credentials.http_scheme, HttpScheme.HTTPS)
        self.assertEqual(credentials.cert, "/path/to/cert")
        self.assertEqual(connection.credentials.prepared_statements_enabled, True)
        self.assertEqual(credentials.client_tags, ["dev", "oauth_console"])
        self.assertEqual(credentials.timezone, "UTC")
        self.assertEqual(credentials.suppress_cert_warning, False)


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
        adapter = TrinoAdapter(config, get_context("spawn"))
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


class TestTrinoColumn(unittest.TestCase):
    def test_bound_varchar(self):
        col = TrinoColumn.from_description("my_col", "VARCHAR(100)")
        assert col.column == "my_col"
        assert col.dtype == "VARCHAR"
        assert col.char_size == 100
        # bounded varchars get formatted to lowercase
        assert col.data_type == "varchar(100)"
        assert col.string_size() == 100
        assert col.is_string() is True
        assert col.is_number() is False
        assert col.is_numeric() is False

    def test_unbound_varchar(self):
        col = TrinoColumn.from_description("my_col", "VARCHAR")
        assert col.column == "my_col"
        assert col.dtype == "VARCHAR"
        assert col.char_size is None
        assert col.data_type == "VARCHAR"
        assert col.string_size() == TRINO_VARCHAR_MAX_LENGTH
        assert col.is_string() is True
        assert col.is_number() is False
        assert col.is_numeric() is False


class TestQueryRouting(TestCase):
    """Per-model `client_tags` / `http_headers`, as used for query routing.

    Assertions are made against the headers trino-python-client would put on the
    wire for the next statement, which is what a router such as Starburst Galaxy
    makes its cluster decision on.
    """

    profile_http_headers = {"X-Trino-Client-Info": "dbt-trino"}

    def setUp(self):
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
                    "http_scheme": "http",
                    "client_tags": ["profile-tag"],
                    "http_headers": dict(self.profile_http_headers),
                }
            },
            "target": "test",
        }
        project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "config-version": 2,
        }
        config = config_from_parts_or_dicts(project_cfg, profile_cfg)
        self.adapter = TrinoAdapter(config, get_context("spawn"))

    def tearDown(self):
        # The override is thread-local, so it would otherwise leak between tests.
        self.adapter.connections.clear_query_overrides()
        self.adapter.cleanup_connections()

    def open_connection(self):
        connection = self.adapter.acquire_connection("dummy")
        connection.handle  # resolves the LazyHandle and opens the connection
        return connection

    @staticmethod
    def wire_headers(connection):
        """Headers trino would send for the next statement on this connection."""
        return dict(connection.handle.handle.cursor()._request.http_headers)

    @staticmethod
    def cached_headers(connection):
        """Headers cached on the shared requests session, which are sent too."""
        return dict(connection.handle.handle._http_session.headers)

    def test_no_routing_config_leaves_profile_routing_alone(self):
        connection = self.open_connection()

        assert self.adapter.pre_model_hook({}) is None

        headers = self.wire_headers(connection)
        assert headers["X-Trino-Client-Tags"] == "profile-tag"
        assert headers["X-Trino-Client-Info"] == "dbt-trino"

    def test_client_tags_reroute_an_open_connection(self):
        connection = self.open_connection()

        self.adapter.pre_model_hook({"client_tags": ["etl"]})

        assert self.wire_headers(connection)["X-Trino-Client-Tags"] == "etl"

    def test_http_headers_merge_over_profile_headers(self):
        connection = self.open_connection()

        self.adapter.pre_model_hook({"http_headers": {"X-Route": "fault-tolerant"}})

        headers = self.wire_headers(connection)
        assert headers["X-Route"] == "fault-tolerant"
        assert headers["X-Trino-Client-Info"] == "dbt-trino"
        # client_tags was not configured on the model, so the profile's still apply
        assert headers["X-Trino-Client-Tags"] == "profile-tag"

    def test_routing_is_restored_after_the_model(self):
        connection = self.open_connection()

        context = self.adapter.pre_model_hook(
            {"client_tags": ["etl"], "http_headers": {"X-Route": "fault-tolerant"}}
        )
        self.wire_headers(connection)  # a cursor caches the headers on the session
        self.adapter.post_model_hook({}, context)

        headers = self.wire_headers(connection)
        assert headers["X-Trino-Client-Tags"] == "profile-tag"
        assert headers["X-Trino-Client-Info"] == "dbt-trino"
        assert "X-Route" not in headers
        # requests keeps sending whatever is cached on the session, so the
        # override has to be dropped there as well
        assert "X-Route" not in self.cached_headers(connection)
        assert self.cached_headers(connection).get("X-Trino-Client-Tags") != "etl"

    def test_tags_are_dropped_when_the_profile_sets_none(self):
        self.adapter.config.credentials.client_tags = None
        connection = self.open_connection()

        context = self.adapter.pre_model_hook({"client_tags": ["etl"]})
        self.wire_headers(connection)
        self.adapter.post_model_hook({}, context)

        assert "X-Trino-Client-Tags" not in self.wire_headers(connection)
        assert "X-Trino-Client-Tags" not in self.cached_headers(connection)

    def test_override_survives_opening_the_connection_later(self):
        # No connection yet: `open` has to pick the override up from the thread.
        self.adapter.pre_model_hook({"client_tags": ["etl"]})
        connection = self.open_connection()

        assert self.wire_headers(connection)["X-Trino-Client-Tags"] == "etl"

    def test_client_tags_must_be_a_list(self):
        with self.assertRaises(DbtConfigError):
            self.adapter.pre_model_hook({"client_tags": "etl"})

    def test_client_tags_reject_commas(self):
        with self.assertRaises(DbtConfigError):
            self.adapter.pre_model_hook({"client_tags": ["batch,priority"]})

    def test_client_tags_header_is_rejected(self):
        with self.assertRaises(DbtConfigError) as error:
            self.adapter.pre_model_hook({"http_headers": {"X-Trino-Client-Tags": "etl"}})
        assert "client_tags" in str(error.exception)

    def test_reserved_headers_are_rejected(self):
        with self.assertRaises(DbtConfigError):
            self.adapter.pre_model_hook({"http_headers": {"x-trino-user": "someone-else"}})

    def test_http_headers_reject_newlines_in_name(self):
        with self.assertRaises(DbtConfigError):
            self.adapter.pre_model_hook({"http_headers": {"X-Route\r\nX-Injected": "value"}})

    def test_http_headers_reject_newlines_in_value(self):
        with self.assertRaises(DbtConfigError):
            self.adapter.pre_model_hook({"http_headers": {"X-Route": "value\r\nX-Injected: evil"}})

    def test_overrides_do_not_leak_between_adapter_instances(self):
        # A second adapter instance sharing this thread (e.g. a second profile
        # handled by the same process) must not see the first adapter's override.
        other_profile_cfg = {
            "outputs": {
                "test": {
                    "type": "trino",
                    "catalog": "trinodb",
                    "host": "database",
                    "port": 5439,
                    "schema": "dbt_test_schema",
                    "method": "none",
                    "user": "trino_user",
                    "http_scheme": "http",
                    "client_tags": ["other-adapter-tag"],
                }
            },
            "target": "test",
        }
        other_project_cfg = {
            "name": "Y",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "config-version": 2,
        }
        other_config = config_from_parts_or_dicts(other_project_cfg, other_profile_cfg)
        other_adapter = TrinoAdapter(other_config, get_context("spawn"))
        try:
            self.adapter.pre_model_hook({"client_tags": ["etl"]})

            other_connection = other_adapter.acquire_connection("dummy")
            other_connection.handle  # resolves the LazyHandle and opens the connection

            headers = self.wire_headers(other_connection)
            assert headers["X-Trino-Client-Tags"] == "other-adapter-tag"
        finally:
            other_adapter.connections.clear_query_overrides()
            other_adapter.cleanup_connections()
