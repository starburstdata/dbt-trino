import decimal
import os
import re
from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any, Dict, List, Optional

import sqlparse
import trino
from dbt.adapters.contracts.connection import AdapterResponse, Credentials
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.exceptions.connection import FailedToConnectError
from dbt.adapters.sql import SQLConnectionManager
from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError
from dbt_common.helper_types import Port
from trino.transaction import IsolationLevel

from dbt.adapters.trino.__version__ import version

logger = AdapterLogger("Trino")
PREPARED_STATEMENTS_ENABLED_DEFAULT = True


class HttpScheme(Enum):
    HTTP = "http"
    HTTPS = "https"


class TrinoCredentialsFactory:
    @classmethod
    def _create_trino_profile(cls, profile):
        if "method" in profile:
            method = profile["method"]
            if method == "ldap":
                return TrinoLdapCredentials
            elif method == "certificate":
                return TrinoCertificateCredentials
            elif method == "kerberos":
                return TrinoKerberosCredentials
            elif method == "jwt":
                return TrinoJwtCredentials
            elif method == "oauth":
                return TrinoOauthCredentials
            elif method == "oauth_console":
                return TrinoOauthConsoleCredentials
        return TrinoNoneCredentials

    @classmethod
    def translate_aliases(cls, kwargs: Dict[str, Any], recurse: bool = False) -> Dict[str, Any]:
        klazz = cls._create_trino_profile(kwargs)
        return klazz.translate_aliases(kwargs, recurse)

    @classmethod
    def validate(cls, data: Any):
        klazz = cls._create_trino_profile(data)
        return klazz.validate(data)

    @classmethod
    def from_dict(cls, data: Any):
        klazz = cls._create_trino_profile(data)
        return klazz.from_dict(data)


class TrinoCredentials(Credentials, metaclass=ABCMeta):
    _ALIASES = {"catalog": "database"}

    @property
    def type(self):
        return "trino"

    @property
    def unique_field(self):
        return self.host

    def _connection_keys(self):
        return (
            "method",
            "host",
            "port",
            "user",
            "database",
            "schema",
            "cert",
            "prepared_statements_enabled",
        )

    @abstractmethod
    def trino_auth(self) -> Optional[trino.auth.Authentication]:
        pass


@dataclass
class TrinoNoneCredentials(TrinoCredentials):
    host: str
    port: Port
    user: str
    client_tags: Optional[List[str]] = None
    roles: Optional[Dict[str, str]] = None
    cert: Optional[str] = None
    http_scheme: HttpScheme = HttpScheme.HTTP
    http_headers: Optional[Dict[str, str]] = None
    session_properties: Dict[str, Any] = field(default_factory=dict)
    prepared_statements_enabled: bool = PREPARED_STATEMENTS_ENABLED_DEFAULT
    retries: Optional[int] = trino.constants.DEFAULT_MAX_ATTEMPTS
    timezone: Optional[str] = None

    @property
    def method(self):
        return "none"

    def trino_auth(self):
        return trino.constants.DEFAULT_AUTH


@dataclass
class TrinoCertificateCredentials(TrinoCredentials):
    host: str
    port: Port
    client_certificate: str
    client_private_key: str
    user: Optional[str] = None
    client_tags: Optional[List[str]] = None
    roles: Optional[Dict[str, str]] = None
    cert: Optional[str] = None
    http_headers: Optional[Dict[str, str]] = None
    session_properties: Dict[str, Any] = field(default_factory=dict)
    prepared_statements_enabled: bool = PREPARED_STATEMENTS_ENABLED_DEFAULT
    retries: Optional[int] = trino.constants.DEFAULT_MAX_ATTEMPTS
    timezone: Optional[str] = None

    @property
    def http_scheme(self):
        return HttpScheme.HTTPS

    @property
    def method(self):
        return "certificate"

    def trino_auth(self):
        return trino.auth.CertificateAuthentication(
            self.client_certificate, self.client_private_key
        )


@dataclass
class TrinoLdapCredentials(TrinoCredentials):
    host: str
    port: Port
    user: str
    password: str
    impersonation_user: Optional[str] = None
    client_tags: Optional[List[str]] = None
    roles: Optional[Dict[str, str]] = None
    cert: Optional[str] = None
    http_headers: Optional[Dict[str, str]] = None
    session_properties: Dict[str, Any] = field(default_factory=dict)
    prepared_statements_enabled: bool = PREPARED_STATEMENTS_ENABLED_DEFAULT
    retries: Optional[int] = trino.constants.DEFAULT_MAX_ATTEMPTS
    timezone: Optional[str] = None

    @property
    def http_scheme(self):
        return HttpScheme.HTTPS

    @property
    def method(self):
        return "ldap"

    def trino_auth(self):
        return trino.auth.BasicAuthentication(username=self.user, password=self.password)


@dataclass
class TrinoKerberosCredentials(TrinoCredentials):
    host: str
    port: Port
    user: str
    client_tags: Optional[List[str]] = None
    roles: Optional[Dict[str, str]] = None
    keytab: Optional[str] = None
    principal: Optional[str] = None
    krb5_config: Optional[str] = None
    service_name: Optional[str] = "trino"
    mutual_authentication: Optional[bool] = False
    cert: Optional[str] = None
    http_headers: Optional[Dict[str, str]] = None
    force_preemptive: Optional[bool] = False
    hostname_override: Optional[str] = None
    sanitize_mutual_error_response: Optional[bool] = True
    delegate: Optional[bool] = False
    session_properties: Dict[str, Any] = field(default_factory=dict)
    prepared_statements_enabled: bool = PREPARED_STATEMENTS_ENABLED_DEFAULT
    retries: Optional[int] = trino.constants.DEFAULT_MAX_ATTEMPTS
    timezone: Optional[str] = None

    @property
    def http_scheme(self):
        return HttpScheme.HTTPS

    @property
    def method(self):
        return "kerberos"

    def trino_auth(self):
        os.environ["KRB5_CLIENT_KTNAME"] = self.keytab
        return trino.auth.KerberosAuthentication(
            config=self.krb5_config,
            service_name=self.service_name,
            principal=self.principal,
            mutual_authentication=self.mutual_authentication,
            ca_bundle=self.cert,
            force_preemptive=self.force_preemptive,
            hostname_override=self.hostname_override,
            sanitize_mutual_error_response=self.sanitize_mutual_error_response,
            delegate=self.delegate,
        )


@dataclass
class TrinoJwtCredentials(TrinoCredentials):
    host: str
    port: Port
    jwt_token: str
    user: Optional[str] = None
    client_tags: Optional[List[str]] = None
    roles: Optional[Dict[str, str]] = None
    cert: Optional[str] = None
    http_headers: Optional[Dict[str, str]] = None
    session_properties: Dict[str, Any] = field(default_factory=dict)
    prepared_statements_enabled: bool = PREPARED_STATEMENTS_ENABLED_DEFAULT
    retries: Optional[int] = trino.constants.DEFAULT_MAX_ATTEMPTS
    timezone: Optional[str] = None

    @property
    def http_scheme(self):
        return HttpScheme.HTTPS

    @property
    def method(self):
        return "jwt"

    def trino_auth(self):
        return trino.auth.JWTAuthentication(self.jwt_token)


@dataclass
class TrinoOauthCredentials(TrinoCredentials):
    host: str
    port: Port
    user: Optional[str] = None
    client_tags: Optional[List[str]] = None
    roles: Optional[Dict[str, str]] = None
    cert: Optional[str] = None
    http_headers: Optional[Dict[str, str]] = None
    session_properties: Dict[str, Any] = field(default_factory=dict)
    prepared_statements_enabled: bool = PREPARED_STATEMENTS_ENABLED_DEFAULT
    retries: Optional[int] = trino.constants.DEFAULT_MAX_ATTEMPTS
    timezone: Optional[str] = None
    OAUTH = trino.auth.OAuth2Authentication(
        redirect_auth_url_handler=trino.auth.WebBrowserRedirectHandler()
    )

    @property
    def http_scheme(self):
        return HttpScheme.HTTPS

    @property
    def method(self):
        return "oauth"

    def trino_auth(self):
        return self.OAUTH


@dataclass
class TrinoOauthConsoleCredentials(TrinoCredentials):
    host: str
    port: Port
    user: Optional[str] = None
    client_tags: Optional[List[str]] = None
    roles: Optional[Dict[str, str]] = None
    cert: Optional[str] = None
    http_headers: Optional[Dict[str, str]] = None
    session_properties: Dict[str, Any] = field(default_factory=dict)
    prepared_statements_enabled: bool = PREPARED_STATEMENTS_ENABLED_DEFAULT
    retries: Optional[int] = trino.constants.DEFAULT_MAX_ATTEMPTS
    timezone: Optional[str] = None
    OAUTH = trino.auth.OAuth2Authentication(
        redirect_auth_url_handler=trino.auth.ConsoleRedirectHandler()
    )

    @property
    def http_scheme(self):
        return HttpScheme.HTTPS

    @property
    def method(self):
        return "oauth_console"

    def trino_auth(self):
        return self.OAUTH


class ConnectionWrapper(object):
    """Wrap a Trino connection in a way that accomplishes two tasks:

    - prefetch results from execute() calls so that trino calls actually
        persist to the db but then present the usual cursor interface
    - provide `cancel()` on the same object as `commit()`/`rollback()`/...

    """

    def __init__(self, handle, prepared_statements_enabled):
        self.handle = handle
        self._cursor = None
        self._fetch_result = None
        self._prepared_statements_enabled = prepared_statements_enabled

    def cursor(self):
        self._cursor = self.handle.cursor()
        return self

    def cancel(self):
        if self._cursor is not None:
            self._cursor.cancel()

    def close(self):
        # this is a noop on trino, but pass it through anyway
        self.handle.close()

    def commit(self):
        pass

    def rollback(self):
        pass

    def start_transaction(self):
        pass

    def fetchall(self):
        if self._cursor is None:
            return None

        if self._fetch_result is not None:
            ret = self._fetch_result
            self._fetch_result = None
            return ret

        return None

    def fetchone(self):
        if self._cursor is None:
            return None

        if self._fetch_result is not None:
            ret = self._fetch_result[0]
            self._fetch_result = None
            return ret

        return None

    def fetchmany(self, size):
        if self._cursor is None:
            return None

        if self._fetch_result is not None:
            ret = self._fetch_result[:size]
            self._fetch_result = None
            return ret

        return None

    def execute(self, sql, bindings=None):
        if not self._prepared_statements_enabled and bindings is not None:
            # DEPRECATED: by default prepared statements are used.
            # Code is left as an escape hatch if prepared statements
            # are failing.
            bindings = tuple(self._escape_value(b) for b in bindings)
            sql = sql % bindings

            result = self._cursor.execute(sql)
        else:
            result = self._cursor.execute(sql, params=bindings)

        self._fetch_result = self._cursor.fetchall()
        return result

    @property
    def description(self):
        return self._cursor.description

    @classmethod
    def _escape_value(cls, value):
        """A not very comprehensive system for escaping bindings.

        I think "'" (a single quote) is the only character that matters.
        """
        numbers = (decimal.Decimal, int, float)
        if value is None:
            return "NULL"
        elif isinstance(value, str):
            return "'{}'".format(value.replace("'", "''"))
        elif isinstance(value, numbers):
            return value
        elif isinstance(value, datetime):
            time_formatted = value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            return "TIMESTAMP '{}'".format(time_formatted)
        elif isinstance(value, date):
            date_formatted = value.strftime("%Y-%m-%d")
            return "DATE '{}'".format(date_formatted)
        else:
            raise ValueError("Cannot escape {}".format(type(value)))


@dataclass
class TrinoAdapterResponse(AdapterResponse):
    query: str = ""
    query_id: str = ""


class TrinoConnectionManager(SQLConnectionManager):
    TYPE = "trino"

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield
        except trino.exceptions.Error as e:
            msg = str(e)

            if "Failed to establish a new connection" in msg:
                raise FailedToConnectError(msg) from e

            if isinstance(e, trino.exceptions.TrinoQueryError):
                logger.debug("Trino query id: {}".format(e.query_id))
            logger.debug("Trino error: {}".format(msg))

            raise DbtDatabaseError(msg)
        except Exception as e:
            msg = str(e)
            if isinstance(e, DbtRuntimeError):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise
            raise DbtRuntimeError(msg) from e

    # For connection in auto-commit mode there is no need to start
    # separate transaction. If using auto-commit, the client will
    # create a new transaction and commit/rollback for each query
    def add_begin_query(self):
        pass

    def add_commit_query(self):
        pass

    @classmethod
    def open(cls, connection):
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = connection.credentials

        # it's impossible for trino to fail here as 'connections' are actually
        # just cursor factories.
        trino_conn = trino.dbapi.connect(
            host=credentials.host,
            port=credentials.port,
            user=credentials.impersonation_user
            if getattr(credentials, "impersonation_user", None)
            else credentials.user,
            client_tags=credentials.client_tags,
            roles=credentials.roles,
            catalog=credentials.database,
            schema=credentials.schema,
            http_scheme=credentials.http_scheme.value,
            http_headers=credentials.http_headers,
            session_properties=credentials.session_properties,
            auth=credentials.trino_auth(),
            max_attempts=credentials.retries,
            isolation_level=IsolationLevel.AUTOCOMMIT,
            source=f"dbt-trino-{version}",
            verify=credentials.cert,
            timezone=credentials.timezone,
        )
        connection.state = "open"
        connection.handle = ConnectionWrapper(trino_conn, credentials.prepared_statements_enabled)
        return connection

    @classmethod
    def get_response(cls, cursor) -> TrinoAdapterResponse:
        message = "SUCCESS"
        return TrinoAdapterResponse(
            _message=message,
            query=cursor._cursor.query,
            query_id=cursor._cursor.query_id,
            rows_affected=cursor._cursor.rowcount,
        )  # type: ignore

    def cancel(self, connection):
        connection.handle.cancel()

    def add_query(self, sql, auto_begin=True, bindings=None, abridge_sql_log=False):
        connection = None
        cursor = None

        # TODO: is this sufficient? Largely copy+pasted from snowflake, so
        # there's some common behavior here we can maybe factor out into the
        # SQLAdapter?
        queries = [q.rstrip(";") for q in sqlparse.split(sql)]

        for individual_query in queries:
            # hack -- after the last ';', remove comments and don't run
            # empty queries. this avoids using exceptions as flow control,
            # and also allows us to return the status of the last cursor
            without_comments = re.sub(
                re.compile("^.*(--.*)$", re.MULTILINE), "", individual_query
            ).strip()

            if without_comments == "":
                continue

            parent = super(TrinoConnectionManager, self)
            connection, cursor = parent.add_query(
                individual_query, auto_begin, bindings, abridge_sql_log
            )

        if cursor is None:
            conn = self.get_thread_connection()
            if conn is None or conn.name is None:
                conn_name = "<None>"
            else:
                conn_name = conn.name

            raise DbtRuntimeError(
                "Tried to run an empty query on model '{}'. If you are "
                "conditionally running\nsql, eg. in a model hook, make "
                "sure your `else` clause contains valid sql!\n\n"
                "Provided SQL:\n{}".format(conn_name, sql)
            )

        return connection, cursor

    @classmethod
    def data_type_code_to_name(cls, type_code) -> str:
        return type_code.split("(")[0].upper()
