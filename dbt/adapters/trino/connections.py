import decimal
import os
import re
import sys
import threading
from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union, cast

import sqlparse
import trino
from dbt.adapters.contracts.connection import AdapterResponse, Credentials
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.exceptions.connection import FailedToConnectError
from dbt.adapters.sql import SQLConnectionManager
from dbt_common.exceptions import DbtConfigError, DbtDatabaseError, DbtRuntimeError
from dbt_common.helper_types import Port
from trino.transaction import IsolationLevel

from dbt.adapters.trino.__version__ import version
from dbt.adapters.trino.starburst.catalog_sync import VALID_FAILURE_STRATEGIES

logger = AdapterLogger("Trino")
PREPARED_STATEMENTS_ENABLED_DEFAULT = True

# Headers that trino-python-client builds from other connection settings on every
# request. Passing any of them through `http_headers` makes the client raise, so
# they are rejected up front with a message pointing at the setting to use instead.
RESERVED_HTTP_HEADERS = frozenset(
    [
        trino.constants.HEADER_CATALOG,
        trino.constants.HEADER_SCHEMA,
        trino.constants.HEADER_SOURCE,
        trino.constants.HEADER_USER,
        trino.constants.HEADER_ORIGINAL_USER,
        trino.constants.HEADER_TIMEZONE,
        trino.constants.HEADER_ENCODING,
        trino.constants.HEADER_CLIENT_CAPABILITIES,
        trino.constants.HEADER_ROLE,
        trino.constants.HEADER_CLIENT_TAGS,
        trino.constants.HEADER_SESSION,
        trino.constants.HEADER_PREPARED_STATEMENT,
        trino.constants.HEADER_TRANSACTION,
        trino.constants.HEADER_EXTRA_CREDENTIAL,
        "user-agent",
    ]
)

# Per-model routing overrides live in thread-local storage: dbt gives each thread
# its own connection and runs one node at a time on it, and the override has to
# survive a reconnect in the middle of a model.
_query_overrides = threading.local()


class HttpScheme(Enum):
    HTTP = "http"
    HTTPS = "https"


def _own_annotations(cls) -> Dict[str, Any]:
    """Return a class's own (non-inherited) annotations, deferred-evaluation safe.

    ``cls.__dict__.get("__annotations__", {})`` is how this used to be written, but
    under PEP 649/749 lazy annotations (the default starting in Python 3.14) a class
    that only has annotated assignments in its body no longer gets an
    ``__annotations__`` entry in ``__dict__`` at all -- it's materialized on first
    access instead. That made this always return ``{}`` on 3.14, silently dropping
    every field the decorated class itself declared (e.g. ``session_properties``)
    before they got overwritten below, which then made ``dataclass()`` reject the
    class with "is a field but has no type annotation".

    ``inspect.get_annotations(cls)`` is the version-safe replacement recommended for
    exactly this case, but it isn't available before Python 3.10 -- the dict lookup
    is kept as a fallback there since annotations are still eager that far back.
    """
    if sys.version_info >= (3, 10):
        import inspect

        return inspect.get_annotations(cls, eval_str=False)
    return dict(cls.__dict__.get("__annotations__", {}))


def with_starburst_fields(cls):
    """Add the shared Starburst metadata-sync credential fields to a dataclass.

    The fields are appended after the class's own fields so they never precede a
    required field, keeping this compatible with Python 3.9 where dataclass
    ``kw_only`` is unavailable.
    """
    cls.__annotations__ = {
        **_own_annotations(cls),
        "starburst_url": Optional[str],
        "starburst_client_id": Optional[str],
        "starburst_secret_key": Optional[str],
        "starburst_metadata_failure_strategy": Optional[str],
        "starburst_max_column_batch_size": Optional[int],
    }
    cls.starburst_url = None
    cls.starburst_client_id = None
    cls.starburst_secret_key = field(default=None, repr=False)
    cls.starburst_metadata_failure_strategy = "continue_on_error"
    cls.starburst_max_column_batch_size = 100

    def __post_init__(self) -> None:
        if (
            self.starburst_max_column_batch_size is not None
            and self.starburst_max_column_batch_size < 1
        ):
            raise DbtConfigError(
                "starburst_max_column_batch_size must be a positive integer, got "
                f"{self.starburst_max_column_batch_size}"
            )
        if (
            self.starburst_metadata_failure_strategy
            and self.starburst_metadata_failure_strategy not in VALID_FAILURE_STRATEGIES
        ):
            raise DbtConfigError(
                f"starburst_metadata_failure_strategy must be one of "
                f"{sorted(VALID_FAILURE_STRATEGIES)}, got "
                f"'{self.starburst_metadata_failure_strategy}'"
            )

    cls.__post_init__ = __post_init__
    return dataclass(cls)


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
            elif method == "gssapi":
                return TrinoGssapiCredentials
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

    # Declared for the benefit of code that routes queries; every concrete
    # credentials type below defines them as fields.
    client_tags: Optional[List[str]]
    http_headers: Optional[Dict[str, str]]

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
            "starburst_url",
            "starburst_metadata_failure_strategy",
            "starburst_max_column_batch_size",
        )

    @abstractmethod
    def trino_auth(self) -> Optional[trino.auth.Authentication]:
        pass


@with_starburst_fields
class TrinoNoneCredentials(TrinoCredentials):
    host: str
    port: Port
    user: str
    client_tags: Optional[List[str]] = None
    roles: Optional[Dict[str, str]] = None
    cert: Optional[Union[str, bool]] = None
    http_scheme: HttpScheme = HttpScheme.HTTP
    http_headers: Optional[Dict[str, str]] = None
    session_properties: Dict[str, Any] = field(default_factory=dict)
    prepared_statements_enabled: bool = PREPARED_STATEMENTS_ENABLED_DEFAULT
    retries: Optional[int] = trino.constants.DEFAULT_MAX_ATTEMPTS
    timezone: Optional[str] = None
    suppress_cert_warning: Optional[bool] = None

    @property
    def method(self):
        return "none"

    def trino_auth(self):
        return trino.constants.DEFAULT_AUTH


@with_starburst_fields
class TrinoCertificateCredentials(TrinoCredentials):
    host: str
    port: Port
    client_certificate: str
    client_private_key: str
    user: Optional[str] = None
    client_tags: Optional[List[str]] = None
    roles: Optional[Dict[str, str]] = None
    cert: Optional[Union[str, bool]] = None
    http_headers: Optional[Dict[str, str]] = None
    session_properties: Dict[str, Any] = field(default_factory=dict)
    prepared_statements_enabled: bool = PREPARED_STATEMENTS_ENABLED_DEFAULT
    retries: Optional[int] = trino.constants.DEFAULT_MAX_ATTEMPTS
    timezone: Optional[str] = None
    suppress_cert_warning: Optional[bool] = None

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


@with_starburst_fields
class TrinoLdapCredentials(TrinoCredentials):
    host: str
    port: Port
    user: str
    password: str
    impersonation_user: Optional[str] = None
    client_tags: Optional[List[str]] = None
    roles: Optional[Dict[str, str]] = None
    cert: Optional[Union[str, bool]] = None
    http_headers: Optional[Dict[str, str]] = None
    session_properties: Dict[str, Any] = field(default_factory=dict)
    prepared_statements_enabled: bool = PREPARED_STATEMENTS_ENABLED_DEFAULT
    retries: Optional[int] = trino.constants.DEFAULT_MAX_ATTEMPTS
    timezone: Optional[str] = None
    suppress_cert_warning: Optional[bool] = None

    @property
    def http_scheme(self):
        return HttpScheme.HTTPS

    @property
    def method(self):
        return "ldap"

    def trino_auth(self):
        return trino.auth.BasicAuthentication(username=self.user, password=self.password)


@with_starburst_fields
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
    cert: Optional[Union[str, bool]] = None
    http_headers: Optional[Dict[str, str]] = None
    force_preemptive: Optional[bool] = False
    hostname_override: Optional[str] = None
    sanitize_mutual_error_response: Optional[bool] = True
    delegate: Optional[bool] = False
    session_properties: Dict[str, Any] = field(default_factory=dict)
    prepared_statements_enabled: bool = PREPARED_STATEMENTS_ENABLED_DEFAULT
    retries: Optional[int] = trino.constants.DEFAULT_MAX_ATTEMPTS
    timezone: Optional[str] = None
    suppress_cert_warning: Optional[bool] = None

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


# Mapping from human-readable mutual-authentication mode (used in dbt profiles)
# to trino-python-client's integer constants. Kept module-level so it's exposed
# for tests and for any future auth methods that need the same translation.
_GSSAPI_MUTUAL_AUTH_VALUES = {
    "REQUIRED": trino.auth.GSSAPIAuthentication.MUTUAL_REQUIRED,
    "OPTIONAL": trino.auth.GSSAPIAuthentication.MUTUAL_OPTIONAL,
    "DISABLED": trino.auth.GSSAPIAuthentication.MUTUAL_DISABLED,
}


@with_starburst_fields
class TrinoGssapiCredentials(TrinoCredentials):
    host: str
    port: Port
    user: str
    client_tags: Optional[List[str]] = None
    roles: Optional[Dict[str, str]] = None
    principal: Optional[str] = None
    krb5_config: Optional[str] = None
    service_name: Optional[str] = None
    # One of "REQUIRED", "OPTIONAL", "DISABLED" (case-insensitive). Defaults to
    # "DISABLED" to match trino-python-client's GSSAPIAuthentication default.
    mutual_authentication: str = "DISABLED"
    cert: Optional[Union[str, bool]] = None
    http_headers: Optional[Dict[str, str]] = None
    force_preemptive: Optional[bool] = False
    hostname_override: Optional[str] = None
    sanitize_mutual_error_response: Optional[bool] = True
    delegate: Optional[bool] = False
    session_properties: Dict[str, Any] = field(default_factory=dict)
    prepared_statements_enabled: bool = PREPARED_STATEMENTS_ENABLED_DEFAULT
    retries: Optional[int] = trino.constants.DEFAULT_MAX_ATTEMPTS
    timezone: Optional[str] = None
    suppress_cert_warning: Optional[bool] = None

    @property
    def http_scheme(self):
        return HttpScheme.HTTPS

    @property
    def method(self):
        return "gssapi"

    def trino_auth(self):
        return trino.auth.GSSAPIAuthentication(
            config=self.krb5_config,
            service_name=self.service_name,
            mutual_authentication=self._resolve_mutual_authentication(),
            force_preemptive=self.force_preemptive,
            hostname_override=self.hostname_override,
            sanitize_mutual_error_response=self.sanitize_mutual_error_response,
            principal=self.principal,
            delegate=self.delegate,
            ca_bundle=self.cert,
        )

    def _resolve_mutual_authentication(self) -> int:
        value = self.mutual_authentication.upper()
        try:
            return _GSSAPI_MUTUAL_AUTH_VALUES[value]
        except KeyError:
            raise DbtConfigError(
                f"Invalid mutual_authentication value {self.mutual_authentication!r}. "
                "Expected one of: REQUIRED, OPTIONAL, DISABLED."
            )


@with_starburst_fields
class TrinoJwtCredentials(TrinoCredentials):
    host: str
    port: Port
    jwt_token: str
    user: Optional[str] = None
    client_tags: Optional[List[str]] = None
    roles: Optional[Dict[str, str]] = None
    cert: Optional[Union[str, bool]] = None
    http_headers: Optional[Dict[str, str]] = None
    session_properties: Dict[str, Any] = field(default_factory=dict)
    prepared_statements_enabled: bool = PREPARED_STATEMENTS_ENABLED_DEFAULT
    retries: Optional[int] = trino.constants.DEFAULT_MAX_ATTEMPTS
    timezone: Optional[str] = None
    suppress_cert_warning: Optional[bool] = None

    @property
    def http_scheme(self):
        return HttpScheme.HTTPS

    @property
    def method(self):
        return "jwt"

    def trino_auth(self):
        return trino.auth.JWTAuthentication(self.jwt_token)


@with_starburst_fields
class TrinoOauthCredentials(TrinoCredentials):
    host: str
    port: Port
    user: Optional[str] = None
    client_tags: Optional[List[str]] = None
    roles: Optional[Dict[str, str]] = None
    cert: Optional[Union[str, bool]] = None
    http_headers: Optional[Dict[str, str]] = None
    session_properties: Dict[str, Any] = field(default_factory=dict)
    prepared_statements_enabled: bool = PREPARED_STATEMENTS_ENABLED_DEFAULT
    retries: Optional[int] = trino.constants.DEFAULT_MAX_ATTEMPTS
    timezone: Optional[str] = None
    # trino-python-client types this param as CompositeRedirectHandler, but
    # WebBrowserRedirectHandler is a sibling RedirectHandler subclass that works
    # identically at runtime; the stub is just overly narrow.
    OAUTH = trino.auth.OAuth2Authentication(
        redirect_auth_url_handler=trino.auth.WebBrowserRedirectHandler()  # type: ignore[arg-type]
    )
    suppress_cert_warning: Optional[bool] = None

    @property
    def http_scheme(self):
        return HttpScheme.HTTPS

    @property
    def method(self):
        return "oauth"

    def trino_auth(self):
        return self.OAUTH


@with_starburst_fields
class TrinoOauthConsoleCredentials(TrinoCredentials):
    host: str
    port: Port
    user: Optional[str] = None
    client_tags: Optional[List[str]] = None
    roles: Optional[Dict[str, str]] = None
    cert: Optional[Union[str, bool]] = None
    http_headers: Optional[Dict[str, str]] = None
    session_properties: Dict[str, Any] = field(default_factory=dict)
    prepared_statements_enabled: bool = PREPARED_STATEMENTS_ENABLED_DEFAULT
    retries: Optional[int] = trino.constants.DEFAULT_MAX_ATTEMPTS
    timezone: Optional[str] = None
    # trino-python-client types this param as CompositeRedirectHandler, but
    # ConsoleRedirectHandler is a sibling RedirectHandler subclass that works
    # identically at runtime; the stub is just overly narrow.
    OAUTH = trino.auth.OAuth2Authentication(
        redirect_auth_url_handler=trino.auth.ConsoleRedirectHandler()  # type: ignore[arg-type]
    )
    suppress_cert_warning: Optional[bool] = None

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
    behavior_flags = None

    def __init__(self, profile, mp_context, behavior_flags=None) -> None:
        super().__init__(profile, mp_context)

        TrinoConnectionManager.behavior_flags = behavior_flags

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

        # set default `cert` value, according to
        # require_certificate_validation behavior flag
        if credentials.cert is None:
            req_cert_val_flag = cls.behavior_flags.require_certificate_validation.setting
            if req_cert_val_flag:
                credentials.cert = True

        if credentials.suppress_cert_warning:
            import urllib3

            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        # A model may have set routing overrides before the connection was opened,
        # or the connection may be reopening in the middle of one.
        client_tags, http_headers = cls.effective_routing(credentials)

        # it's impossible for trino to fail here as 'connections' are actually
        # just cursor factories.
        trino_conn = trino.dbapi.connect(
            host=credentials.host,
            port=credentials.port,
            user=credentials.impersonation_user
            if getattr(credentials, "impersonation_user", None)
            else credentials.user,
            client_tags=client_tags,
            roles=credentials.roles,
            catalog=credentials.database,
            schema=credentials.schema,
            http_scheme=credentials.http_scheme.value,
            http_headers=http_headers,
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

    @staticmethod
    def effective_routing(credentials: TrinoCredentials):
        """Client tags and HTTP headers to use now: the profile's, unless the model
        currently running on this thread overrode them."""
        override = getattr(_query_overrides, "value", None)
        if override is None:
            return credentials.client_tags, credentials.http_headers
        return override

    def set_query_overrides(self, client_tags, http_headers) -> None:
        """Route the statements of the model that is about to run.

        Trino rebuilds the request headers from the client session for every
        statement, so mutating the session re-routes subsequent queries without
        reopening the connection.
        """
        _query_overrides.value = (client_tags, http_headers)
        self._apply_routing(client_tags, http_headers)

    def clear_query_overrides(self) -> None:
        """Restore the profile's routing once a model is done."""
        _query_overrides.value = None
        credentials = cast(TrinoCredentials, self.profile.credentials)
        self._apply_routing(credentials.client_tags, credentials.http_headers)

    def _apply_routing(self, client_tags, http_headers) -> None:
        connection = self.get_if_exists()
        if connection is None or connection.state != "open":
            # Nothing to update; `open` picks the override up from the thread.
            return

        trino_connection = connection.handle.handle
        session = trino_connection._client_session
        cached_headers = trino_connection._http_session.headers

        # trino-python-client copies the session headers into the shared
        # requests session every time a cursor is created, and requests keeps
        # sending them afterwards. Drop the ones we manage first, otherwise a
        # model inherits the routing of the model that ran before it.
        for header in set(session.headers) | {trino.constants.HEADER_CLIENT_TAGS}:
            cached_headers.pop(header, None)

        session.headers.clear()
        session.headers.update(http_headers or {})
        session.client_tags[:] = client_tags or []

    @classmethod
    def get_response(cls, cursor) -> TrinoAdapterResponse:
        code = cursor._cursor.update_type
        if code is None:
            code = "SUCCESS"

        rows_affected = cursor._cursor.rowcount
        if rows_affected == -1:
            message = f"{code}"
        else:
            message = f"{code} ({rows_affected:_} rows)"
        return TrinoAdapterResponse(
            _message=message,
            query=cursor._cursor.query,
            query_id=cursor._cursor.query_id,
            rows_affected=rows_affected,
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
