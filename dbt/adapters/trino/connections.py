from contextlib import contextmanager

import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import AdapterResponse
from dbt.events import AdapterLogger

from dataclasses import dataclass
from typing import Any, Optional, Dict
from dbt.helper_types import Port

from datetime import date, datetime
import decimal
import re
import trino
from trino.transaction import IsolationLevel
import sqlparse


logger = AdapterLogger("Trino")


@dataclass
class TrinoCredentials(Credentials):
    host: str
    port: Port
    user: str
    password: Optional[str] = None
    method: Optional[str] = None
    http_headers: Optional[Dict[str, str]] = None
    http_scheme: Optional[str] = None
    session_properties: Optional[Dict[str, Any]] = None
    _ALIASES = {"catalog": "database"}

    @property
    def type(self):
        return "trino"

    @property
    def unique_field(self):
        return self.host

    def _connection_keys(self):
        return ("host", "port", "user", "database", "schema")


class ConnectionWrapper(object):
    """Wrap a Trino connection in a way that accomplishes two tasks:

    - prefetch results from execute() calls so that trino calls actually
        persist to the db but then present the usual cursor interface
    - provide `cancel()` on the same object as `commit()`/`rollback()`/...

    """

    def __init__(self, handle):
        self.handle = handle
        self._cursor = None
        self._fetch_result = None

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
        self.handle.commit()

    def rollback(self):
        self.handle.rollback()

    def start_transaction(self):
        self.handle.start_transaction()

    def fetchall(self):
        if self._cursor is None:
            return None

        if self._fetch_result is not None:
            ret = self._fetch_result
            self._fetch_result = None
            return ret

        return None

    def execute(self, sql, bindings=None):

        if bindings is not None:
            # trino doesn't actually pass bindings along so we have to do the
            # escaping and formatting ourselves
            bindings = tuple(self._escape_value(b) for b in bindings)
            sql = sql % bindings

        result = self._cursor.execute(sql)
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


class TrinoConnectionManager(SQLConnectionManager):
    TYPE = "trino"

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield
        # TODO: introspect into `DatabaseError`s and expose `errorName`,
        # `errorType`, etc instead of stack traces full of garbage!
        except Exception as exc:
            logger.debug("Error while running:\n{}".format(sql))
            logger.debug(exc)
            raise dbt.exceptions.RuntimeException(str(exc))

    def add_begin_query(self):
        connection = self.get_thread_connection()
        with self.exception_handler("handle.start_transaction()"):
            connection.handle.start_transaction()

    def add_commit_query(self):
        connection = self.get_thread_connection()
        with self.exception_handler("handle.commit()"):
            connection.handle.commit()

    @classmethod
    def open(cls, connection):
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = connection.credentials
        if credentials.method == "ldap":
            auth = trino.auth.BasicAuthentication(
                credentials.user,
                credentials.password,
            )
            if credentials.http_scheme and credentials.http_scheme != "https":
                raise dbt.exceptions.RuntimeException(
                    "http_scheme must be set to 'https' for 'ldap' method."
                )
            http_scheme = "https"
        elif credentials.method == "kerberos":
            auth = trino.auth.KerberosAuthentication()
            if credentials.http_scheme and credentials.http_scheme != "https":
                raise dbt.exceptions.RuntimeException(
                    "http_scheme must be set to 'https' for 'kerberos' method."
                )
            http_scheme = "https"
        else:
            auth = trino.constants.DEFAULT_AUTH
            http_scheme = credentials.http_scheme or "http"

        # it's impossible for trino to fail here as 'connections' are actually
        # just cursor factories.
        trino_conn = trino.dbapi.connect(
            host=credentials.host,
            port=credentials.port,
            user=credentials.user,
            catalog=credentials.database,
            schema=credentials.schema,
            http_scheme=http_scheme,
            http_headers=credentials.http_headers,
            session_properties=credentials.session_properties,
            auth=auth,
            isolation_level=IsolationLevel.AUTOCOMMIT,
            source="dbt-trino",
        )
        connection.state = "open"
        connection.handle = ConnectionWrapper(trino_conn)
        return connection

    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:
        message = "SUCCESS"
        return AdapterResponse(_message=message)

    def cancel(self, connection):
        connection.handle.cancel()

    def add_query(self, sql, auto_begin=True,
                  bindings=None, abridge_sql_log=False):

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
            raise dbt.exceptions.RuntimeException(
                "Tried to run an empty query on model '{}'. If you are "
                "conditionally running\nsql, eg. in a model hook, make "
                "sure your `else` clause contains valid sql!\n\n"
                "Provided SQL:\n{}".format(connection.name, sql)
            )

        return connection, cursor

    def execute(self, sql, auto_begin=False, fetch=False):
        _, cursor = self.add_query(sql, auto_begin)
        status = self.get_response(cursor)
        table = self.get_result_from_cursor(cursor)
        return status, table
