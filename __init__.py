# parseable_connector.py
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
import requests
import json
import sys
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler
from sqlalchemy import types
from sqlalchemy.engine import reflection
from sqlalchemy.engine.base import Connection
from sqlalchemy.engine.interfaces import Dialect
import base64

# DBAPI required attributes
apilevel = '2.0'
threadsafety = 1
paramstyle = 'named'

# DBAPI exceptions
class Error(Exception):
    pass

class InterfaceError(Error):
    pass

class DatabaseError(Error):
    pass

def parse_timestamp(timestamp_str: str) -> datetime:
    try:
        return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
    except ValueError:
        return None

class ParseableCursor:
    def __init__(self, connection):
        self.connection = connection
        self._rows = []
        self._rowcount = 0
        self.description = None
        self.arraysize = 1

    def execute(self, operation: str, parameters: Optional[Dict] = None):
        # Extract time range from query parameters if provided
        start_time = "10m"  # default
        end_time = "now"    # default
        
        if parameters and 'start_time' in parameters:
            start_time = parameters['start_time']
        if parameters and 'end_time' in parameters:
            end_time = parameters['end_time']

        # Prepare request
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Basic {self.connection.credentials}'
        }
        
        data = {
            'query': operation,
            'startTime': start_time,
            'endTime': end_time
        }


        # Log the request details
        print("Debug: Sending request to Parseable", file=sys.stderr)
        print(f"URL: {self.connection.host}/api/v1/query", file=sys.stderr)
        print(f"Headers: {headers}", file=sys.stderr)
        print(f"Payload: {json.dumps(data, indent=2)}", file=sys.stderr)


        # Make request to Parseable
        response = requests.post(
            f"{self.connection.host}/api/v1/query",
            headers=headers,
            json=data
        )

        print(f"Response Status: {response.status_code}", file=sys.stderr)
        print(f"Response Content: {response.text}", file=sys.stderr)

        if response.status_code != 200:
            raise DatabaseError(f"Query failed: {response.text}")

        # Process response
        result = response.json()
        
        if not result:
            self._rows = []
            self._rowcount = 0
            self.description = None
            return

        # Set up column descriptions (required for DBAPI compliance)
        if result and len(result) > 0:
            first_row = result[0]
            self.description = []
            for column_name in first_row.keys():
                # (name, type_code, display_size, internal_size, precision, scale, null_ok)
                self.description.append((column_name, None, None, None, None, None, None))

        self._rows = result
        self._rowcount = len(result)

    def executemany(self, operation: str, seq_of_parameters: List[Dict]):
        raise NotImplementedError("executemany is not supported")

    def fetchall(self) -> List[Tuple]:
        return [tuple(row.values()) for row in self._rows]

    def fetchone(self) -> Optional[Tuple]:
        if not self._rows:
            return None
        return tuple(self._rows.pop(0).values())

    def fetchmany(self, size: Optional[int] = None) -> List[Tuple]:
        if size is None:
            size = self.arraysize
        result = self._rows[:size]
        self._rows = self._rows[size:]
        return [tuple(row.values()) for row in result]

    @property
    def rowcount(self) -> int:
        return self._rowcount

    def close(self):
        self._rows = []

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass

class ParseableConnection:
    def __init__(self, host: str, port: str, username: str, password: str):
        self.host = f"http://{host}:{port}".rstrip('/')
        credentials = f"{username}:{password}"
        self.credentials = base64.b64encode(credentials.encode()).decode()

    def cursor(self):
        return ParseableCursor(self)

    def close(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

def connect(username: Optional[str] = None, 
           password: Optional[str] = None, 
           host: Optional[str] = None,
           port: Optional[str] = None,
           **kwargs) -> ParseableConnection:
    """
    Connect to a Parseable instance.
    
    :param username: Username for authentication (default: admin)
    :param password: Password for authentication (default: admin)
    :param host: Host address (default: localhost)
    :param port: Port number (default: 8000)
    :return: ParseableConnection object
    """
    username = username or 'admin'
    password = password or 'admin'
    host = host or 'localhost'
    port = port or '8000'
    
    return ParseableConnection(host=host, port=port, username=username, password=password)

# SQLAlchemy dialect
class ParseableCompiler(compiler.SQLCompiler):
    def visit_select(self, select, **kwargs):
        return super().visit_select(select, **kwargs)

class ParseableDialect(default.DefaultDialect):
    name = 'parseable'
    driver = 'rest'
    
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_native_boolean = True

    @classmethod
    def dbapi(cls):
        return sys.modules[__name__]

    def create_connect_args(self, url):
        kwargs = {
            'host': url.host or 'localhost',
            'port': str(url.port or 8000),
            'username': url.username or 'admin',
            'password': url.password or 'admin'
        }
        return [], kwargs

    def do_ping(self, dbapi_connection):
        try:
            cursor = dbapi_connection.cursor()
            cursor.execute('SELECT * FROM "adheip" LIMIT 1')
            return True
        except:
            return False

    def get_columns(self, connection: Connection, table_name: str, schema: Optional[str] = None, **kw) -> List[Dict]:
        return [
            {
                'name': 'timestamp',
                'type': types.TIMESTAMP(),
                'nullable': True,
                'default': None,
            },
            {
                'name': 'message',
                'type': types.String(),
                'nullable': True,
                'default': None,
            }
        ]

    def get_table_names(self, connection: Connection, schema: Optional[str] = None, **kw) -> List[str]:
        return ["adheip"]

    def get_view_names(self, connection: Connection, schema: Optional[str] = None, **kw) -> List[str]:
        return []

    def get_schema_names(self, connection: Connection, **kw) -> List[str]:
        return ['default']

    def get_pk_constraint(self, connection: Connection, table_name: str, schema: Optional[str] = None, **kw) -> Dict[str, Any]:
        return {'constrained_columns': [], 'name': None}

    def get_foreign_keys(self, connection: Connection, table_name: str, schema: Optional[str] = None, **kw) -> List[Dict[str, Any]]:
        return []

    def get_indexes(self, connection: Connection, table_name: str, schema: Optional[str] = None, **kw) -> List[Dict[str, Any]]:
        return []

    def do_rollback(self, dbapi_connection):
        pass

    def _check_unicode_returns(self, connection: Connection, additional_tests: Optional[List] = None):
        pass

    def _check_unicode_description(self, connection: Connection):
        pass
    