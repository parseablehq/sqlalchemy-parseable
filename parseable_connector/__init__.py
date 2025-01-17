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

    def _extract_and_remove_time_conditions(self, query: str) -> Tuple[str, str, str]:
        """
        Extract time conditions from WHERE clause and remove them from query.
        Returns (modified_query, start_time, end_time)
        """
        import re
        
        # Default values
        start_time = None
        end_time = None
        modified_query = query

        # Find timestamp conditions in WHERE clause
        timestamp_pattern = r"WHERE\s+p_timestamp\s*>=\s*'([^']+)'\s*AND\s+p_timestamp\s*<\s*'([^']+)'"
        match = re.search(timestamp_pattern, query, re.IGNORECASE)
        
        if match:
            # Extract the timestamps
            start_time = match.group(1)
            end_time = match.group(2)
            
            # Convert to Parseable format (adding Z for UTC)
            start_time = start_time.replace(' ', 'T') + 'Z'
            end_time = end_time.replace(' ', 'T') + 'Z'
            
            # Remove the WHERE clause with timestamp conditions
            where_clause = match.group(0)
            modified_query = query.replace(where_clause, '')
            
            # If there's a WHERE clause with other conditions, preserve them
            if 'WHERE' in modified_query.upper():
                modified_query = modified_query.replace('AND', 'WHERE', 1)
        
        return modified_query.strip(), start_time, end_time

    def execute(self, operation: str, parameters: Optional[Dict] = None):
        # Extract and remove time conditions from query
        modified_query, start_time, end_time = self._extract_and_remove_time_conditions(operation)
        
        # Use extracted times or defaults
        start_time = start_time or "10m"
        end_time = end_time or "now"

        # Log the transformation
        print("Debug: Query transformation", file=sys.stderr)
        print(f"Original query: {operation}", file=sys.stderr)
        print(f"Modified query: {modified_query}", file=sys.stderr)
        print(f"Time range: {start_time} to {end_time}", file=sys.stderr)

        # Prepare request
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Basic {self.connection.credentials}'
        }
        
        data = {
            'query': modified_query,
            'startTime': start_time,
            'endTime': end_time
        }

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

        # Set up column descriptions
        if result and len(result) > 0:
            first_row = result[0]
            self.description = []
            for column_name in first_row.keys():
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
    def visit_table(self, table, asfrom=False, iscrud=False, ashint=False, fromhints=None, **kwargs):
        # Get the original table representation
        text = super().visit_table(table, asfrom, iscrud, ashint, fromhints, **kwargs)
        
        # Remove schema prefix (anything before the dot)
        if '.' in text:
            return text.split('.')[-1]
        return text 

class ParseableDialect(default.DefaultDialect):
    name = 'parseable'
    driver = 'rest'
    statement_compiler = ParseableCompiler
    
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
        try:
            # Get host and credentials from the connection object
            host = connection.engine.url.host
            port = connection.engine.url.port
            username = connection.engine.url.username
            password = connection.engine.url.password
            base_url = f"http://{host}:{port}"
            
            # Prepare the headers for authorization
            credentials = f"{username}:{password}"
            encoded_credentials = base64.b64encode(credentials.encode()).decode()
            headers = {
                'Authorization': f'Basic {encoded_credentials}',
            }
            
            # Fetch the schema for the given table (log stream)
            response = requests.get(f"{base_url}/api/v1/logstream/{table_name}/schema", headers=headers)
            
            # Log the response details for debugging
            print(f"Debug: Fetching schema for {table_name} from {base_url}/api/v1/logstream/{table_name}/schema", file=sys.stderr)
            print(f"Response Status: {response.status_code}", file=sys.stderr)
            print(f"Response Content: {response.text}", file=sys.stderr)
            
            if response.status_code != 200:
                raise DatabaseError(f"Failed to fetch schema for {table_name}: {response.text}")
            
            # Parse the schema response
            schema_data = response.json()
            
            if not isinstance(schema_data, dict) or 'fields' not in schema_data:
                raise DatabaseError(f"Unexpected schema format for {table_name}: {response.text}")
            
            columns = []
            
            # Map each field to a SQLAlchemy column descriptor
            for field in schema_data['fields']:
                column_name = field['name']
                data_type = field['data_type']
                nullable = field['nullable']
                
                # Map Parseable data types to SQLAlchemy types
                if data_type == 'Utf8':
                    sql_type = types.String()
                elif data_type == 'Int64':
                    sql_type = types.BigInteger()
                elif data_type == 'Float64':
                    sql_type = types.Float()
                else:
                    sql_type = types.String()  # Default type if unknown
                
                # Append column definition to columns list
                columns.append({
                    'name': column_name,
                    'type': sql_type,
                    'nullable': nullable,
                    'default': None,  # Assuming no default for now, adjust as needed
                })
            
            return columns
        
        except Exception as e:
            raise DatabaseError(f"Error fetching columns for {table_name}: {str(e)}")


    def get_table_names(self, connection: Connection, schema: Optional[str] = None, **kw) -> List[str]:
        """
        Fetch the list of log streams (tables) from the Parseable instance.

        :param connection: SQLAlchemy Connection object.
        :param schema: Optional schema (not used for Parseable).
        :param kw: Additional keyword arguments.
        :return: List of table names (log streams).
        """
        try:
            # Get host and credentials from the connection object
            host = connection.engine.url.host
            port = connection.engine.url.port
            username = connection.engine.url.username
            password = connection.engine.url.password
            base_url = f"http://{host}:{port}"
            
            # Prepare the headers
            credentials = f"{username}:{password}"
            encoded_credentials = base64.b64encode(credentials.encode()).decode()
            headers = {
                'Authorization': f'Basic {encoded_credentials}',
            }
            
            # Make the GET request
            response = requests.get(f"{base_url}/api/v1/logstream", headers=headers)
            
            # Log the response details for debugging
            print(f"Debug: Fetching table names from {base_url}/api/v1/logstream", file=sys.stderr)
            print(f"Response Status: {response.status_code}", file=sys.stderr)
            print(f"Response Content: {response.text}", file=sys.stderr)
            
            if response.status_code != 200:
                raise DatabaseError(f"Failed to fetch table names: {response.text}")
            
            # Parse the response JSON
            log_streams = response.json()
            if not isinstance(log_streams, list):
                raise DatabaseError(f"Unexpected response format: {response.text}")
            
            # Extract table names (log stream names)
            return [stream['name'] for stream in log_streams if 'name' in stream]
        except Exception as e:
            raise DatabaseError(f"Error fetching table names: {str(e)}")

    def has_table(self, connection: Connection, table_name: str, schema: Optional[str] = None, **kw) -> bool:
        """
        Check if a table (log stream) exists in Parseable.
        
        :param connection: SQLAlchemy Connection object
        :param table_name: Name of the table (log stream) to check
        :param schema: Schema name (not used for Parseable)
        :return: True if the table exists, False otherwise
        """
        try:
            # Get connection details
            host = connection.engine.url.host
            port = connection.engine.url.port
            username = connection.engine.url.username
            password = connection.engine.url.password
            base_url = f"http://{host}:{port}"
            
            # Prepare headers
            credentials = f"{username}:{password}"
            encoded_credentials = base64.b64encode(credentials.encode()).decode()
            headers = {
                'Authorization': f'Basic {encoded_credentials}',
            }
            
            # Make request to list log streams
            response = requests.get(f"{base_url}/api/v1/logstream", headers=headers)
            
            if response.status_code != 200:
                return False
                
            log_streams = response.json()
            
            # Check if the table name exists in the list of log streams
            return any(stream['name'] == table_name for stream in log_streams if 'name' in stream)
            
        except Exception as e:
            print(f"Error checking table existence: {str(e)}", file=sys.stderr)
            return False

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
