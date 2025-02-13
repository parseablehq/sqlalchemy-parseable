from __future__ import absolute_import
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
from urllib.parse import urlparse

# DBAPI required attributes
apilevel = '2.0'
threadsafety = 1
paramstyle = 'named'

class Error(Exception):
    pass

class InterfaceError(Error):
    pass

class DatabaseError(Error):
    pass

class ParseableClient:
    def __init__(self, host: str, port: str, username: str, password: str, verify_ssl: bool = True, use_https: bool = True):
        # Strip any existing protocol
        host = host.replace('https://', '').replace('http://', '')
        
        # Construct base URL with appropriate protocol
        protocol = 'https' if use_https else 'http'
        self.base_url = f"{protocol}://{host}"
        
        # Add port if specified and not default
        if port:
            if (use_https and port != '443') or (not use_https and port != '80'):
                self.base_url += f":{port}"
        
        credentials = f"{username}:{password}"
        self.headers = {
            'Authorization': f'Basic {base64.b64encode(credentials.encode()).decode()}',
            'Content-Type': 'application/json'
        }
        self.verify_ssl = verify_ssl if use_https else False
        self.timeout = 300  # Default timeout of 300 seconds

    def _make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        url = f"{self.base_url}/api/v1/{endpoint.lstrip('/')}"
        kwargs['headers'] = {**self.headers, **kwargs.get('headers', {})}
        kwargs['verify'] = self.verify_ssl
        kwargs['timeout'] = kwargs.get('timeout', self.timeout)
        
        try:
            response = requests.request(method, url, **kwargs)
            print(f"Debug: {method} request to {url}", file=sys.stderr)
            print(f"Response Status: {response.status_code}", file=sys.stderr)
            print(f"Response Content: {response.text}", file=sys.stderr)
            
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            raise DatabaseError(f"Request failed: {str(e)}")

    def get_logstreams(self) -> requests.Response:
        """Get list of all logstreams"""
        return self._make_request('GET', 'logstream')

    def get_schema(self, table_name: str) -> requests.Response:
        """Get schema for a table/stream"""
        escaped_table_name = self._escape_table_name(table_name)
        return self._make_request('GET', f'logstream/{table_name}/schema')

    def _escape_table_name(self, table_name: str) -> str:
        """Escape table name to handle special characters"""
        if '-' in table_name or ' ' in table_name or '.' in table_name:
            return f'"{table_name}"'
        return table_name

    def _transform_query(self, query: str) -> str:
        """Transform the query to handle type casting and add default limit"""
        import re
        
        # Convert avg, sum, count on string fields
        numeric_agg_pattern = r'(AVG|SUM|COUNT)\s*\(([^)]+)\)'
        def replace_agg(match):
            agg_func = match.group(1).upper()
            field = match.group(2).strip()
            
            if agg_func in ('AVG', 'SUM'):
                return f"{agg_func}(TRY_CAST({field} AS DOUBLE))"
            return f"{agg_func}({field})"
        
        modified_query = re.sub(numeric_agg_pattern, replace_agg, query, flags=re.IGNORECASE)
        
        # Check if query already has a LIMIT clause
        limit_pattern = r'\bLIMIT\s+(\d+)\b'
        limit_match = re.search(limit_pattern, modified_query, re.IGNORECASE)
        
        # Remove any existing LIMIT clause
        if limit_match:
            current_limit = int(limit_match.group(1))
            modified_query = re.sub(limit_pattern, '', modified_query, flags=re.IGNORECASE)
        else:
            current_limit = None

        # Add our limit (either 100 or the original if it was smaller)
        if current_limit is None or current_limit > 100:
            current_limit = 100

        # Add LIMIT at the end
        modified_query = modified_query.strip() + f" LIMIT {current_limit}"
        
        return modified_query

    def execute_query(self, table_name: str, query: str) -> Dict:
        """Execute a query against a specific table/stream"""
        modified_query = self._transform_query(query)
        modified_query, start_time, end_time = self._extract_and_remove_time_conditions(modified_query)
        
        if not (modified_query.find(f'"{table_name}"') >= 0):
            escaped_table_name = self._escape_table_name(table_name)
            modified_query = modified_query.replace(table_name, escaped_table_name)
        
        data = {
            "query": modified_query,
            "startTime": start_time,
            "endTime": end_time
        }
        
        headers = {**self.headers, 'X-P-Stream': table_name}
        url = f"{self.base_url}/api/v1/query"
        
        print("\n=== QUERY EXECUTION ===", file=sys.stderr)
        print(f"Table: {table_name}", file=sys.stderr)
        print(f"Original Query: {query}", file=sys.stderr)
        print(f"Modified Query: {modified_query}", file=sys.stderr)
        print(f"Time Range: {start_time} to {end_time}", file=sys.stderr)
        
        try:
            response = requests.post(
                url,
                headers=headers,
                json=data,
                verify=self.verify_ssl,
                timeout=self.timeout
            )
            
            print("\n=== QUERY RESPONSE ===", file=sys.stderr)
            print(f"Status Code: {response.status_code}", file=sys.stderr)
            print(f"Headers: {json.dumps(dict(response.headers), indent=2)}", file=sys.stderr)
            print(f"Content: {response.text[:1000]}{'...' if len(response.text) > 1000 else ''}", file=sys.stderr)
            print("=====================\n", file=sys.stderr)
            
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"\n=== QUERY ERROR ===\n{str(e)}\n================\n", file=sys.stderr)
            raise DatabaseError(f"Query execution failed: {str(e)}")

    def _get_time_grain_expressions(self) -> Dict[str, str]:
        """Time grain expressions for Parseable."""
        return {
            None: "{col}",
            "second": "date_trunc('second', {col})",
            "minute": "date_trunc('minute', {col})",
            "hour": "date_trunc('hour', {col})",
            "day": "date_trunc('day', {col})",
            "week": "date_trunc('week', {col})",
            "month": "date_trunc('month', {col})",
            "quarter": "date_trunc('quarter', {col})",
            "year": "date_trunc('year', {col})"
        }

    def _handle_epoch_timestamps(self, col: str, unit: str = 'ms') -> str:
        """Convert epoch timestamps to datetime."""
        if unit == 'ms':
            return f"to_timestamp({col} / 1000)"
        return f"to_timestamp({col})"

    def convert_timestamp(self, dttm: datetime) -> str:
        """Convert Python datetime to Parseable timestamp string."""
        return f"'{dttm.strftime('%Y-%m-%dT%H:%M:%S.000')}'"

    def _extract_and_remove_time_conditions(self, query: str) -> Tuple[str, str, str]:
        """Extract time conditions from WHERE clause and remove them from query."""
        import re
        
        timestamp_pattern = r"WHERE\s+p_timestamp\s*>=\s*'([^']+)'\s*AND\s+p_timestamp\s*<\s*'([^']+)'"
        match = re.search(timestamp_pattern, query, re.IGNORECASE)
        
        if match:
            start_str = match.group(1).replace(' ', 'T') + 'Z'
            end_str = match.group(2).replace(' ', 'T') + 'Z'
            
            where_clause = match.group(0)
            modified_query = query.replace(where_clause, '')
            
            if 'WHERE' in modified_query.upper():
                modified_query = modified_query.replace('AND', 'WHERE', 1)
                
            return modified_query.strip(), start_str, end_str
        
        return query.strip(), "10m", "now"

class ParseableCursor:
    def __init__(self, connection):
        self.connection = connection
        self._rows = []
        self._rowcount = -1
        self.description = None
        self.arraysize = 1

    def execute(self, operation: str, parameters: Optional[Dict] = None):
        if not self.connection.table_name:
            raise DatabaseError("No table name specified in connection string")
        
        try:
            if operation.strip().upper() == "SELECT 1":
                result = self.connection.client.execute_query(
                    table_name=self.connection.table_name,
                    query=f"select * from {self.connection.table_name} limit 1"
                )
                self._rows = [{"result": 1}]
                self._rowcount = 1
                self.description = [("result", types.INTEGER, None, None, None, None, None)]
                return self._rowcount
            
            result = self.connection.client.execute_query(
                table_name=self.connection.table_name,
                query=operation
            )
            
            if result and isinstance(result, list):
                self._rows = result
                self._rowcount = len(result)
                
                if self._rows:
                    first_row = self._rows[0]
                    self.description = [
                        (col, types.VARCHAR, None, None, None, None, None)
                        for col in first_row.keys()
                    ]
            
            return self._rowcount
            
        except Exception as e:
            raise DatabaseError(str(e))

    def fetchone(self) -> Optional[Tuple]:
        if not self._rows:
            return None
        return tuple(self._rows.pop(0).values())

    def fetchall(self) -> List[Tuple]:
        result = [tuple(row.values()) for row in self._rows]
        self._rows = []
        return result

    def close(self):
        self._rows = []

class ParseableConnection:
    def __init__(self, host: str, port: str, username: str, password: str, database: str = None, 
                 verify_ssl: bool = True, use_https: bool = True):
        self.client = ParseableClient(
            host=host, 
            port=port, 
            username=username, 
            password=password, 
            verify_ssl=verify_ssl,
            use_https=use_https
        )
        self._closed = False
        self.table_name = database.lstrip('/') if database else None

    def cursor(self):
        if self._closed:
            raise InterfaceError("Connection is closed")
        return ParseableCursor(self)

    def close(self):
        self._closed = True

    def commit(self):
        pass

    def rollback(self):
        pass

class ParseableCompiler(compiler.SQLCompiler):
    def visit_table(self, table, asfrom=False, iscrud=False, ashint=False, fromhints=None, **kwargs):
        text = super().visit_table(table, asfrom, iscrud, ashint, fromhints, **kwargs)
        return text.split('.')[-1] if '.' in text else text

class ParseableDialect(default.DefaultDialect):
    name = 'parseable'
    driver = 'rest'
    statement_compiler = ParseableCompiler
    
    # Basic dialect properties
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_native_boolean = True
    supports_multivalues_insert = False
    
    # Read-only properties
    supports_statement_cache = True
    is_readonly = True
    supports_schemas = False
    postfetch_lastrowid = False
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    supports_default_metavalue = False

    @classmethod
    def dbapi(cls):
        return sys.modules[__name__]

    def _get_server_version_info(self, connection):
        return (1, 0, 0)

    def create_connect_args(self, url):
        table_name = url.database if url.database else None
        
        # Determine protocol from URL scheme and port
        if '+' in url.drivername:
            base, protocol = url.drivername.split('+')
            use_https = protocol.lower() == 'https'
        else:
            # If port is 80, use HTTP regardless of default
            use_https = False if url.port == 80 else True
        
        # Set default ports based on protocol
        default_port = '443' if use_https else '80'
        
        kwargs = {
            'host': url.host or 'localhost',
            'port': str(url.port or default_port),
            'username': url.username or 'admin',
            'password': url.password or 'admin',
            'verify_ssl': use_https,  # Only verify SSL if using HTTPS
            'use_https': use_https,
            'database': table_name
        }
        print(f"\n=== CONNECTION ARGS ===\nProtocol: {'HTTPS' if use_https else 'HTTP'}\nPort: {kwargs['port']}\nSSL Verify: {kwargs['verify_ssl']}\n=====================\n", file=sys.stderr)
        return [], kwargs

    def _check_unicode_returns(self, connection, additional_tests=None):
        return True
    
    def _check_unicode_description(self, connection):
        return True

    def do_execute(self, cursor, statement, parameters, context=None):
        """Execute a statement with parameters."""
        import sqlparse
        
        # Parse and validate the SQL statement
        parsed = sqlparse.parse(statement)
        if not parsed:
            raise DatabaseError("Empty SQL statement")
            
        stmt = parsed[0]
        stmt_type = stmt.get_type().upper()
        
        # Only allow SELECT statements
        if stmt_type not in ('SELECT', 'UNKNOWN'):
            raise DatabaseError(f"Only SELECT statements are allowed. Got: {stmt_type}")
            
        # For UNKNOWN type, check if it starts with SELECT
        if stmt_type == 'UNKNOWN' and not statement.strip().upper().startswith('SELECT'):
            raise DatabaseError("Only SELECT statements are allowed")
            
        return cursor.execute(statement, parameters)

    def do_ping(self, dbapi_connection):
        try:
            cursor = dbapi_connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            return True
        except Exception:
            return False

    def get_table_names(self, connection: Connection, schema: Optional[str] = None, **kw) -> List[str]:
        """Get table name from connection string"""
        table_name = connection.connection.table_name
        if table_name:
            return [table_name]
        return []

    def get_view_names(self, connection: Connection, schema: Optional[str] = None, **kw) -> List[str]:
        """Get view names"""
        return []

    def has_table(self, connection: Connection, table_name: str, schema: Optional[str] = None, **kw) -> bool:
        """Check if table exists - always return True for the table name in connection string"""
        return table_name == connection.connection.table_name

    def get_columns(self, connection: Connection, table_name: str, schema: Optional[str] = None, **kw) -> List[Dict]:
        try:
            # Remove schema prefix if present
            if '.' in table_name:
                schema, table_name = table_name.split('.')
            
            response = connection.connection.client.get_schema(table_name)
            
            if response.status_code != 200:
                raise DatabaseError(f"Failed to fetch schema for {table_name}: {response.text}")
            
            schema_data = response.json()
            
            if not isinstance(schema_data, dict) or 'fields' not in schema_data:
                raise DatabaseError(f"Unexpected schema format for {table_name}: {response.text}")
            
            columns = []
            type_map = {
                'Utf8': types.String(),
                'Int64': types.BigInteger(),
                'Float64': types.Float()
            }
            
            for field in schema_data['fields']:
                data_type = field['data_type']
                if isinstance(data_type, dict):
                    if 'Timestamp' in data_type:
                        sql_type = types.TIMESTAMP()
                    else:
                        sql_type = types.String()
                else:
                    sql_type = type_map.get(data_type, types.String())
                
                columns.append({
                    'name': field['name'],
                    'type': sql_type,
                    'nullable': field['nullable'],
                    'default': None
                })
            
            return columns
        
        except Exception as e:
            raise DatabaseError(f"Error fetching columns for {table_name}: {str(e)}")

    def get_schema_names(self, connection: Connection, **kw) -> List[str]:
        return ['default']
    
    def get_pk_constraint(self, connection: Connection, table_name: str, schema: Optional[str] = None, **kw) -> Dict[str, Any]:
        return {'constrained_columns': [], 'name': None}

    def get_foreign_keys(self, connection: Connection, table_name: str, schema: Optional[str] = None, **kw) -> List[Dict[str, Any]]:
        return []

    def get_indexes(self, connection: Connection, table_name: str, schema: Optional[str] = None, **kw) -> List[Dict[str, Any]]:
        return []

    def get_isolation_level(self, connection):
        """Return AUTOCOMMIT as isolation level."""
        return 'AUTOCOMMIT'

    def do_rollback(self, dbapi_connection):
        """No-op for rollback in read-only mode."""
        pass

    def do_commit(self, dbapi_connection):
        """No-op for commit in read-only mode."""
        pass

    def do_terminate(self, dbapi_connection):
        """Close the connection."""
        dbapi_connection.close()

    def has_sequence(self, connection, sequence_name, schema=None):
        return False

    @classmethod
    def get_sync_status(cls):
        return True

def connect(*args, **kwargs):
    """Connect to a Parseable database."""
    return ParseableConnection(
        host=kwargs.get('host', 'localhost'),
        port=str(kwargs.get('port', '80')),
        username=kwargs.get('username', 'admin'),
        password=kwargs.get('password', 'admin'),
        database=kwargs.get('database'),
        verify_ssl=kwargs.get('verify_ssl', True),
        use_https=kwargs.get('use_https', True)
)

# Export the connect function at module level
__all__ = ['ParseableDialect', 'connect', 'Error', 'DatabaseError', 'InterfaceError']

# Register dialects
from sqlalchemy.dialects import registry
registry.register("parseable", "parseable_connector.parseable_dialect", "ParseableDialect")
registry.register("parseable.http", "parseable_connector.parseable_dialect", "ParseableDialect")
registry.register("parseable.https", "parseable_connector.parseable_dialect", "ParseableDialect")