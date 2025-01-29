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

def parse_timestamp(timestamp_str: str) -> datetime:
    try:
        return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
    except ValueError:
        return None

class ParseableClient:
    def __init__(self, host: str, port: str, username: str, password: str, verify_ssl: bool = True):
        # Remove https:// if included in host
        host = host.replace('https://', '')
        self.base_url = f"https://{host}"
        if port and port != '443':
            self.base_url += f":{port}"
        
        credentials = f"{username}:{password}"
        self.headers = {
            'Authorization': f'Basic {base64.b64encode(credentials.encode()).decode()}',
            'Content-Type': 'application/json'
        }
        self.verify_ssl = verify_ssl

    def _make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        url = f"{self.base_url}/api/v1/{endpoint.lstrip('/')}"
        kwargs['headers'] = {**self.headers, **kwargs.get('headers', {})}
        kwargs['verify'] = self.verify_ssl
        
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
        # Handle table names with special characters
        if '-' in table_name or ' ' in table_name or '.' in table_name:
            return f'"{table_name}"'
        return table_name

    # In ParseableClient class:
    def execute_query(self, table_name: str, query: str) -> Dict:
        """Execute a query against a specific table/stream"""
        # First, let's transform the query to handle type casting
        modified_query = self._transform_query(query)
        
        # Then extract time conditions
        modified_query, start_time, end_time = self._extract_and_remove_time_conditions(modified_query)
        
        # Escape table name in query if needed, but only if it's not already escaped
        if not (modified_query.find(f'"{table_name}"') >= 0):
            escaped_table_name = self._escape_table_name(table_name)
            modified_query = modified_query.replace(table_name, escaped_table_name)
        
        data = {
            "query": modified_query,
            "startTime": start_time,
            "endTime": end_time
        }
        
        headers = {**self.headers, 'X-P-Stream': table_name}  # Keep original table name in header
        
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
                verify=self.verify_ssl
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

    def _transform_query(self, query: str) -> str:
        """Transform the query to handle proper type casting"""
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
        
        return modified_query

    def _extract_and_remove_time_conditions(self, query: str) -> Tuple[str, str, str]:
        """Extract time conditions from WHERE clause and remove them from query."""
        import re
        
        timestamp_pattern = r"WHERE\s+p_timestamp\s*>=\s*'([^']+)'\s*AND\s+p_timestamp\s*<\s*'([^']+)'"
        match = re.search(timestamp_pattern, query, re.IGNORECASE)
        
        if match:
            # Convert to proper RFC3339 format
            start_str = match.group(1).replace(' ', 'T') + 'Z'
            end_str = match.group(2).replace(' ', 'T') + 'Z'
            
            # Remove the time conditions from query
            where_clause = match.group(0)
            modified_query = query.replace(where_clause, '')
            
            if 'WHERE' in modified_query.upper():
                modified_query = modified_query.replace('AND', 'WHERE', 1)
                
            return modified_query.strip(), start_str, end_str
        
        # Default values if no time conditions found
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
                # For connection test, execute a real query to test API connectivity
                # Don't escape the table name here since execute_query will handle it
                result = self.connection.client.execute_query(
                    table_name=self.connection.table_name,
                    query=f"select * from {self.connection.table_name} limit 1"
                )
                self._rows = [{"result": 1}]
                self._rowcount = 1
                self.description = [("result", types.INTEGER, None, None, None, None, None)]
                return self._rowcount
            
            # Handle actual queries
            result = self.connection.client.execute_query(
                table_name=self.connection.table_name,
                query=operation
            )
            
            if result and isinstance(result, list):
                self._rows = result
                self._rowcount = len(result)
                
                # Set description based on the first row if available
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
    def __init__(self, host: str, port: str, username: str, password: str, database: str = None, verify_ssl: bool = True):
        self.client = ParseableClient(host, port, username, password, verify_ssl)
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
        # Parse the database name from the URL path - this will be our table name
        table_name = url.database if url.database else None
        
        kwargs = {
            'host': url.host or 'localhost',
            'port': str(url.port or 443),
            'username': url.username or 'admin',
            'password': url.password or 'admin',
            'verify_ssl': True,
            'database': table_name  # This will be used as the table name
        }
        return [], kwargs

    def do_ping(self, dbapi_connection):
        try:
            cursor = dbapi_connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            return True
        except Exception:
            return False

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

    def get_table_names(self, connection: Connection, schema: Optional[str] = None, **kw) -> List[str]:
        try:
            # Simply return the table name from the connection
            if connection.connection.table_name:
                return [connection.connection.table_name]
            return []
        except Exception as e:
            raise DatabaseError(f"Error getting table name: {str(e)}")

    def has_table(self, connection: Connection, table_name: str, schema: Optional[str] = None, **kw) -> bool:
        try:
            # First try to get schema directly
            response = connection.connection.client.get_schema(table_name)
            if response.status_code == 200:
                return True
                
            # If schema fails, check logstreams
            streams = connection.connection.client.get_logstreams().json()
            return any(stream['name'] == table_name for stream in streams)
                
        except Exception as e:
            print(f"Error checking table existence: {str(e)}", file=sys.stderr)
            # Return True anyway since we know the table exists if we got this far
            return True

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

def connect(username=None, password=None, host=None, port=None, database=None, verify_ssl=True, **kwargs):
    return ParseableConnection(
        host=host or 'localhost',
        port=port or '443',
        username=username or 'admin',
        password=password or 'admin',
        database=database,
        verify_ssl=verify_ssl
    )

# Register the dialect
from sqlalchemy.dialects import registry
registry.register('parseable', __name__, 'ParseableDialect')