# SQLAlchemy Parseable Connector for Apache Superset

A SQLAlchemy dialect and DBAPI implementation for connecting Apache Superset to Parseable, enabling seamless data visualization and analytics of your log data.

## Features

- Full SQLAlchemy dialect implementation for Parseable
- Support for timestamp-based queries
- Automatic schema detection
- Support for special characters in table names (e.g., "ingress-nginx")
- Type mapping from Parseable to SQLAlchemy types
- Connection pooling and management

## Prerequisites

- Python 3.11.6 or higher
- Apache Superset
- A running Parseable instance

## Installation

### 1. Set Up Python Environment

First, create and activate a Python virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate  # On Linux/Mac
# or
.\venv\Scripts\activate  # On Windows
```

### 2. Install and Configure Superset

Install Apache Superset and perform initial setup:

```bash
# Install Superset
pip install apache-superset

# Configure Superset
export SUPERSET_SECRET_KEY=your-secure-secret-key
export FLASK_APP=superset

# Initialize the database
superset db upgrade

# Create an admin user
superset fab create-admin

# Load initial data
superset init
```

### 3. Install Parseable Connector

Install the Parseable connector in development mode:

```bash
cd sqlalchemy-parseable
pip install -e .
```

## Running Superset

Start the Superset development server:

```bash
superset run -p 8088 --with-threads --reload --debugger
```

Access Superset at http://localhost:8088

## Connecting to Parseable

1. In the Superset UI, go to Data → Databases → + Database
2. Select "Other" as the database type
3. Use the following SQLAlchemy URI format:
   ```
   parseable://username:password@host:port/table_name
   ```
   Example:
   ```
   parseable://admin:admin@demo.parseable.com:443/ingress-nginx
   ```

## Query Examples

The connector supports standard SQL queries with some Parseable-specific features:

```sql
-- Basic query with time range
SELECT method, status, COUNT(*) as count
FROM ingress-nginx
WHERE p_timestamp >= '2024-01-01T00:00:00Z'
  AND p_timestamp < '2024-01-02T00:00:00Z'
GROUP BY method, status;

-- Status code analysis
SELECT status, COUNT(*) as count
FROM ingress-nginx
WHERE p_timestamp >= '2024-01-01T00:00:00Z'
GROUP BY status;
```

## Development

The connector implements several key components:

- `ParseableDialect`: SQLAlchemy dialect implementation
- `ParseableClient`: HTTP client for Parseable API
- `ParseableConnection`: DBAPI connection implementation
- `ParseableCursor`: DBAPI cursor implementation

## Features and Limitations

### Supported Features
- Query execution with time range filtering
- Schema inspection
- Column type mapping
- Connection testing
- Table existence checking

### Current Limitations
- No transaction support (Parseable is append-only)
- No write operations support
- Limited to supported Parseable query operations

## Troubleshooting

### Common Issues

1. Connection Errors
   - Verify Parseable host and port are correct
   - Ensure credentials are valid
   - Check if table name exists in Parseable

2. Query Errors
   - Verify time range format (should be ISO8601)
   - Check if column names exist in schema
   - Ensure proper quoting for table names with special characters

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

[Apache License 2.0](LICENSE)
