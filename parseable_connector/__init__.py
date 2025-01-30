# parseable_connector/__init__.py

from .parseable_dialect import (
    ParseableDialect,
    Error,
    DatabaseError,
    InterfaceError,
    connect,
    ParseableConnection,
    ParseableCursor
)

__version__ = "0.1.1"

# DBAPI required attributes
apilevel = '2.0'
threadsafety = 1
paramstyle = 'named'

__all__ = [
    "ParseableDialect",
    "Error",
    "DatabaseError",
    "InterfaceError",
    "ParseableConnection",
    "ParseableCursor",
    "connect",
]