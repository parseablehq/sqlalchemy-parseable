# setup.py
from setuptools import setup, find_packages

setup(
    name="sqlalchemy-parseable",
    version="0.1.0",
    description="SQLAlchemy dialect for Parseable",
    packages=find_packages(),
    install_requires=[
        "sqlalchemy>=1.4.0",
        "requests>=2.25.0"
    ],
    entry_points={
        "sqlalchemy.dialects": [
            "parseable = parseable_connector:ParseableDialect",
        ],
    }
)
