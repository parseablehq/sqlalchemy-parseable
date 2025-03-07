from setuptools import setup, find_packages

setup(
    name="sqlalchemy-parseable",
    version="0.1.6",
    description="SQLAlchemy dialect for Parseable",
    long_description="A SQLAlchemy dialect for interacting with Parseable, a high-performance log analytics database.",
    long_description_content_type="text/plain",
    author="Parseable",
    author_email="adheip@parseable.com",
    packages=find_packages(),
    python_requires=">=3.7",
    install_requires=[
        "sqlalchemy>=1.4.0",
        "requests>=2.25.0",
    ],
    entry_points={
        "sqlalchemy.dialects": [
            "parseable = parseable_connector.parseable_dialect:ParseableDialect",
            "parseable.http = parseable_connector.parseable_dialect:ParseableDialect",
            "parseable.https = parseable_connector.parseable_dialect:ParseableDialect",
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)
