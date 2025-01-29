from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="sqlalchemy-parseable",
    version="0.1.0",
    author="Parseable",
    author_email="adheip@parseable.com",
    description="SQLAlchemy dialect for Parseable",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/parseablehq/sqlalchemy-parseable",
    packages=find_packages(),
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Database",
    ],
    python_requires=">=3.8",
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
