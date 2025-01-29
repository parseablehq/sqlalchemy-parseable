# sqlalchemy-parseable

A DBAPI and SQLAlchemy dialect for Parseable.

## Getting Started on local machine.

- Install superset, initalise parseable connector and configure superset.

## Install Superset

- Make sure ```Python 3.11.6``` is installed.

```
python3 -m venv venv
. venv/bin/activate
pip install apache-superset
export SUPERSET_SECRET_KEY=YOUR-SECRET-KEY
export FLASK_APP=superset
superset db upgrade
superset fab create-admin
superset init
```

- Initalise parseable connector.

```
cd sqlalchemy-parseable
pip install -e .
```

- Run superset.

```
superset run -p 8088 --with-threads --reload --debugger
```