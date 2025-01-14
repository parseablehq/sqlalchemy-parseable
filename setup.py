from setuptools import setup

setup(
    name='sqlalchemy-parseable',
    version='0.1.0',
    description='SQLAlchemy dialect for Parseable',
    author='Your Name',
    packages=['parseable_connector'],
    entry_points={
        'sqlalchemy.dialects': [
            'parseable = parseable_connector:ParseableDialect'
        ]
    },
    install_requires=[
        'sqlalchemy>=1.4.0',
        'requests>=2.25.0'
    ]
)
