"""Alembic environment for the GPS database only (see decision 0016).

The legacy source databases are a reconstruction of a system this repository does not
own (see legacy_models.py); migrating them here would be meaningless, so this
directory's `target_metadata` is `GpsBase.metadata` and nothing else. The connection
comes from the same `GPS_SQL_*` environment variables (and `.env` file) every other
entry point in this repository reads, via the same URL-building helper
`DatabaseManager.create_engine` uses, so `alembic.ini`'s own `sqlalchemy.url` is left
unset on purpose -- there is exactly one place credentials are assembled.
"""

import os
from logging.config import fileConfig

from dotenv import load_dotenv
from sqlalchemy import engine_from_config, pool

from alembic import context
from medicare_rebuild.models import GpsBase
from medicare_rebuild.utils.db_utils import build_mssql_url

load_dotenv()

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = GpsBase.metadata


def _gps_url() -> str:
    return build_mssql_url(
        username=os.environ["GPS_SQL_USERNAME"],
        password=os.environ["GPS_SQL_PASSWORD"],
        host=os.environ["GPS_SQL_HOST"],
        database=os.environ["GPS_SQL_DB"],
    ).render_as_string(hide_password=False)


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    context.configure(
        url=_gps_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = _gps_url()
    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
