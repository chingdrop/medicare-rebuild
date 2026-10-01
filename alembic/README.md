# Migrations

Versioned migrations for the GPS database's schema (`medicare_rebuild.models`), the
schema of record (see [decision 0015](../docs/decisions/0015-full-orm-schema-of-record.md)).
Only the GPS database is migrated here -- the legacy source databases are a
reconstruction of a system this repository doesn't own, so migrating them would be
meaningless. See [decision 0016](../docs/decisions/0016-alembic-migrations-for-the-gps-database.md).

```sh
uv run alembic upgrade head              # apply every migration up to the latest
uv run alembic revision --autogenerate -m "add a column"   # after changing models.py
uv run alembic check                     # fails if models.py and the migrations disagree
uv run alembic downgrade -1              # undo the most recent migration
```

All of the above read connection details from the same `GPS_SQL_*` environment
variables (and `.env` file) every other entry point in this repository uses -- see
[`env.py`](env.py). The demo and integration tests do not use Alembic: they build an
ephemeral, throwaway database from `GpsBase.metadata.create_all()` directly, which is
faster and needs no migration history. `tests/integration/test_alembic_integration.py`
is what actually exercises the migration chain, confirming it produces the same schema
`create_all()` would.

Not every migration is a schema change: `31d8eabb7bab_seed_lookup_tables.py` inserts the
lookup rows (vendors, note types, patient statuses, billing codes) the pipeline resolves
against, from a frozen copy of `models.LOOKUP_SEEDS`. `tests/test_alembic_seed.py` fails
if the two copies disagree. A change to the required values needs a new data migration,
not an edit to that one.
