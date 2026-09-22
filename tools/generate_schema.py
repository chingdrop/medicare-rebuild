"""Generate sql/schema.sql from src/medicare_rebuild/models.py and legacy_models.py.

Run via `make schema`. The checked-in file is verified against fresh output by
tests/test_generate_schema.py, so a model change without regenerating fails CI.
"""

from pathlib import Path

from sqlalchemy.dialects import mssql
from sqlalchemy.schema import CreateTable

from medicare_rebuild.legacy_models import LegacyMetadata
from medicare_rebuild.models import GpsBase

OUTPUT = Path(__file__).resolve().parents[1] / "sql" / "schema.sql"

HEADER = """-- Generated from src/medicare_rebuild/models.py and legacy_models.py.
-- Do not hand-edit; run `make schema` to regenerate.
--
-- The GPS tables (below the first marker) are this repository's schema of record.
-- The legacy source tables (below the second marker) are reconstructed from the
-- columns queries.py reads; the real source system is not part of this repository,
-- and these table definitions are not authoritative for it. See
-- docs/decisions/0015-full-orm-schema-of-record.md.
"""


def render() -> str:
    dialect = mssql.dialect()
    parts = [HEADER, "\n-- GPS database (schema of record) --\n"]
    for table in GpsBase.metadata.sorted_tables:
        parts.append(str(CreateTable(table).compile(dialect=dialect)).strip() + ";\n")
    parts.append("\n-- Legacy source tables (reconstructed, not authoritative) --\n")
    for table in LegacyMetadata.sorted_tables:
        parts.append(str(CreateTable(table).compile(dialect=dialect)).strip() + ";\n")
    return "\n".join(parts) + "\n"


def main() -> None:
    OUTPUT.write_text(render())
    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()
