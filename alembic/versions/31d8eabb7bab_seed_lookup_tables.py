"""seed lookup tables

Revision ID: 31d8eabb7bab
Revises: 59683379e9ce
Create Date: 2026-10-01 15:27:44.855198

The pipeline resolves vendor names, note types, patient statuses and billing codes
against these four tables, and nothing else ever fills them: without these rows every
device is dropped as having no vendor (and with it every reading), and billing stops
on the first code type lookup. The values are frozen here, as a migration's must be;
`models.LOOKUP_SEEDS` is the copy the code reads, and
`tests/test_alembic_seed.py` fails if the two drift apart.
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "31d8eabb7bab"
down_revision: str | Sequence[str] | None = "59683379e9ce"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SEEDS: dict[str, list[str]] = {
    "vendor": ["Tenovi", "Omron"],
    "note_type": ["Initial Evaluation", "Alert"],
    "patient_status_type": ["Active", "Inactive", "Onboard", "Do Not Call"],
    "medical_code_type": ["99202", "99453", "99454", "99457", "99458"],
}


def upgrade() -> None:
    """Upgrade schema."""
    for table_name, names in SEEDS.items():
        table = sa.table(table_name, sa.column("name", sa.String))
        op.bulk_insert(table, [{"name": name} for name in names])


def downgrade() -> None:
    """Downgrade schema."""
    for table_name, names in SEEDS.items():
        table = sa.table(table_name, sa.column("name", sa.String))
        op.execute(table.delete().where(table.c.name.in_(names)))
