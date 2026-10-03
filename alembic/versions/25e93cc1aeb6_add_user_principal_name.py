"""add user principal name

Revision ID: 25e93cc1aeb6
Revises: 31d8eabb7bab
Create Date: 2026-10-01 15:40:52.368672

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "25e93cc1aeb6"
down_revision: str | Sequence[str] | None = "31d8eabb7bab"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("user", sa.Column("user_principal_name", sa.String(length=200), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("user", "user_principal_name")
