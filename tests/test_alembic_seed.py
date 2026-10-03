"""The lookup-seed migration keeps its own frozen copy of the values (a migration must
not change meaning when application code does), so this pins it to the copy the code
reads, models.LOOKUP_SEEDS, and pins the guard that refuses to run without them."""

import importlib.util
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from medicare_rebuild.__main__ import _require_lookup_seeds
from medicare_rebuild.models import LOOKUP_SEEDS
from tools.synthetic_data.schema import LOOKUP_SEEDS as DEMO_LOOKUP_SEEDS

MIGRATION = Path(__file__).resolve().parents[1] / "alembic" / "versions" / "31d8eabb7bab_seed_lookup_tables.py"


def _load_migration():
    spec = importlib.util.spec_from_file_location("seed_migration", MIGRATION)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_migration_seeds_match_lookup_seeds():
    expected = {model.__tablename__: names for model, names in LOOKUP_SEEDS.items()}
    assert _load_migration().SEEDS == expected


def test_demo_seeds_cover_every_required_lookup_value():
    """The demo seeds its own (larger) vocabulary; it must still include every value
    the pipeline requires, or the demo would fail the guard below."""
    for model, names in LOOKUP_SEEDS.items():
        assert set(names) <= set(DEMO_LOOKUP_SEEDS[model]), model.__tablename__


def _session_with(rows: dict[str, list[str]]) -> MagicMock:
    """A session whose `scalars(select(Model.name))` returns `rows[table name]`."""
    session = MagicMock()

    def scalars(stmt):
        table = stmt.get_final_froms()[0].name
        return iter(rows.get(table, []))

    session.scalars.side_effect = scalars
    return session


def test_require_lookup_seeds_passes_when_every_row_is_present():
    rows = {model.__tablename__: names for model, names in LOOKUP_SEEDS.items()}
    _require_lookup_seeds(_session_with(rows))


def test_require_lookup_seeds_names_every_missing_row():
    rows = {model.__tablename__: names for model, names in LOOKUP_SEEDS.items()}
    rows["vendor"] = ["Tenovi"]
    del rows["medical_code_type"]

    with pytest.raises(RuntimeError, match="make migrate") as exc:
        _require_lookup_seeds(_session_with(rows))

    message = str(exc.value)
    assert "vendor.Omron" in message
    assert "vendor.Tenovi" not in message
    assert "medical_code_type.99202" in message
