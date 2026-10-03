"""sql/schema.sql must be regenerated whenever the models change (`make schema`).
This fails if the checked-in file has drifted from what the models would produce."""

from tools.generate_schema import OUTPUT, render


def test_schema_sql_matches_the_models():
    assert OUTPUT.read_text() == render(), (
        "sql/schema.sql is out of date with the models; run `make schema` to regenerate"
    )
