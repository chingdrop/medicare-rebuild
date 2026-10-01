"""DataImporter's connection handling, with DatabaseManager mocked out: every legacy
connection it opens is closed again, including when a read fails part-way, and
import_all_data()/create_billing_report() close the GPS connection on failure too."""

from unittest.mock import MagicMock, patch

import pytest

from medicare_rebuild import __main__ as pipeline

ENV = {
    "GPS_SQL_USERNAME": "u",
    "GPS_SQL_PASSWORD": "p",
    "GPS_SQL_HOST": "h",
    "GPS_SQL_DB": "gps",
    "LEGACY_SQL_USERNAME": "u",
    "LEGACY_SQL_PASSWORD": "p",
    "LEGACY_SQL_HOST": "h",
    "LEGACY_SQL_SP_NOTES": "notes",
    "LEGACY_SQL_SP_TIME": "time",
    "LEGACY_SQL_SP_FULFILLMENT": "fulfillment",
    "LEGACY_SQL_SP_READINGS": "readings",
}


@pytest.fixture
def managers(monkeypatch):
    """Every DatabaseManager the pipeline creates, in creation order; the first is the
    GPS connection DataImporter.__init__ opens."""
    for k, v in ENV.items():
        monkeypatch.setenv(k, v)
    created: list[MagicMock] = []

    def make(*_args, **_kwargs):
        db = MagicMock(name=f"db{len(created)}")
        created.append(db)
        return db

    with patch.object(pipeline, "DatabaseManager", side_effect=make):
        yield created


@pytest.mark.parametrize(
    "method",
    [
        "get_device_data",
        "get_gluc_readings",
        "get_bp_readings",
        "get_patient_note_data",
    ],
)
def test_legacy_connection_is_closed_when_the_read_fails(managers, method):
    importer = pipeline.DataImporter("2025-01-01", "2025-03-01")

    def failing_read(*_args, **_kwargs):
        raise RuntimeError("source unavailable")

    with patch.object(pipeline, "DatabaseManager") as legacy_cls:
        legacy = legacy_cls.return_value
        legacy.read_sql.side_effect = failing_read
        with pytest.raises(RuntimeError, match="source unavailable"):
            getattr(importer, method)()
        legacy.close.assert_called_once()


def test_import_all_data_closes_the_gps_connection_on_failure(managers, tmp_path):
    with (
        patch.object(pipeline, "_require_lookup_seeds", side_effect=RuntimeError("x")),
        patch.object(pipeline.Path, "cwd", return_value=tmp_path),
        pytest.raises(RuntimeError),
    ):
        pipeline.import_all_data("2025-01-01", "2025-03-01")

    gps = managers[0]
    gps.get_session.return_value.close.assert_called_once()
    gps.close.assert_called_once()


def test_create_billing_report_closes_the_gps_connection_on_failure(managers):
    with (
        patch.object(pipeline, "run_billing", side_effect=RuntimeError("x")),
        pytest.raises(RuntimeError),
    ):
        pipeline.create_billing_report("2025-02-01", "2025-03-01")

    gps = managers[0]
    gps.get_session.return_value.close.assert_called_once()
    gps.close.assert_called_once()


def test_unique_lookup_leaves_shared_keys_unmatched_and_logs_them(managers, caplog):
    importer = pipeline.DataImporter("2025-01-01", "2025-03-01")
    importer.session = MagicMock()
    importer.session.execute.return_value = [
        ("Jane Doe", 1),
        ("Jane Doe", 2),
        ("Sam Roe", 3),
        (None, 4),
    ]

    with caplog.at_level("WARNING"):
        lookup = importer._unique_lookup(pipeline.User, "display_name", "user_id")

    assert lookup == {"Sam Roe": 3}
    assert "Jane Doe" in caplog.text


def test_unique_lookup_casefold_matches_sign_in_names_case_insensitively(managers):
    importer = pipeline.DataImporter("2025-01-01", "2025-03-01")
    importer.session = MagicMock()
    importer.session.execute.return_value = [("JDoe@Example.com", 7)]

    lookup = importer._unique_lookup(
        pipeline.User, "user_principal_name", "user_id", casefold=True
    )

    assert lookup == {"jdoe@example.com": 7}
