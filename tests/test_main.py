"""main()'s own logic is just date arithmetic wired to import_all_data() and
create_billing_report(); both of those, and everything they touch, are mocked out here
so this stays a unit test -- they each have their own integration test coverage
already."""

from datetime import datetime
from unittest.mock import patch

import time_machine

from medicare_rebuild.__main__ import main


@time_machine.travel(datetime(2025, 3, 15, 12, 0))
@patch("medicare_rebuild.__main__.setup_logger")
@patch("medicare_rebuild.__main__.load_dotenv")
@patch("medicare_rebuild.__main__.create_billing_report")
@patch("medicare_rebuild.__main__.import_all_data")
def test_main_derives_last_months_billing_cycle(
    mock_import_all_data, mock_create_billing_report, *_mocks
):
    """ "Today" in March: the report covers February, and the import window starts a
    full calendar month earlier (January) so the billing rules' rolling windows (up to
    30 days/1 month back from the report end date) have every reading/note they need
    already loaded -- see the comment in main()."""
    main()

    mock_import_all_data.assert_called_once()
    import_start, import_end = mock_import_all_data.call_args.args
    assert import_start == "2025-01-01"
    assert import_end == "2025-02-28"

    mock_create_billing_report.assert_called_once()
    report_start, report_end = mock_create_billing_report.call_args.args
    assert report_start == "2025-02-01"
    assert report_end == "2025-02-28"


@time_machine.travel(datetime(2025, 1, 15, 12, 0))
@patch("medicare_rebuild.__main__.setup_logger")
@patch("medicare_rebuild.__main__.load_dotenv")
@patch("medicare_rebuild.__main__.create_billing_report")
@patch("medicare_rebuild.__main__.import_all_data")
def test_main_import_window_crosses_year_boundary(
    mock_import_all_data, mock_create_billing_report, *_mocks
):
    """ "Today" in January: the report covers last December, and the import window
    starts in November of the prior year."""
    main()

    import_start, import_end = mock_import_all_data.call_args.args
    assert import_start == "2024-11-01"
    assert import_end == "2024-12-31"

    report_start, report_end = mock_create_billing_report.call_args.args
    assert report_start == "2024-12-01"
    assert report_end == "2024-12-31"
