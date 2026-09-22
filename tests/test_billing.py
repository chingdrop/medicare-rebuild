import pandas as pd

from medicare_rebuild.billing import (
    _in_report_window,
    _qualifying_99458,
    _qualifying_by_minutes,
    _qualifying_by_reading_days,
    _since,
)


def _notes(rows: list[tuple[int, str, float]]) -> pd.DataFrame:
    """rows: (patient_id, note_datetime, call_time_seconds)."""
    df = pd.DataFrame(
        rows, columns=["patient_id", "note_datetime", "call_time_seconds"]
    )
    df["note_datetime"] = pd.to_datetime(df["note_datetime"])
    return df


def _readings(rows: list[tuple[int, int, str]]) -> pd.DataFrame:
    """rows: (patient_id, device_id, received_datetime)."""
    df = pd.DataFrame(rows, columns=["patient_id", "device_id", "received_datetime"])
    df["received_datetime"] = pd.to_datetime(df["received_datetime"])
    return df


# -- 99202: FLOOR(SUM(call_time_seconds)) / 60 in [15, 30) -----------------------------


def test_99202_below_threshold_not_qualifying():
    notes = _notes([(1, "2025-02-01 10:00:00", 899)])
    out = _qualifying_by_minutes(
        notes, set(), min_minutes=15, max_minutes=30, floor_seconds=True
    )
    assert out.empty


def test_99202_at_threshold_qualifies():
    notes = _notes([(1, "2025-02-01 10:00:00", 900)])
    out = _qualifying_by_minutes(
        notes, set(), min_minutes=15, max_minutes=30, floor_seconds=True
    )
    assert list(out["patient_id"]) == [1]
    assert out["timestamp_applied"].iloc[0] == pd.Timestamp("2025-02-01 10:00:00")


def test_99202_just_under_upper_bound_qualifies():
    notes = _notes([(1, "2025-02-01 10:00:00", 1799)])
    out = _qualifying_by_minutes(
        notes, set(), min_minutes=15, max_minutes=30, floor_seconds=True
    )
    assert list(out["patient_id"]) == [1]


def test_99202_at_upper_bound_excluded():
    notes = _notes([(1, "2025-02-01 10:00:00", 1800)])
    out = _qualifying_by_minutes(
        notes, set(), min_minutes=15, max_minutes=30, floor_seconds=True
    )
    assert out.empty


def test_99202_excludes_patient_with_existing_code():
    notes = _notes([(1, "2025-02-01 10:00:00", 900)])
    out = _qualifying_by_minutes(
        notes, {1}, min_minutes=15, max_minutes=30, floor_seconds=True
    )
    assert out.empty


def test_99202_timestamp_is_latest_note():
    notes = _notes([(1, "2025-02-01 10:00:00", 500), (1, "2025-02-05 08:00:00", 400)])
    out = _qualifying_by_minutes(
        notes, set(), min_minutes=15, max_minutes=30, floor_seconds=True
    )
    assert out["timestamp_applied"].iloc[0] == pd.Timestamp("2025-02-05 08:00:00")


# -- 99457: SUM(call_time_seconds) / 60 >= 20, no floor, no upper bound ----------------


def test_99457_below_threshold_not_qualifying():
    notes = _notes([(1, "2025-02-01 10:00:00", 1199)])
    out = _qualifying_by_minutes(notes, set(), min_minutes=20)
    assert out.empty


def test_99457_at_threshold_qualifies():
    notes = _notes([(1, "2025-02-01 10:00:00", 1200)])
    out = _qualifying_by_minutes(notes, set(), min_minutes=20)
    assert list(out["patient_id"]) == [1]


def test_99457_no_upper_bound():
    # 40 minutes is well past 99202's 30-minute cutoff -- 99457 has none.
    notes = _notes([(1, "2025-02-01 10:00:00", 2400)])
    out = _qualifying_by_minutes(notes, set(), min_minutes=20)
    assert list(out["patient_id"]) == [1]


# -- 99453/99454: >= 16 distinct received dates -----------------------------------------


def _daily_readings(patient_id: int, device_id: int, n_days: int) -> list[tuple]:
    return [
        (patient_id, device_id, f"2025-02-{d:02d} 10:00:00")
        for d in range(1, n_days + 1)
    ]


def test_reading_days_below_threshold_not_qualifying():
    readings = _readings(_daily_readings(1, 1, 15))
    out = _qualifying_by_reading_days(readings)
    assert out.empty


def test_reading_days_at_threshold_qualifies():
    readings = _readings(_daily_readings(1, 1, 16))
    out = _qualifying_by_reading_days(readings)
    assert list(out["patient_id"]) == [1]
    assert out["timestamp_applied"].iloc[0] == pd.Timestamp("2025-02-16 10:00:00")


def test_reading_days_duplicate_readings_same_day_do_not_double_count():
    rows = _daily_readings(1, 1, 16) + [(1, 1, "2025-02-16 18:00:00")]  # dup date
    readings = _readings(rows)
    out = _qualifying_by_reading_days(readings)
    assert list(out["patient_id"]) == [1]


def test_reading_days_multi_device_combines_by_patient():
    # 8 distinct days on device 1, 8 more distinct days on device 2: combined >= 16,
    # matching the stored procedures' GROUP BY d.patient_id (not d.device_id).
    rows = [
        (1, 1, "2025-02-01 10:00:00"),
        (1, 1, "2025-02-02 10:00:00"),
        (1, 1, "2025-02-03 10:00:00"),
        (1, 1, "2025-02-04 10:00:00"),
        (1, 1, "2025-02-05 10:00:00"),
        (1, 1, "2025-02-06 10:00:00"),
        (1, 1, "2025-02-07 10:00:00"),
        (1, 1, "2025-02-08 10:00:00"),
        (1, 2, "2025-02-09 10:00:00"),
        (1, 2, "2025-02-10 10:00:00"),
        (1, 2, "2025-02-11 10:00:00"),
        (1, 2, "2025-02-12 10:00:00"),
        (1, 2, "2025-02-13 10:00:00"),
        (1, 2, "2025-02-14 10:00:00"),
        (1, 2, "2025-02-15 10:00:00"),
        (1, 2, "2025-02-16 10:00:00"),
    ]
    readings = _readings(rows)
    out = _qualifying_by_reading_days(readings)
    assert list(out["patient_id"]) == [1]


def test_reading_days_exclude_mask_drops_rows_below_threshold():
    readings = _readings(_daily_readings(1, 1, 16))
    # Exclude every row -- as if the device is already linked to a 99453.
    exclude = pd.Series([True] * len(readings), index=readings.index)
    out = _qualifying_by_reading_days(readings, exclude)
    assert out.empty


# -- rolling windows: _since -------------------------------------------------------------


def test_since_includes_boundary_and_excludes_before():
    start = pd.Timestamp("2025-01-29")
    readings = _readings([(1, 1, "2025-01-29 00:00:00"), (2, 1, "2025-01-28 23:59:59")])
    out = _since(readings, "received_datetime", start)
    assert list(out["patient_id"]) == [1]


def test_99454_window_edge_matches_worked_example():
    # docs/billing-rules.md worked example S08/S09: report end date 2025-02-28, so the
    # 30-day window starts 2025-01-29. 16 days starting exactly on the window start
    # qualifies for 99454; 16 days starting one day earlier only has 15 inside the
    # window and does not (see the next test).
    window_start = pd.Timestamp("2025-02-28").normalize() - pd.Timedelta(days=30)
    assert window_start == pd.Timestamp("2025-01-29")

    dates = pd.date_range("2025-01-29", periods=16, freq="D") + pd.Timedelta(hours=9)
    readings = _readings([(8, 1, str(d)) for d in dates])
    windowed = _since(readings, "received_datetime", window_start)
    out = _qualifying_by_reading_days(windowed)
    assert list(out["patient_id"]) == [8]


def test_99454_window_edge_one_day_short_of_threshold():
    window_start = pd.Timestamp("2025-01-29")
    # 16 days starting 2025-01-28: only 2025-01-29 onward (15 days) is inside the window.
    dates = pd.date_range("2025-01-28", periods=16, freq="D") + pd.Timedelta(hours=9)
    readings = _readings([(9, 1, str(d)) for d in dates])
    windowed = _since(readings, "received_datetime", window_start)
    out = _qualifying_by_reading_days(windowed)
    assert out.empty


# -- 99458: blocks of 20 minutes, capped at 4, minus existing 99458 rows, minus 1 -------


def _windowed_notes(patient_id: int, minutes: float) -> pd.DataFrame:
    return pd.DataFrame(
        {"patient_id": [patient_id], "call_time_seconds": [minutes * 60]}
    )


def _windowed_codes(patient_id: int, existing_99458: int = 0) -> pd.DataFrame:
    names = ["99457"] + ["99458"] * existing_99458
    return pd.DataFrame({"patient_id": [patient_id] * len(names), "name": names})


def test_99458_39_minutes_earns_nothing():
    notes = _windowed_notes(1030, 39)
    codes = _windowed_codes(1030)
    all_notes = pd.DataFrame(
        {"patient_id": [1030], "note_datetime": [pd.Timestamp("2025-02-20")]}
    )
    out = _qualifying_99458(notes, codes, all_notes)
    assert out.empty


def test_99458_40_minutes_earns_one():
    notes = _windowed_notes(1031, 40)
    codes = _windowed_codes(1031)
    all_notes = pd.DataFrame(
        {"patient_id": [1031], "note_datetime": [pd.Timestamp("2025-02-20")]}
    )
    out = _qualifying_99458(notes, codes, all_notes)
    assert len(out) == 1


def test_99458_60_minutes_earns_two():
    notes = _windowed_notes(1, 60)
    codes = _windowed_codes(1)
    all_notes = pd.DataFrame(
        {"patient_id": [1], "note_datetime": [pd.Timestamp("2025-02-20")]}
    )
    out = _qualifying_99458(notes, codes, all_notes)
    assert len(out) == 2


def test_99458_80_minutes_earns_three():
    notes = _windowed_notes(1, 80)
    codes = _windowed_codes(1)
    all_notes = pd.DataFrame(
        {"patient_id": [1], "note_datetime": [pd.Timestamp("2025-02-20")]}
    )
    out = _qualifying_99458(notes, codes, all_notes)
    assert len(out) == 3


def test_99458_100_minutes_still_capped_at_three():
    notes = _windowed_notes(1, 100)
    codes = _windowed_codes(1)
    all_notes = pd.DataFrame(
        {"patient_id": [1], "note_datetime": [pd.Timestamp("2025-02-20")]}
    )
    out = _qualifying_99458(notes, codes, all_notes)
    assert len(out) == 3


def test_99458_requires_an_existing_code_in_the_window():
    notes = _windowed_notes(1, 80)
    codes = pd.DataFrame(columns=["patient_id", "name"])  # no code this month at all
    all_notes = pd.DataFrame(
        {"patient_id": [1], "note_datetime": [pd.Timestamp("2025-02-20")]}
    )
    out = _qualifying_99458(notes, codes, all_notes)
    assert out.empty


def test_99458_existing_99458_rows_reduce_the_count():
    # 80 minutes -> 4 blocks, but 3 99458s already exist this month: 4 - 3 = 1, not > 1.
    notes = _windowed_notes(1, 80)
    codes = _windowed_codes(1, existing_99458=3)
    all_notes = pd.DataFrame(
        {"patient_id": [1], "note_datetime": [pd.Timestamp("2025-02-20")]}
    )
    out = _qualifying_99458(notes, codes, all_notes)
    assert out.empty


def test_99458_stamped_at_latest_note_overall_not_just_in_window():
    # The stamp uses the patient's latest note overall, per the stored procedure's
    # unwindowed outer join -- confirmed against a later note that falls outside the
    # windowed_notes/windowed_codes inputs.
    notes = _windowed_notes(1, 60)
    codes = _windowed_codes(1)
    all_notes = pd.DataFrame(
        {
            "patient_id": [1, 1],
            "note_datetime": [pd.Timestamp("2025-02-20"), pd.Timestamp("2025-03-15")],
        }
    )
    out = _qualifying_99458(notes, codes, all_notes)
    assert (out["timestamp_applied"] == pd.Timestamp("2025-03-15")).all()


# -- report window: midnight-of-end-date exclusion --------------------------------------


def test_report_window_includes_start_and_end_midnight():
    codes = pd.DataFrame(
        {
            "patient_id": [1, 2],
            "timestamp_applied": [
                pd.Timestamp("2025-02-01 00:00:00"),
                pd.Timestamp("2025-02-28 00:00:00"),
            ],
            "name": ["99453", "99454"],
        }
    )
    out = _in_report_window(
        codes, pd.Timestamp("2025-02-01"), pd.Timestamp("2025-02-28")
    )
    assert len(out) == 2


def test_report_window_excludes_after_midnight_on_end_date():
    # A code timestamped after midnight on the end date is excluded even though its
    # calendar date is the report's last day -- the stored procedure's @end_date
    # parameter is implicitly cast to midnight before comparison.
    codes = pd.DataFrame(
        {
            "patient_id": [1],
            "timestamp_applied": [pd.Timestamp("2025-02-28 00:20:00")],
            "name": ["99453"],
        }
    )
    out = _in_report_window(
        codes, pd.Timestamp("2025-02-01"), pd.Timestamp("2025-02-28")
    )
    assert out.empty


def test_report_window_excludes_before_start():
    codes = pd.DataFrame(
        {
            "patient_id": [1],
            "timestamp_applied": [pd.Timestamp("2025-01-31 23:59:59")],
            "name": ["99453"],
        }
    )
    out = _in_report_window(
        codes, pd.Timestamp("2025-02-01"), pd.Timestamp("2025-02-28")
    )
    assert out.empty
