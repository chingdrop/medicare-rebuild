"""Scenario builders.

Each builder creates source data for one patient AND states what the pipeline
should produce for it. The expectations are written down as part of building the
data (e.g. "16 distinct days of readings, so a 99453 stamped at the last
reading"), not computed by re-running any billing logic. The thresholds come from
`medicare_rebuild.billing` (a faithful pandas/ORM port of sql/stored_procedures/*.sql,
see decision 0014) and the repo's tests; see docs/demo.md.
"""

import math
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime

from tools.synthetic_data import config as cfg
from tools.synthetic_data.factory import Factory, day, days_between
from tools.synthetic_data.model import ExpectedCode, Note, Plan, Reading

IE = "Initial Evaluation"


@dataclass(frozen=True)
class Scenario:
    key: str
    rule: str  # billing rule or pipeline behaviour being exercised
    summary: str
    build: Callable[[Factory, int], Plan]
    edge: str = ""  # boundary or quirk this scenario pins down, if any


# -- helpers ------------------------------------------------------------------


def _plan(f: Factory, sid: int, key: str, contacts: int = 1, **overrides) -> Plan:
    row = f.patient_row(sid, contacts=contacts)
    row.update(overrides)
    dx = [c for c in str(row["DX_Code"]).replace(";", ",").split(",") if c.strip()]
    return Plan(
        scenario=key,
        patient_id=sid,
        row=row,
        emergency_contacts=contacts,
        dx_rows=max(len(dx), 1),
    )


def _last(readings: list[Reading]) -> datetime:
    return max(r.received for r in readings)


def _rpm_codes(readings: list[Reading], codes=("99453", "99454")) -> list[ExpectedCode]:
    return [ExpectedCode(c, _last(readings)) for c in codes]


def _notes_totalling(
    f: Factory,
    dates: list[date],
    total_seconds: int,
    *,
    note_type: str = "Follow-Up",
    upn: str | None = None,
) -> list[Note]:
    """Spread `total_seconds` evenly (to the minute) across one note per date."""
    per = math.ceil(total_seconds / len(dates) / 60) * 60
    notes, left = [], total_seconds
    for d in dates:
        secs = min(per, left)
        left -= secs
        if secs:
            notes.append(f.note_on(d, secs, note_type=note_type, upn=upn))
    return notes


def _last_note(notes: list[Note]) -> datetime:
    return max(n.timestamp for n in notes)


def _time_codes(
    notes: list[Note], total: int, effective_ie: bool = False
) -> list[ExpectedCode]:
    """Codes for a patient whose ONLY time comes from `notes` (all inside the 99457 window).

    Written from the documented thresholds: 99457 needs 20 minutes, each further 20
    minutes is a 99458 (first block belongs to 99457, capped at 3 extra).
    """
    codes = []
    if effective_ie and 900 <= total < 1800:
        codes.append(ExpectedCode("99202", _last_note(notes)))
    if total >= 1200:
        codes.append(ExpectedCode("99457", _last_note(notes)))
    for _ in range(min(max(total // 1200 - 1, 0), 3)):
        codes.append(ExpectedCode("99458", _last_note(notes)))
    return codes


# -- remote monitoring (99453 / 99454) ----------------------------------------


# `day()` returns an immutable date, so sharing the default is safe.
def _rpm(
    key_days: int,
    kind: str = "bg",
    first: date = day(2, 3),  # noqa: B008
    per_day: int = 1,
):
    def build(f: Factory, sid: int) -> Plan:
        p = _plan(f, sid, "")
        p.devices = [f.device(kind, sid)]
        p.readings = f.daily_readings(kind, days_between(first, key_days), per_day)
        if key_days >= 16:
            p.expected_codes = _rpm_codes(p.readings)
        return p

    return build


def _rpm_multi_device(f: Factory, sid: int) -> Plan:
    p = _plan(f, sid, "", contacts=2)
    p.devices = [f.device("bg", sid), f.device("bp", sid)]
    bg = f.daily_readings("bg", days_between(day(2, 3), 16))
    bp = f.daily_readings("bp", days_between(day(2, 3), 16))
    p.readings = bg + bp
    # The glucose procedure runs first and links the code to every device the patient
    # has, so the blood pressure procedures skip: one 99453, one 99454.
    p.expected_codes = _rpm_codes(bg)
    p.flags = ["reading_fanout"]
    return p


def _rpm_window(first: date, count: int, codes: tuple[str, ...]):
    def build(f: Factory, sid: int) -> Plan:
        p = _plan(f, sid, "")
        p.devices = [f.device("bg", sid)]
        p.readings = f.daily_readings("bg", days_between(first, count))
        p.expected_codes = _rpm_codes(p.readings, codes)
        return p

    return build


def _rpm_duplicates(f: Factory, sid: int) -> Plan:
    p = _plan(f, sid, "")
    p.devices = [f.device("bg", sid)]
    once = f.daily_readings("bg", days_between(day(2, 5), 16))
    p.readings = once + [
        Reading(r.kind, r.recorded, r.received, r.values, r.manual) for r in once
    ]
    p.duplicate_readings = len(once)
    p.expected_codes = _rpm_codes(once)
    p.flags = ["duplicate_rows_loaded_as_is"]
    return p


def _rpm_out_of_order(f: Factory, sid: int) -> Plan:
    p = _plan(f, sid, "")
    p.devices = [f.device("bp", sid)]
    readings = f.daily_readings("bp", days_between(day(2, 5), 16))
    # Clock skew: a few devices report a received time just BEFORE the recorded time.
    for i in (2, 7, 11):
        r = readings[i]
        readings[i] = f.reading_at("bp", r.recorded, latency_min=-3)
    f.rng.shuffle(readings)  # source rows arrive in no particular order
    p.readings = readings
    p.expected_codes = _rpm_codes(readings)
    return p


def _rpm_received_after_cutoff(f: Factory, sid: int) -> Plan:
    p = _plan(f, sid, "")
    p.devices = [f.device("bg", sid)]
    readings = f.daily_readings("bg", days_between(day(2, 13), 15))
    # Recorded inside the extract window (before 2025-02-28 00:00) but received just
    # after midnight, which is also the 16th distinct received date.
    readings.append(f.reading_at("bg", datetime(2025, 2, 27, 23, 50), latency_min=30))
    p.readings = readings
    p.expected_codes = _rpm_codes(readings)  # stamped 2025-02-28 00:20
    p.flags = ["report_end_date_edge"]
    return p


def _rpm_recorded_after_cutoff(f: Factory, sid: int) -> Plan:
    p = _plan(f, sid, "")
    p.devices = [f.device("bg", sid)]
    readings = f.daily_readings("bg", days_between(day(2, 13), 15))
    readings.append(
        f.reading_at("bg", datetime(2025, 2, 28, 9, 0))
    )  # after extract window
    p.readings = readings  # only 15 distinct days are extracted -> no codes
    return p


def _rpm_null_values(f: Factory, sid: int) -> Plan:
    p = _plan(f, sid, "")
    p.devices = [f.device("bg", sid)]
    readings = f.daily_readings("bg", days_between(day(2, 3), 16))
    for i in (1, 5, 9):
        readings[i].values = (None,)
        readings[i].manual = None
    p.readings = readings
    p.expected_codes = _rpm_codes(readings)  # billing counts days, not values
    return p


def _rpm_no_device(f: Factory, sid: int) -> Plan:
    p = _plan(f, sid, "")
    p.readings = f.daily_readings("bg", days_between(day(2, 3), 16))
    return p  # readings need a device row to load; none loads, so no codes


def _resupply_device(f: Factory, sid: int) -> Plan:
    p = _plan(f, sid, "")
    p.devices = [f.device("bg", sid), f.device("bp", sid, resupply=True)]
    p.readings = f.daily_readings("bg", days_between(day(2, 3), 16))
    p.expected_codes = _rpm_codes(p.readings)
    return p


# -- clinical time (99202 / 99457 / 99458) ------------------------------------


def _np_initial_eval(f: Factory, sid: int) -> Plan:
    p = _plan(f, sid, "")
    # Logged as 7 minutes; the pipeline forces NursePractitioner time to 15 minutes.
    p.notes = [f.note_on(day(2, 10), 420, note_type=IE, upn=cfg.NURSE_PRACTITIONER)]
    p.expected_codes = [ExpectedCode("99202", p.notes[0].timestamp)]
    return p


def _ie(seconds: int, code_99202: bool, code_99457: bool):
    def build(f: Factory, sid: int) -> Plan:
        p = _plan(f, sid, "")
        p.notes = [f.note_on(day(2, 10), seconds, note_type=IE)]
        ts = p.notes[0].timestamp
        if code_99202:
            p.expected_codes.append(ExpectedCode("99202", ts))
        if code_99457:
            p.expected_codes.append(ExpectedCode("99457", ts))
        return p

    return build


def _ie_two_notes(f: Factory, sid: int) -> Plan:
    p = _plan(f, sid, "")
    p.notes = [
        f.note_on(day(2, 4), 600, note_type=IE),
        f.note_on(day(2, 6), 400, note_type=IE),
    ]
    p.expected_codes = [ExpectedCode("99202", _last_note(p.notes))]  # 1000 s = 16.7 min
    return p


def _rn_forced_type(f: Factory, sid: int) -> Plan:
    p = _plan(f, sid, "")
    # Logged as a follow-up; the pipeline re-types RegisteredNurse notes as Initial Evaluation.
    p.notes = [
        f.note_on(day(2, 12), 900, note_type="Follow-Up", upn=cfg.REGISTERED_NURSE)
    ]
    p.expected_codes = [ExpectedCode("99202", p.notes[0].timestamp)]
    return p


def _time_only(total: int, dates: list[date], **kw):
    def build(f: Factory, sid: int) -> Plan:
        p = _plan(f, sid, "")
        p.notes = _notes_totalling(f, dates, total, **kw)
        p.expected_codes = _time_codes(p.notes, total)
        return p

    return build


def _window_notes(first_at: datetime, expect_99457: bool):
    def build(f: Factory, sid: int) -> Plan:
        p = _plan(f, sid, "")
        p.notes = [f.note(first_at, 600), f.note_on(day(2, 10), 600)]
        if expect_99457:
            p.expected_codes = [ExpectedCode("99457", _last_note(p.notes))]
        return p

    return build


def _duplicate_notes(f: Factory, sid: int) -> Plan:
    p = _plan(f, sid, "")
    note = f.note_on(day(2, 11), 600)
    p.notes = [
        note,
        Note(
            note.timestamp,
            note.upn,
            note.body,
            note.note_type,
            note.seconds,
            note.auto_time,
            same_as=note,
        ),
    ]
    p.duplicate_notes = 1
    # No de-duplication anywhere in the pipeline: both rows load and 2 x 600 s counts.
    p.expected_codes = [ExpectedCode("99457", note.timestamp)]
    p.flags = ["duplicate_rows_loaded_as_is", "call_time_double_counted"]
    return p


def _notes_without_time(f: Factory, sid: int) -> Plan:
    p = _plan(f, sid, "")
    p.notes = [f.note_on(day(2, d), None, html=(d == 8)) for d in (4, 8, 12)]
    return p


def _alert_notes(f: Factory, sid: int) -> Plan:
    p = _plan(f, sid, "")
    p.notes = [f.note_on(day(2, 6), 1200, note_type="Follow-Up", upn=cfg.ALERT_MEMBER)]
    p.expected_codes = [
        ExpectedCode("99457", p.notes[0].timestamp)
    ]  # any note type counts
    return p


def _full_stack(f: Factory, sid: int) -> Plan:
    p = _plan(f, sid, "", contacts=2)
    p.devices = [f.device("bg", sid)]
    p.readings = f.daily_readings(
        "bg", days_between(day(2, 5), 16)
    )  # last reading 2025-02-20
    p.notes = _notes_totalling(f, [day(2, 6), day(2, 12), day(2, 20)], 2400)
    p.expected_codes = _rpm_codes(p.readings) + _time_codes(p.notes, 2400)
    return p


# -- data quality / rejects ---------------------------------------------------


def _rejected(reason: str, **overrides):
    def build(f: Factory, sid: int) -> Plan:
        p = _plan(f, sid, "", **overrides)
        # Would qualify for 99453/99454 if it were not rejected: proves the dependents
        # (device, readings, notes) are dropped with the patient.
        p.devices = [f.device("bg", sid)]
        p.readings = f.daily_readings("bg", days_between(day(2, 3), 16))
        p.notes = [f.note_on(day(2, 9), 1500)]
        p.rejected_reason = reason
        return p

    return build


def _malformed_email(f: Factory, sid: int) -> Plan:
    return _plan(f, sid, "", Email="not-an-email-address")


def _messy_formatting(f: Factory, sid: int) -> Plan:
    p = _plan(f, sid, "")
    r = p.row
    assert r is not None
    r["First Name"] = f"  {str(r['First Name']).upper()}  "
    r["Last Name"] = str(r["Last Name"]).lower()
    r["Phone Number"] = "415.555.0142"
    r["Email"] = "  Mixed.Case.Syn@Example.COM "
    r["State"] = "california"
    r["Weight"] = "185"
    r["Height"] = "5ft 8in"
    r["DX_Code"] = "e11.9 ; I10"
    r["Member_Status"] = "In-Active"
    p.dx_rows = 2
    return p


def _missing_optional(f: Factory, sid: int) -> Plan:
    p = _plan(f, sid, "")
    r = p.row
    assert r is not None
    for col in (
        "Middle Name",
        "Suffix",
        "Race",
        "Weight",
        "Height",
        "Email",
        "DX_Code",
        "Preferred_Language",
    ):
        r[col] = ""
    p.devices = [f.device("bg", sid)]
    p.readings = f.daily_readings("bg", days_between(day(2, 3), 16))
    p.expected_codes = _rpm_codes(p.readings)
    p.dx_rows = 1  # an empty diagnosis list still produces one (blank) row
    return p


def _orphan(f: Factory, sid: int) -> Plan:
    p = Plan(scenario="", patient_id=sid, in_export=False)
    p.devices = [f.device("bg", sid)]
    p.readings = f.daily_readings("bg", days_between(day(2, 3), 16))
    p.notes = [f.note_on(day(2, 9), 2400)]
    return p  # no patient row -> nothing can link -> nothing loads, no codes


# -- registry -----------------------------------------------------------------

SCENARIOS: list[Scenario] = [
    Scenario(
        "rpm_bg_16_days",
        "99453 + 99454",
        "16 distinct glucose days",
        _rpm(16),
        "threshold (16)",
    ),
    Scenario(
        "rpm_bg_15_days",
        "99453 + 99454",
        "15 distinct glucose days",
        _rpm(15),
        "just below threshold",
    ),
    Scenario(
        "rpm_bg_17_days",
        "99453 + 99454",
        "17 distinct glucose days",
        _rpm(17),
        "just above threshold",
    ),
    Scenario(
        "rpm_bg_many_per_day",
        "99453 + 99454",
        "15 days, 3 readings/day (45 rows)",
        _rpm(15, per_day=3),
        "counts days, not readings",
    ),
    Scenario(
        "rpm_bp_16_days",
        "99453 + 99454",
        "16 distinct blood pressure days",
        _rpm(16, "bp"),
        "threshold (16), bp procedures",
    ),
    Scenario(
        "rpm_bp_15_days",
        "99453 + 99454",
        "15 distinct blood pressure days",
        _rpm(15, "bp"),
        "just below threshold, bp procedures",
    ),
    Scenario(
        "rpm_multi_device",
        "99453 + 99454",
        "one patient, glucose + BP devices, 16 days each",
        _rpm_multi_device,
        "one code per patient; readings duplicated by device join",
    ),
    Scenario(
        "rpm_window_inside",
        "99454",
        "16 days starting 2025-01-29",
        _rpm_window(day(1, 29), 16, ("99453", "99454")),
        "30-day window edge, inside",
    ),
    Scenario(
        "rpm_window_outside",
        "99454",
        "16 days starting 2025-01-28",
        _rpm_window(day(1, 28), 16, ("99453",)),
        "30-day window edge, outside (99453 only)",
    ),
    Scenario(
        "rpm_january_only",
        "99453",
        "16 days in early January",
        _rpm_window(day(1, 2), 16, ("99453",)),
        "code applied outside report period",
    ),
    Scenario(
        "rpm_duplicate_readings",
        "99453 + 99454",
        "16 days, every reading duplicated",
        _rpm_duplicates,
        "duplicate source rows",
    ),
    Scenario(
        "rpm_out_of_order",
        "99453 + 99454",
        "16 days, shuffled rows, received before recorded",
        _rpm_out_of_order,
        "out-of-order dates",
    ),
    Scenario(
        "rpm_received_after_midnight",
        "report",
        "last reading received 2025-02-28 00:20",
        _rpm_received_after_cutoff,
        "report end-date edge: coded but not reported",
    ),
    Scenario(
        "rpm_recorded_after_cutoff",
        "extract window",
        "15 days + a reading recorded 2025-02-28 09:00",
        _rpm_recorded_after_cutoff,
        "extract end-date edge: 16th day never loaded",
    ),
    Scenario(
        "rpm_null_values",
        "99453 + 99454",
        "16 days, some readings with NULL values",
        _rpm_null_values,
        "missing fields",
    ),
    Scenario(
        "rpm_readings_no_device",
        "load",
        "readings but no device on file",
        _rpm_no_device,
        "dependent rows dropped",
    ),
    Scenario(
        "rpm_resupply_device",
        "load",
        "extra device flagged Resupply",
        _resupply_device,
        "source query excludes resupply rows",
    ),
    Scenario(
        "ie_nurse_practitioner",
        "99202",
        "NursePractitioner note logged at 7 min",
        _np_initial_eval,
        "time forced to 15 min",
    ),
    Scenario(
        "ie_899_seconds",
        "99202",
        "Initial Evaluation, 899 s",
        _ie(899, False, False),
        "just below 15 min",
    ),
    Scenario(
        "ie_900_seconds",
        "99202",
        "Initial Evaluation, 900 s",
        _ie(900, True, False),
        "threshold (15 min)",
    ),
    Scenario(
        "ie_1799_seconds",
        "99202 + 99457",
        "Initial Evaluation, 1799 s",
        _ie(1799, True, True),
        "just below 30 min",
    ),
    Scenario(
        "ie_1800_seconds",
        "99457",
        "Initial Evaluation, 1800 s",
        _ie(1800, False, True),
        "at 30 min: no 99202",
    ),
    Scenario(
        "ie_two_notes",
        "99202",
        "two Initial Evaluation notes, 600 s + 400 s",
        _ie_two_notes,
        "time summed across notes",
    ),
    Scenario(
        "ie_registered_nurse",
        "99202",
        "RegisteredNurse1 follow-up note, 900 s",
        _rn_forced_type,
        "note type forced to Initial Evaluation",
    ),
    Scenario(
        "time_1199_seconds",
        "99457",
        "follow-up notes totalling 1199 s",
        _time_only(1199, [day(2, 10)]),
        "just below 20 min",
    ),
    Scenario(
        "time_1200_seconds",
        "99457",
        "follow-up notes totalling 1200 s",
        _time_only(1200, [day(2, 10)]),
        "threshold (20 min)",
    ),
    Scenario(
        "time_three_notes",
        "99457",
        "three 400 s notes",
        _time_only(1200, [day(2, 4), day(2, 11), day(2, 18)]),
        "time summed across notes",
    ),
    Scenario(
        "time_window_inside",
        "99457",
        "600 s on 2025-01-28 + 600 s in Feb",
        _window_notes(datetime(2025, 1, 28, 9, 0), True),
        "1-month window edge, inside",
    ),
    Scenario(
        "time_window_outside",
        "99457",
        "600 s on 2025-01-27 + 600 s in Feb",
        _window_notes(datetime(2025, 1, 27, 23, 0), False),
        "1-month window edge, outside",
    ),
    Scenario(
        "time_39_minutes",
        "99457 / 99458",
        "notes totalling 39 min",
        _time_only(2340, [day(2, 4), day(2, 11)]),
        "99458 not yet earned",
    ),
    Scenario(
        "time_40_minutes",
        "99457 / 99458",
        "notes totalling 40 min",
        _time_only(2400, [day(2, 4), day(2, 11)]),
        "first 99458",
    ),
    Scenario(
        "time_60_minutes",
        "99457 / 99458",
        "notes totalling 60 min",
        _time_only(3600, [day(2, 4), day(2, 11), day(2, 18)]),
        "two 99458",
    ),
    Scenario(
        "time_80_minutes",
        "99457 / 99458",
        "notes totalling 80 min",
        _time_only(4800, [day(2, 4), day(2, 11), day(2, 18), day(2, 24)]),
        "three 99458",
    ),
    Scenario(
        "time_100_minutes",
        "99457 / 99458",
        "notes totalling 100 min",
        _time_only(6000, [day(2, 3), day(2, 8), day(2, 13), day(2, 18), day(2, 24)]),
        "99458 capped at three",
    ),
    Scenario(
        "time_duplicate_notes",
        "99457",
        "one 600 s note present twice",
        _duplicate_notes,
        "duplicate source rows double-count time",
    ),
    Scenario(
        "time_no_time_log",
        "99457",
        "notes with no time-log row",
        _notes_without_time,
        "missing fields; HTML markup in note text",
    ),
    Scenario(
        "time_alert_notes",
        "99457",
        "AlertTeamMember1 note, 1200 s",
        _alert_notes,
        "note type forced to Alert; any type counts",
    ),
    Scenario(
        "rpm_and_time_same_day",
        "all codes",
        "16 glucose days + 40 min of notes ending 2025-02-20",
        _full_stack,
        "one report row carries four codes",
    ),
    Scenario(
        "reject_phone",
        "load",
        "phone number with extension (14 digits)",
        _rejected(
            "phone number longer than 11 digits",
            **{"Phone Number": "+1 (415) 555-0142 x123"},
        ),
        "patient rejected, dependents dropped",
    ),
    Scenario(
        "reject_state",
        "load",
        "state that is not a US state",
        _rejected("state not recognised (longer than 2 characters)", State="Atlantis"),
        "patient rejected, dependents dropped",
    ),
    Scenario(
        "reject_zip",
        "load",
        "7-digit ZIP code",
        _rejected("ZIP code longer than 5 characters", **{"Zip code": "0004567"}),
        "patient rejected, dependents dropped",
    ),
    Scenario(
        "reject_emergency_phone",
        "load",
        "emergency contact number with extension",
        _rejected(
            "emergency contact number longer than 11 digits",
            EmergencyNumber="(415) 555-0142 x9999999",
        ),
        "patient rejected, dependents dropped",
    ),
    Scenario(
        "malformed_email",
        "load",
        "email that is not an address",
        _malformed_email,
        "loads with NULL email",
    ),
    Scenario(
        "messy_formatting",
        "load",
        "odd case, spacing, punctuation and units",
        _messy_formatting,
        "values normalised, not rejected",
    ),
    Scenario(
        "missing_optional_fields",
        "load",
        "blank optional fields incl. diagnosis codes",
        _missing_optional,
        "blank diagnosis still yields a report row",
    ),
    Scenario(
        "orphan_source_rows",
        "load",
        "notes, device and readings for an ID not in the export",
        _orphan,
        "no matching patient: nothing loads",
    ),
]


# -- filler patients ----------------------------------------------------------
# Everything past the named scenarios is drawn from these profiles so a larger
# --patients value still has a realistic mix, with expectations stated the same way.


def _filler_quiet(kind: str):
    def build(f: Factory, sid: int) -> Plan:
        p = _plan(f, sid, "filler", contacts=f.rng.choice([1, 2]))
        p.devices = [f.device(kind, sid)]
        first = day(2, f.rng.randint(1, 14))
        p.readings = f.daily_readings(kind, days_between(first, f.rng.randint(3, 10)))
        for _ in range(f.rng.randint(0, 2)):
            p.notes.append(
                f.note_on(day(2, f.rng.randint(3, 26)), f.rng.choice([120, 180, 300]))
            )
        return p

    return build


def _filler_none(f: Factory, sid: int) -> Plan:
    return _plan(f, sid, "filler", contacts=f.rng.choice([1, 2]))


def _filler_rpm(kind: str):
    def build(f: Factory, sid: int) -> Plan:
        p = _plan(f, sid, "filler", contacts=f.rng.choice([1, 2]))
        count = f.rng.randint(16, 24)
        first = day(2, f.rng.randint(1, 4))  # last day is at most 2025-02-27
        p.devices = [f.device(kind, sid)]
        p.readings = f.daily_readings(kind, days_between(first, count))
        p.expected_codes = _rpm_codes(p.readings)
        return p

    return build


def _filler_time(f: Factory, sid: int) -> Plan:
    p = _plan(f, sid, "filler", contacts=f.rng.choice([1, 2]))
    total = f.rng.choice([1200, 1500, 1800, 2100])
    p.notes = _notes_totalling(f, [day(2, 5), day(2, f.rng.randint(6, 25))], total)
    p.expected_codes = _time_codes(p.notes, total)
    return p


def _filler_time_long(f: Factory, sid: int) -> Plan:
    p = _plan(f, sid, "filler", contacts=f.rng.choice([1, 2]))
    total = f.rng.choice([2400, 2700, 3600, 4500])
    p.notes = _notes_totalling(
        f, [day(2, 4), day(2, 11), day(2, f.rng.randint(12, 25))], total
    )
    p.expected_codes = _time_codes(p.notes, total)
    return p


def _filler_initial_eval(f: Factory, sid: int) -> Plan:
    p = _plan(f, sid, "filler", contacts=f.rng.choice([1, 2]))
    p.notes = [
        f.note_on(
            day(2, f.rng.randint(3, 26)), 300, note_type=IE, upn=cfg.NURSE_PRACTITIONER
        )
    ]
    p.expected_codes = [
        ExpectedCode("99202", p.notes[0].timestamp)
    ]  # forced to 15 minutes
    return p


FILLER_PROFILES: list[tuple[int, Callable[[Factory, int], Plan]]] = [
    (22, _filler_quiet("bg")),
    (12, _filler_quiet("bp")),
    (24, _filler_none),
    (10, _filler_rpm("bg")),
    (6, _filler_rpm("bp")),
    (8, _filler_time),
    (6, _filler_time_long),
    (8, _filler_initial_eval),
]


def build_filler(f: Factory, sid: int) -> Plan:
    weights = [w for w, _ in FILLER_PROFILES]
    builder = f.rng.choices([b for _, b in FILLER_PROFILES], weights)[0]
    return builder(f, sid)
