"""The reconciliation checks. Pure functions over DataFrames so they can be unit
tested with small hand-built inputs. Results carry counts and synthetic or
surrogate IDs only, never field values."""

from collections import Counter

import pandas as pd

from tools.reconcile.model import CheckResult, Loaded, Settings, Source

CAP = 20  # most violation IDs recorded per check

CODES = ["99202", "99453", "99454", "99457", "99458"]

PRIMARY_KEYS = {
    "user": "user_id",
    "patient": "patient_id",
    "patient_address": "patient_address_id",
    "patient_insurance": "patient_insurance_id",
    "medical_necessity": "medical_necessity_id",
    "patient_status_type": "patient_status_type_id",
    "patient_status": "patient_status_id",
    "emergency_contact": "emergency_contact_id",
    "vendor": "vendor_id",
    "device": "device_id",
    "glucose_reading": "glucose_reading_id",
    "blood_pressure_reading": "blood_pressure_reading_id",
    "note_type": "note_type_id",
    "patient_note": "patient_note_id",
    "medical_code_type": "med_code_type_id",
    "medical_code": "med_code_id",
    "medical_code_device": "medical_code_device_id",
}

# Natural keys that should be unique in the loaded data.
UNIQUE_KEYS = {
    "patient": ["sharepoint_id"],
    "device": ["hardware_uuid"],
    "user": ["ms_entra_id", "display_name"],
    "patient_address": ["patient_id"],
    "patient_insurance": ["patient_id"],
    "patient_status": ["patient_id"],
}

# (child table, child column, parent table, parent column)
FOREIGN_KEYS = [
    ("patient", "user_id", "user", "user_id"),
    ("patient_address", "patient_id", "patient", "patient_id"),
    ("patient_insurance", "patient_id", "patient", "patient_id"),
    ("medical_necessity", "patient_id", "patient", "patient_id"),
    ("patient_status", "patient_id", "patient", "patient_id"),
    (
        "patient_status",
        "patient_status_type_id",
        "patient_status_type",
        "patient_status_type_id",
    ),
    ("emergency_contact", "patient_id", "patient", "patient_id"),
    ("device", "patient_id", "patient", "patient_id"),
    ("device", "vendor_id", "vendor", "vendor_id"),
    ("glucose_reading", "device_id", "device", "device_id"),
    ("blood_pressure_reading", "device_id", "device", "device_id"),
    ("patient_note", "patient_id", "patient", "patient_id"),
    ("patient_note", "note_type_id", "note_type", "note_type_id"),
    ("patient_note", "user_id", "user", "user_id"),
    ("medical_code", "patient_id", "patient", "patient_id"),
    ("medical_code", "med_code_type_id", "medical_code_type", "med_code_type_id"),
    ("medical_code_device", "med_code_id", "medical_code", "med_code_id"),
    ("medical_code_device", "device_id", "device", "device_id"),
]

# Assumption: the fields a loaded row must always have. The real schema is not in
# the repository, so this set is reconcile's own, chosen conservatively.
REQUIRED_FIELDS = {
    "patient": ["sharepoint_id", "first_name", "last_name", "date_of_birth"],
    "device": ["hardware_uuid", "patient_id", "vendor_id"],
    "glucose_reading": [
        "device_id",
        "recorded_datetime",
        "received_datetime",
    ],
    "blood_pressure_reading": [
        "device_id",
        "recorded_datetime",
        "received_datetime",
    ],
    "patient_note": ["patient_id", "note_datetime", "note_content"],
    "medical_code": ["patient_id", "med_code_type_id", "timestamp_applied"],
    "medical_code_device": ["med_code_id", "device_id"],
}


def _cap(values) -> list:
    return sorted(values)[:CAP]


def _ids(series: pd.Series) -> set[int]:
    return {int(v) for v in series.dropna()}


def _sid_of_patient_id(loaded: Loaded) -> dict[int, int]:
    p = loaded.tables["patient"]
    return {
        int(a): int(b) for a, b in zip(p["patient_id"], p["sharepoint_id"], strict=True)
    }


def device_counts_by_sharepoint_id(loaded: Loaded) -> pd.Series:
    pt = loaded.tables["patient"][["patient_id", "sharepoint_id"]]
    dv = loaded.tables["device"][["patient_id"]]
    return dv.merge(pt, on="patient_id", how="inner").groupby("sharepoint_id").size()


# -- 1. row conservation --------------------------------------------------------


def check_row_conservation(
    src: Source,
    loaded: Loaded,
    cfg: Settings,
    reasons: dict[int, list[str]],
    derived_expected: dict[str, int],
) -> CheckResult:
    """Every source row is accounted for: loaded, merged, rejected, or dropped for a
    documented reason. Each source shows its equation with real numbers."""
    t = loaded.tables
    export_ids = _ids(src.patients["ID"])
    rejected_ids = set(reasons)
    loaded_ids = _ids(t["patient"]["sharepoint_id"])
    sources: dict[str, dict] = {}

    # patients
    by_reason = Counter(
        code
        for pid, codes in reasons.items()
        for code in codes
        for _ in range(int((src.patients["ID"] == pid).sum()))
    )
    n_rejected = int(src.patients["ID"].isin(rejected_ids).sum())
    n_loaded = len(t["patient"])
    sources["patients"] = {
        "source": len(src.patients),
        "loaded": n_loaded,
        "loaded_from_fanout": 0,
        "duplicates_merged": 0,
        "dispositions": {"PATIENT_REJECTED": n_rejected},
        "rejected_by_reason": dict(sorted(by_reason.items())),
        "unexplained": len(src.patients) - n_loaded - n_rejected,
    }
    missing = export_ids - loaded_ids
    unexplained_ids = missing - rejected_ids
    rejected_but_loaded = rejected_ids & loaded_ids

    # devices: eligibility is the source query's filter (Resupply = 0, known vendors)
    d = src.devices
    eligible = (d["Resupply"].astype(int) == 0) & d["Vendor"].isin(cfg.device_vendors)
    elig = d[eligible]
    no_patient = int((~elig["Patient_ID"].isin(export_ids)).sum())
    patient_rejected = int(elig["Patient_ID"].isin(rejected_ids).sum())
    n_loaded = len(t["device"])
    disp = {
        "EXCLUDED_BY_SOURCE_QUERY": int((~eligible).sum()),
        "NO_PATIENT_IN_EXPORT": no_patient,
        "PATIENT_REJECTED": patient_rejected,
    }
    sources["devices"] = {
        "source": len(d),
        "loaded": n_loaded,
        "loaded_from_fanout": 0,
        "duplicates_merged": 0,
        "dispositions": disp,
        "unexplained": len(d) - n_loaded - sum(disp.values()),
    }

    # readings and notes
    dev_counts = device_counts_by_sharepoint_id(loaded)
    for name, frame, table, id_col, time_col, needs_device in [
        (
            "glucose readings",
            src.glucose,
            "glucose_reading",
            "SharePoint_ID",
            "Time_Recorded",
            True,
        ),
        (
            "blood pressure readings",
            src.bp,
            "blood_pressure_reading",
            "SharePoint_ID",
            "Time_Recorded",
            True,
        ),
        (
            "patient notes",
            src.notes,
            "patient_note",
            "SharePoint_ID",
            "TimeStamp",
            False,
        ),
    ]:
        when = pd.to_datetime(frame[time_col])
        outside = (when < cfg.extract_start) | (when > cfg.extract_end)
        inside = frame[~outside]
        no_patient = int((~inside[id_col].isin(export_ids)).sum())
        rejected = int(inside[id_col].isin(rejected_ids).sum())
        rest = inside[
            inside[id_col].isin(export_ids) & ~inside[id_col].isin(rejected_ids)
        ]
        disp = {
            "OUTSIDE_EXTRACT_WINDOW": int(outside.sum()),
            "NO_PATIENT_IN_EXPORT": no_patient,
            "PATIENT_REJECTED": rejected,
        }
        fanout = 0
        if needs_device:
            counts = rest[id_col].map(dev_counts).fillna(0).astype(int)
            disp["NO_DEVICE_ON_FILE"] = int((counts == 0).sum())
            fanout = int((counts[counts > 0] - 1).sum())
        n_loaded = len(t[table])
        sources[name] = {
            "source": len(frame),
            "loaded": n_loaded,
            "loaded_from_fanout": fanout,
            "duplicates_merged": 0,
            "dispositions": disp,
            "unexplained": len(frame) - (n_loaded - fanout) - sum(disp.values()),
        }

    sources["users"] = {
        "source": len(src.users),
        "loaded": len(t["user"]),
        "loaded_from_fanout": 0,
        "duplicates_merged": 0,
        "dispositions": {},
        "unexplained": len(src.users) - len(t["user"]),
    }

    derived = {
        table: {
            "expected": expected,
            "loaded": len(t[table]),
            "unexplained": len(t[table]) - expected,
        }
        for table, expected in derived_expected.items()
    }

    unexplained_total = sum(abs(s["unexplained"]) for s in sources.values()) + sum(
        abs(v["unexplained"]) for v in derived.values()
    )
    ok = not unexplained_total and not unexplained_ids and not rejected_but_loaded
    bad = [k for k, s in sources.items() if s["unexplained"]] + [
        k for k, v in derived.items() if v["unexplained"]
    ]
    return CheckResult(
        "1 row conservation",
        ok,
        "every source row is accounted for"
        if ok
        else f"unexplained differences in: {', '.join(bad) or 'patient IDs'}",
        {"sources": sources, "derived_tables": derived},
        _cap(unexplained_ids | rejected_but_loaded),
    )


# -- 2. key integrity -----------------------------------------------------------


def check_key_uniqueness(loaded: Loaded) -> CheckResult:
    t = loaded.tables
    problems: dict[str, int] = {}
    violations: list = []
    checked = 0
    for table, pk in PRIMARY_KEYS.items():
        if table in t and pk in t[table]:
            checked += 1
            dup = t[table][pk].duplicated(keep=False)
            if dup.any():
                problems[f"{table}.{pk}"] = int(dup.sum())
                violations += [f"{table}:{int(v)}" for v in t[table].loc[dup, pk]]
    for table, cols in UNIQUE_KEYS.items():
        for col in cols:
            checked += 1
            frame = t[table]
            dup = frame[col].notna() & frame[col].duplicated(keep=False)
            if dup.any():
                problems[f"{table}.{col}"] = int(dup.sum())
                pk = PRIMARY_KEYS[table]
                violations += [f"{table}:{int(v)}" for v in frame.loc[dup, pk]]
    ok = not problems
    return CheckResult(
        "2a key uniqueness",
        ok,
        f"{checked} keys unique" if ok else f"duplicate keys: {', '.join(problems)}",
        {"keys_checked": checked, "rows_involved_by_key": problems},
        _cap(violations),
    )


def check_foreign_keys(loaded: Loaded) -> CheckResult:
    t = loaded.tables
    problems: dict[str, int] = {}
    violations: list = []
    for child, col, parent, pcol in FOREIGN_KEYS:
        values = t[child][col].dropna()  # NULL means "not set", not an orphan
        orphan = ~values.isin(t[parent][pcol])
        if orphan.any():
            problems[f"{child}.{col} -> {parent}"] = int(orphan.sum())
            pk = PRIMARY_KEYS[child]
            violations += [
                f"{child}:{int(v)}" for v in t[child].loc[values[orphan].index, pk]
            ]
    ok = not problems
    return CheckResult(
        "2b foreign keys",
        ok,
        f"{len(FOREIGN_KEYS)} relationships have no orphans"
        if ok
        else f"orphans: {', '.join(problems)}",
        {
            "relationships_checked": len(FOREIGN_KEYS),
            "orphans_by_relationship": problems,
        },
        _cap(violations),
    )


def check_required_fields(loaded: Loaded) -> CheckResult:
    t = loaded.tables
    problems: dict[str, int] = {}
    violations: list = []
    for table, cols in REQUIRED_FIELDS.items():
        for col in cols:
            missing = t[table][col].isna()
            if missing.any():
                problems[f"{table}.{col}"] = int(missing.sum())
                violations += [
                    f"{table}:{int(v)}"
                    for v in t[table].loc[missing, PRIMARY_KEYS[table]]
                ]
    ok = not problems
    return CheckResult(
        "2c required fields",
        ok,
        "no required field is empty after load"
        if ok
        else f"empty required fields: {', '.join(problems)}",
        {
            "fields_checked": sum(len(c) for c in REQUIRED_FIELDS.values()),
            "empty_by_field": problems,
        },
        _cap(violations),
    )


# -- 3. cross-source consistency -------------------------------------------------


def check_cross_source(
    src: Source, loaded: Loaded, rejected_ids: set[int]
) -> CheckResult:
    """A patient present in more than one source resolves to one loaded entity."""
    where = {
        "export": _ids(src.patients["ID"]),
        "devices": _ids(src.devices["Patient_ID"]),
        "glucose": _ids(src.glucose["SharePoint_ID"]),
        "blood pressure": _ids(src.bp["SharePoint_ID"]),
        "notes": _ids(src.notes["SharePoint_ID"]),
    }
    seen = Counter(i for ids in where.values() for i in ids)
    multi = {i for i, n in seen.items() if n >= 2 and i in where["export"]}
    loadable = multi - rejected_ids
    loaded_count = loaded.tables["patient"]["sharepoint_id"].value_counts()
    bad = {i for i in loadable if int(loaded_count.get(i, 0)) != 1}
    dup_in_export = {
        int(i) for i, n in src.patients["ID"].value_counts().items() if n > 1
    }
    ok = not bad and not dup_in_export
    return CheckResult(
        "3 cross-source consistency",
        ok,
        f"{len(loadable)} patients in 2+ sources each resolve to one loaded entity"
        if ok
        else f"{len(bad | dup_in_export)} patients do not resolve to exactly one entity",
        {
            "patients_in_2plus_sources": len(multi),
            "excluded_because_rejected": len(multi & rejected_ids),
            "resolving_to_exactly_one": len(loadable) - len(bad),
            "duplicate_ids_in_export": len(dup_in_export),
        },
        _cap(bad | dup_in_export),
    )


# -- 4. rejection accounting -----------------------------------------------------


def check_rejection_accounting(
    src: Source,
    loaded: Loaded,
    reasons: dict[int, list[str]],
    agrees: bool,
) -> CheckResult:
    """Every patient missing after load has a reason code, and no loaded patient
    has one. Also confirms the reason derivation agrees with the pipeline."""
    export_ids = _ids(src.patients["ID"])
    loaded_ids = _ids(loaded.tables["patient"]["sharepoint_id"])
    missing = export_ids - loaded_ids
    rejected = set(reasons)
    no_reason = missing - rejected
    reason_but_loaded = rejected & loaded_ids
    by_reason = Counter(code for pid in rejected for code in reasons[pid])
    ok = agrees and not no_reason and not reason_but_loaded
    if not agrees:
        summary = "reason codes disagree with the pipeline's own constraint check"
    elif ok:
        summary = f"{len(rejected)} rejected patients, each with a reason code"
    else:
        summary = (
            f"{len(no_reason)} patients missing without a reason, "
            f"{len(reason_but_loaded)} loaded despite a reason"
        )
    return CheckResult(
        "4 rejection accounting",
        ok,
        summary,
        {
            "rejected_patients": len(rejected),
            "by_reason": dict(sorted(by_reason.items())),
            "reason_derivation_agrees_with_pipeline": agrees,
        },
        _cap(no_reason | reason_but_loaded),
    )


# -- 5. billing lineage ------------------------------------------------------------

# Which source rows back each code, selected from the rules table in
# docs/billing-rules.md. This selects what to trace; it does not decide whether a
# patient qualifies (the manifest comparison covers that).
IE_TYPE = "Initial Evaluation"


def _billed_codes(loaded: Loaded) -> pd.DataFrame:
    t = loaded.tables
    codes = (
        t["medical_code"]
        .merge(
            t["medical_code_type"][["med_code_type_id", "name"]],
            on="med_code_type_id",
            how="left",
        )
        .merge(
            t["patient"][["patient_id", "sharepoint_id"]], on="patient_id", how="left"
        )
        .rename(columns={"name": "code"})
    )
    # A code whose patient row is missing keeps a -1 placeholder rather than failing.
    codes["sharepoint_id"] = codes["sharepoint_id"].fillna(-1).astype(int)
    return codes


def _sample(rows: pd.DataFrame, sample: int | None) -> pd.DataFrame:
    rows = rows.sort_values(["sharepoint_id", "code", "med_code_id"]).reset_index(
        drop=True
    )
    if not sample or sample >= len(rows):
        return rows
    step = len(rows) / sample
    return rows.iloc[[int(i * step) for i in range(sample)]]


def check_billing_lineage(
    src: Source,
    loaded: Loaded,
    cfg: Settings,
    expected_codes: list[tuple[int, str, str]] | None,
    sample: int | None = None,
) -> CheckResult:
    t = loaded.tables
    end = pd.Timestamp(cfg.report_end)
    window_30 = end - pd.Timedelta(days=30)
    window_month = end - pd.DateOffset(months=1)

    sid = _sid_of_patient_id(loaded)
    # Readings link to a patient only through their device (the pipeline does not
    # write a patient key on reading rows), so resolve patient via device.
    device_sid = {
        int(d): sid.get(int(p))
        for d, p in zip(
            t["device"]["device_id"], t["device"]["patient_id"], strict=True
        )
    }
    readings = []
    for kind, table in (
        ("glucose", "glucose_reading"),
        ("bp", "blood_pressure_reading"),
    ):
        r = t[table].copy()
        r["sid"] = r["device_id"].map(device_sid)
        r["kind"] = kind
        r["when"] = pd.to_datetime(r["received_datetime"])
        readings.append(
            r[["sid", "kind", "recorded_datetime", "received_datetime", "when"]]
        )
    readings_db = pd.concat(readings, ignore_index=True)
    source_readings = {
        (kind, int(i), pd.Timestamp(a), pd.Timestamp(b))
        for kind, frame in (("glucose", src.glucose), ("bp", src.bp))
        for i, a, b in zip(
            frame["SharePoint_ID"],
            frame["Time_Recorded"],
            frame["Time_Recieved"],
            strict=True,
        )
    }
    notes_db = t["patient_note"].merge(
        t["note_type"][["note_type_id", "name"]], on="note_type_id", how="left"
    )
    notes_db["sid"] = notes_db["patient_id"].map(sid)
    notes_db["when"] = pd.to_datetime(notes_db["note_datetime"])
    source_notes = {
        (int(i), pd.Timestamp(w), str(u))
        for i, w, u in zip(
            src.notes["SharePoint_ID"],
            src.notes["TimeStamp"],
            src.notes["AZURE_UPN"],
            strict=True,
        )
    }

    codes = _sample(_billed_codes(loaded), sample)
    stats = Counter()
    violations: list = []
    for row in codes.itertuples():
        code, pid = row.code, int(row.sharepoint_id)
        stamp = pd.Timestamp(row.timestamp_applied)
        if code in ("99453", "99454"):
            backing = readings_db[readings_db["sid"] == pid]
            if code == "99454":
                backing = backing[backing["when"] >= window_30]
            in_source = all(
                (
                    r.kind,
                    pid,
                    pd.Timestamp(r.recorded_datetime),
                    pd.Timestamp(r.received_datetime),
                )
                in source_readings
                for r in backing.itertuples()
            )
            anchors = {g["when"].max() for _, g in backing.groupby("kind")}
        else:
            backing = notes_db[notes_db["sid"] == pid]
            if code == "99202":
                backing = backing[backing["name"] == IE_TYPE]
            else:
                backing = backing[backing["when"] >= window_month]
            in_source = all(
                (pid, pd.Timestamp(r.note_datetime), str(r.temp_user)) in source_notes
                for r in backing.itertuples()
            )
            anchors = {backing["when"].max()} if len(backing) else set()
        stats["traced"] += 1
        has_rows = len(backing) > 0
        anchored = has_rows and stamp in anchors
        stats["with_backing_rows"] += has_rows
        stats["backing_rows_in_source"] += bool(has_rows and in_source)
        stats["stamped_at_latest_backing_row"] += bool(anchored)
        if not (has_rows and in_source and anchored):
            violations.append(f"{pid}:{code}")

    manifest = {"compared": expected_codes is not None}
    manifest_bad: list = []
    if expected_codes is not None:
        billed = Counter(
            (
                int(r.sharepoint_id),
                r.code,
                pd.Timestamp(r.timestamp_applied).strftime("%Y-%m-%d %H:%M:%S"),
            )
            for r in _billed_codes(loaded).itertuples()
        )
        expected = Counter(expected_codes)
        missing = expected - billed
        unexpected = billed - expected
        manifest.update(
            {
                "billed": sum(billed.values()),
                "expected": sum(expected.values()),
                "expected_but_not_billed": sum(missing.values()),
                "billed_but_not_expected": sum(unexpected.values()),
            }
        )
        manifest_bad = [f"{p}:{c}" for (p, c, _), _n in (missing + unexpected).items()]
    ok = not violations and not manifest_bad
    return CheckResult(
        "5 billing lineage",
        ok,
        f"{stats['traced']} billed codes traced to source rows; manifest agrees"
        if ok
        else f"{len(set(violations) | set(manifest_bad))} codes fail lineage or disagree with the manifest",
        {
            "codes_in_database": int(len(_billed_codes(loaded))),
            "codes_traced": stats["traced"],
            "with_backing_rows": stats["with_backing_rows"],
            "backing_rows_in_source": stats["backing_rows_in_source"],
            "stamped_at_latest_backing_row": stats["stamped_at_latest_backing_row"],
            "sampled": bool(sample and sample < len(_billed_codes(loaded))),
            "manifest": manifest,
        },
        _cap(set(violations) | set(manifest_bad)),
    )


# -- 6. report-level totals -------------------------------------------------------


def check_report_totals(loaded: Loaded, cfg: Settings) -> CheckResult:
    """Report counts equal the database counts for the same window and joins."""
    t = loaded.tables
    codes = _billed_codes(loaded)
    stamp = pd.to_datetime(codes["timestamp_applied"])
    in_window = codes[(stamp >= cfg.report_start) & (stamp <= cfg.report_end)]
    # The report inner-joins address, insurance and diagnosis rows.
    complete = (
        set(t["patient_address"]["patient_id"])
        & set(t["patient_insurance"]["patient_id"])
        & set(t["medical_necessity"]["patient_id"])
    )
    in_report = in_window[in_window["patient_id"].isin(complete)]
    db_by_code = {c: int((in_report["code"] == c).sum()) for c in CODES}
    rep = loaded.report
    rep_by_code = {c: int(rep[c].sum()) if c in rep else 0 for c in CODES}
    db_rows = int(
        in_report.assign(day=pd.to_datetime(in_report["timestamp_applied"]).dt.date)
        .groupby(["sharepoint_id", "day"])
        .ngroups
    )
    unknown_ids = set(rep["ID"].astype(int)) - _ids(t["patient"]["sharepoint_id"])
    diff = {
        c: rep_by_code[c] - db_by_code[c]
        for c in CODES
        if rep_by_code[c] != db_by_code[c]
    }
    ok = not diff and len(rep) == db_rows and not unknown_ids
    return CheckResult(
        "6 report totals",
        ok,
        f"report equals the database: {len(rep)} rows, {sum(rep_by_code.values())} codes"
        if ok
        else f"report differs from the database (codes: {diff or 'equal'}, rows {len(rep)} vs {db_rows})",
        {
            "report_rows": len(rep),
            "database_rows": db_rows,
            "report_by_code": rep_by_code,
            "database_by_code": db_by_code,
            "codes_outside_window_or_report_joins": int(len(codes) - len(in_report)),
        },
        _cap(unknown_ids),
    )
