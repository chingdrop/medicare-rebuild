"""One-command demo runner: load synthetic data, run the real pipeline, check the result.

Run it through `make demo` (see docs/demo.md). What it does, in order:

1. Create two throwaway databases on the dev SQL Server container: a "legacy" source
   database (the tables the pipeline reads) and a GPS target database.
2. Load the generated CSV files into the legacy tables; create the GPS tables
   (reconstructed, see schema.py) and apply the repo's stored procedures verbatim.
3. Run the unmodified pipeline: `import_all_data`, the two patient-note steps that
   `import_all_data` does not call, then `create_billing_report`.
4. Compare what came out with the manifest the generator wrote.

Deviation from `medicare_rebuild.__main__.main()` (documented in docs/demo.md):
* `main()` never imports patient notes (dropped in an earlier refactor), so the runner
  calls `get_patient_note_data` / `import_patient_note_data` itself and re-runs the two
  note UPDATE statements. Without this, 99202/99457/99458 could not be demonstrated.
* The Microsoft Graph step is replaced by a local-file stand-in so nothing touches the
  network. `DataImporter.get_user_data` itself runs unmodified.

The SQL Server credentials below are the throwaway ones already in docker-compose.yml.
They are dev-only and protect nothing.
"""

import argparse
import hashlib
import json
import logging
import os
import shutil
import sys
import time
import warnings
from collections import Counter
from contextlib import chdir
from dataclasses import dataclass, field
from pathlib import Path
from unittest import mock

import pandas as pd
import pyodbc

from tools.synthetic_data import config as cfg
from tools.synthetic_data import schema
from tools.synthetic_data.generator import FILES

# Dev-only defaults; identical to docker-compose.yml and tests/integration/conftest.py.
DB_HOST = os.environ.get("INTEGRATION_DB_HOST", "localhost")
DB_PORT = os.environ.get("INTEGRATION_DB_PORT", "14330")
DB_USER = os.environ.get("INTEGRATION_DB_USER", "sa")
DB_PASSWORD = os.environ.get("INTEGRATION_DB_PASSWORD", "IntegrationTest_Passw0rd!")
# A pooled connection handed back by wait_for_server() can keep autocommit off, which
# makes CREATE DATABASE fail; pooling buys nothing for a one-shot script.
pyodbc.pooling = False

GPS_DB = "medicare_demo_gps"
LEGACY_DB = "medicare_demo_legacy"

LOADED_TABLES = [
    "user",
    "patient",
    "patient_address",
    "patient_insurance",
    "medical_necessity",
    "patient_status",
    "emergency_contact",
    "device",
    "glucose_reading",
    "blood_pressure_reading",
    "patient_note",
]


# -- the local stand-in for Microsoft Graph -------------------------------------


def local_user_source(users_file: Path) -> type:
    """A class with the same interface as MSGraphApi that reads users from a file."""

    class LocalUserSource:
        def __init__(self, tenant_id, client_id, client_secret, logger=None):
            self.logger = logger

        def request_access_token(self) -> None:
            return None  # nothing to authenticate against

        def get_group_members(self, group_id: str) -> dict:
            return json.loads(users_file.read_text())

    return LocalUserSource


# -- database plumbing ------------------------------------------------------------


def _connect_str(database: str | None = None) -> str:
    base = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={DB_HOST},{DB_PORT};UID={DB_USER};PWD={DB_PASSWORD};"
        "TrustServerCertificate=yes"
    )
    return base + (f";DATABASE={database}" if database else "")


def wait_for_server(timeout: float = 240.0) -> None:
    deadline = time.monotonic() + timeout
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            pyodbc.connect(_connect_str(), timeout=5).close()
            return
        except Exception as exc:  # the container is still starting
            last = exc
            time.sleep(3)
    raise RuntimeError(
        f"SQL Server not reachable at {DB_HOST}:{DB_PORT} after {timeout:.0f}s "
        f"(is the container up? try `docker compose up -d`): {last}"
    )


def _run(database: str | None, statements: list[str]) -> None:
    conn = pyodbc.connect(_connect_str(database))
    conn.autocommit = True  # CREATE/DROP DATABASE cannot run inside a transaction
    try:
        cur = conn.cursor()
        for statement in statements:
            cur.execute(statement)
    finally:
        conn.close()


def prepare_databases(data_dir: Path) -> None:
    from medicare_rebuild.utils.db_utils import DatabaseManager

    for name in (GPS_DB, LEGACY_DB):
        _run(
            None,
            [
                f"IF DB_ID('{name}') IS NOT NULL BEGIN "
                f"ALTER DATABASE {name} SET SINGLE_USER WITH ROLLBACK IMMEDIATE; "
                f"DROP DATABASE {name}; END",
                f"CREATE DATABASE {name}",
            ],
        )
    _run(
        GPS_DB,
        [
            *schema.GPS_TABLES,
            *schema.seed_statements(),
            *(schema.procedure_sql(p) for p in schema.PROCEDURES),
        ],
    )
    _run(LEGACY_DB, schema.LEGACY_TABLES)

    legacy = DatabaseManager()
    legacy.create_engine(DB_USER, DB_PASSWORD, f"{DB_HOST},{DB_PORT}", LEGACY_DB)
    try:
        for table, (csv, date_cols) in schema.LEGACY_LOADS.items():
            df = pd.read_csv(
                data_dir / "legacy" / csv,
                parse_dates=date_cols,
                dtype={"Recording_Time": "str"},
            )
            legacy.to_sql(df, table, if_exists="append")
    finally:
        legacy.close()


# -- running the real pipeline ------------------------------------------------------


class _ErrorCounter(logging.Handler):
    """The pipeline's DatabaseManager logs and swallows SQL errors; count them so a
    silently failing stored procedure fails the demo instead of passing quietly."""

    def __init__(self) -> None:
        super().__init__(level=logging.ERROR)
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


def run_pipeline(data_dir: Path, work_dir: Path) -> list[str]:
    """Run the pipeline with the demo's environment. Returns logged error messages."""
    env = {
        "GPS_SQL_USERNAME": DB_USER,
        "GPS_SQL_PASSWORD": DB_PASSWORD,
        "GPS_SQL_HOST": f"{DB_HOST},{DB_PORT}",
        "GPS_SQL_DB": GPS_DB,
        "LEGACY_SQL_USERNAME": DB_USER,
        "LEGACY_SQL_PASSWORD": DB_PASSWORD,
        "LEGACY_SQL_HOST": f"{DB_HOST},{DB_PORT}",
        "LEGACY_SQL_SP_NOTES": LEGACY_DB,
        "LEGACY_SQL_SP_TIME": LEGACY_DB,
        "LEGACY_SQL_SP_FULFILLMENT": LEGACY_DB,
        "LEGACY_SQL_SP_READINGS": LEGACY_DB,
        # Never used: the Graph client is replaced below. Obvious placeholders.
        "AZURE_TENANT_ID": "demo-placeholder-tenant",
        "AZURE_CLIENT_ID": "demo-placeholder-client",
        "AZURE_CLIENT_SECRET": "demo-placeholder-secret",
        "AZURE_GROUP_ID": "00000000-0000-0000-0000-000000000000",
    }
    logger = logging.getLogger("medicare_demo")
    logger.setLevel(logging.WARNING)
    logger.handlers.clear()
    counter = _ErrorCounter()
    logger.addHandler(counter)
    logger.addHandler(logging.StreamHandler(sys.stderr))
    logger.propagate = False

    (work_dir / "data").mkdir(parents=True, exist_ok=True)
    shutil.copy(data_dir / FILES["patients"], work_dir / "data" / "Patient_Export.csv")

    from medicare_rebuild import __main__ as pipeline
    from medicare_rebuild.queries import update_patient_note_stmt, update_user_note_stmt

    start, end = (
        cfg.IMPORT_START.strftime("%Y-%m-%d"),
        cfg.IMPORT_END.strftime("%Y-%m-%d"),
    )
    r_start, r_end = (
        cfg.REPORT_START.strftime("%Y-%m-%d"),
        cfg.REPORT_END.strftime("%Y-%m-%d"),
    )
    warnings.filterwarnings("ignore")
    graph = local_user_source(data_dir / FILES["users"])
    with (
        mock.patch.dict(os.environ, env),
        mock.patch.object(pipeline, "MSGraphApi", graph),
        chdir(work_dir),
    ):
        pipeline.import_all_data(start, end, logger=logger)
        # The two note steps main() no longer runs (see module docstring).
        importer = pipeline.DataImporter(start, end, logger=logger)
        importer.import_patient_note_data(importer.get_patient_note_data())
        importer.gps.execute_query(update_patient_note_stmt)
        importer.gps.execute_query(update_user_note_stmt)
        importer.close_db()
        pipeline.create_billing_report(r_start, r_end, logger=logger)
    return counter.messages


# -- what actually happened ---------------------------------------------------------


@dataclass
class Actual:
    source_rows: dict[str, int]
    loaded_rows: dict[str, int]
    export_ids: set[int]
    loaded_patient_ids: set[int]
    applied_codes: Counter
    report_rows: list[dict]
    unresolved: dict[str, int]
    pipeline_errors: list[str]
    report_hash: str = ""


def _read(sql: str, database: str) -> pd.DataFrame:
    conn = pyodbc.connect(_connect_str(database))
    try:
        return pd.read_sql(sql, conn)  # type: ignore[arg-type]
    finally:
        conn.close()


def _count(table: str, database: str) -> int:
    # `table` is always a constant from this module, never user input.
    query = f"SELECT COUNT(*) AS n FROM [{table}]"  # noqa: S608
    return int(_read(query, database)["n"].iloc[0])


def collect_actual(data_dir: Path, report_path: Path, errors: list[str]) -> Actual:
    source = {
        "users": len(json.loads((data_dir / FILES["users"]).read_text())["value"]),
        "patients": len(pd.read_csv(data_dir / FILES["patients"])),
        "devices": _count("Fulfillment_All", LEGACY_DB),
        "glucose_reading": _count("Glucose_Readings", LEGACY_DB),
        "blood_pressure_reading": _count("Blood_Pressure_Readings", LEGACY_DB),
        "patient_note": _count("Medical_Notes", LEGACY_DB),
        "time_log": _count("Time_Log", LEGACY_DB),
    }
    loaded = {t: _count(t, GPS_DB) for t in LOADED_TABLES}
    ids = _read("SELECT sharepoint_id FROM patient", GPS_DB)["sharepoint_id"]
    codes = _read(
        "SELECT p.sharepoint_id AS id, mct.name AS code, mc.timestamp_applied AS applied_at "
        "FROM medical_code mc "
        "JOIN patient p ON p.patient_id = mc.patient_id "
        "JOIN medical_code_type mct ON mct.med_code_type_id = mc.med_code_type_id",
        GPS_DB,
    )
    applied = Counter(
        (
            int(r.id),
            str(r.code),
            pd.Timestamp(r.applied_at).strftime("%Y-%m-%d %H:%M:%S"),
        )
        for r in codes.itertuples()
    )
    unresolved = {
        "note types": int(
            _read(
                "SELECT COUNT(*) AS n FROM patient_note WHERE note_type_id IS NULL",
                GPS_DB,
            )["n"].iloc[0]
        ),
        "note authors": int(
            _read(
                "SELECT COUNT(*) AS n FROM patient_note WHERE user_id IS NULL", GPS_DB
            )["n"].iloc[0]
        ),
        "patient coaches": int(
            _read("SELECT COUNT(*) AS n FROM patient WHERE user_id IS NULL", GPS_DB)[
                "n"
            ].iloc[0]
        ),
        "patient statuses": int(
            _read(
                "SELECT COUNT(*) AS n FROM patient_status WHERE patient_status_type_id IS NULL",
                GPS_DB,
            )["n"].iloc[0]
        ),
    }

    report = pd.read_excel(report_path)
    report.columns = [str(c) for c in report.columns]
    rows = []
    for r in report.to_dict("records"):
        rows.append(
            {
                "ID": int(r["ID"]),
                "DateOfService": pd.Timestamp(r["DateOfService"]).strftime("%Y-%m-%d"),
                **{c: int(r[c]) for c in cfg.BILLING_CODES},
            }
        )
    rows.sort(key=lambda r: (r["ID"], r["DateOfService"]))
    digest = hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest()[:16]
    return Actual(
        source,
        loaded,
        set(pd.read_csv(data_dir / FILES["patients"])["ID"]),
        {int(i) for i in ids},
        applied,
        rows,
        unresolved,
        errors,
        digest,
    )


# -- comparison with the manifest ---------------------------------------------------


@dataclass
class Check:
    name: str
    ok: bool
    detail: str = ""


@dataclass
class DemoResult:
    actual: Actual
    checks: list[Check]
    scenarios_ok: dict[str, bool]
    summary: str = ""
    report_path: Path | None = None
    seconds: float = 0.0
    manifest: dict = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return all(c.ok for c in self.checks)


def _equal(name: str, expected, actual) -> Check:
    ok = expected == actual
    return Check(name, ok, "" if ok else f"expected {expected}, got {actual}")


def compare(manifest: dict, a: Actual) -> tuple[list[Check], dict[str, bool]]:
    e = manifest["expected"]
    checks: list[Check] = []

    for key, n in e["source_rows"].items():
        if key in a.source_rows:
            checks.append(_equal(f"source rows: {key}", n, a.source_rows[key]))
    for table, n in e["loaded_rows"].items():
        checks.append(_equal(f"loaded rows: {table}", n, a.loaded_rows[table]))

    rejected = {r["id"] for r in e["rejected_patients"]}
    checks.append(
        _equal(
            "rejected patients",
            sorted(rejected),
            sorted(a.export_ids - a.loaded_patient_ids),
        )
    )

    expected_codes = Counter(
        (c["id"], c["code"], c["applied_at"]) for c in e["applied_codes"]
    )
    checks.append(
        Check(
            "billing codes applied (patient, code, timestamp)",
            expected_codes == a.applied_codes,
            f"missing {sum((expected_codes - a.applied_codes).values())}, "
            f"unexpected {sum((a.applied_codes - expected_codes).values())}",
        )
    )
    exp_report = sorted(e["report_rows"], key=lambda r: (r["ID"], r["DateOfService"]))
    checks.append(
        Check(
            "billing report rows",
            exp_report == a.report_rows,
            f"expected {len(exp_report)} rows, got {len(a.report_rows)}",
        )
    )
    for what, n in a.unresolved.items():
        checks.append(_equal(f"unresolved {what}", 0, n))
    checks.append(
        Check(
            "no SQL errors logged by the pipeline",
            not a.pipeline_errors,
            f"{len(a.pipeline_errors)} error(s); first: {a.pipeline_errors[0][:160]}"
            if a.pipeline_errors
            else "",
        )
    )

    scenarios: dict[str, bool] = {}
    by_id_actual: dict[int, Counter] = {}
    for (pid, code, at), n in a.applied_codes.items():
        by_id_actual.setdefault(pid, Counter())[(code, at)] += n
    report_by_id: dict[int, Counter] = {}
    for r in a.report_rows:
        c = report_by_id.setdefault(r["ID"], Counter())
        for code in cfg.BILLING_CODES:
            if r[code]:
                c[(code, r["DateOfService"])] += r[code]
    for s in manifest["scenarios"]:
        pid = s["patient_id"]
        want = Counter((c["code"], c["applied_at"]) for c in s["expected_codes"])
        want_report = Counter(
            (c["code"], c["applied_at"][:10])
            for c in s["expected_codes"]
            if c["in_report"]
        )
        ok = (
            want == by_id_actual.get(pid, Counter())
            and want_report == report_by_id.get(pid, Counter())
            and (pid in a.loaded_patient_ids)
            == (s["in_export"] and s["rejected_reason"] is None)
        )
        scenarios[s["key"]] = ok
    checks.append(
        Check(
            "every named scenario behaves as designed",
            all(scenarios.values()),
            "failing: " + ", ".join(k for k, v in scenarios.items() if not v),
        )
    )
    return checks, scenarios


# -- presentation -------------------------------------------------------------------


_GROUPS = [
    ("source rows: ", "source row counts"),
    ("loaded rows: ", "loaded row counts"),
    ("unresolved ", "references resolved (note types, authors, coaches, statuses)"),
]


def _check_lines(checks: list[Check], manifest: dict, a: Actual) -> list[str]:
    """One line per check, except the many per-table ones, which are rolled up."""
    out, done = [], set()
    for c in checks:
        group = next(((p, label) for p, label in _GROUPS if c.name.startswith(p)), None)
        if group is None:
            name = c.name
            if c.name.startswith("every named scenario"):
                total = len(manifest["scenarios"])
                name = f"named scenarios behave as designed ({total} scenarios)"
            out.append(
                f"  [{'PASS' if c.ok else 'FAIL'}] {name}"
                + ("" if c.ok else f" - {c.detail}")
            )
            continue
        prefix, label = group
        if prefix in done:
            continue
        done.add(prefix)
        members = [m for m in checks if m.name.startswith(prefix)]
        bad = [m for m in members if not m.ok]
        out.append(
            f"  [{'PASS' if not bad else 'FAIL'}] {label} ({len(members)} checks)"
        )
        out.extend(f"         {m.name} - {m.detail}" for m in bad)
    return out


def render_summary(manifest: dict, a: Actual, checks: list[Check]) -> str:
    e = manifest["expected"]
    gen = manifest["generator"]
    lines = [
        f"Synthetic demo - seed {gen['seed']}, {a.source_rows['patients']} patients, "
        f"{len(manifest['scenarios'])} named scenarios",
        "",
        "Rows: source -> loaded",
    ]
    pairs = [
        ("users", "users", "user"),
        ("patients", "patients", "patient"),
        ("devices", "devices", "device"),
        ("glucose readings", "glucose_reading", "glucose_reading"),
        ("blood pressure readings", "blood_pressure_reading", "blood_pressure_reading"),
        ("patient notes", "patient_note", "patient_note"),
    ]
    for label, src, dst in pairs:
        lines.append(f"  {label:<25}{a.source_rows[src]:>6} -> {a.loaded_rows[dst]:>5}")
    lines += [
        "",
        f"Rejected patients ({len(e['rejected_patients'])}) - reasons are from the manifest;",
    ]
    lines.append(
        "the pipeline drops these rows silently, along with their devices, readings and notes:"
    )
    for r in e["rejected_patients"]:
        lines.append(f"  ID {r['id']}: {r['reason']}")
    other: dict[str, list[str]] = {}
    for x in e["excluded_rows"]:
        if x["table"] == "patient" or x["reason"].startswith("patient rejected"):
            continue
        other.setdefault(x["reason"], []).append(
            f"{x['count']} {x['table'].replace('_', ' ')}"
        )
    lines.append("Other rows not loaded:")
    for reason, parts in other.items():
        lines.append(f"  {reason}: {', '.join(parts)}")
    if e["multi_device_patients"]:
        lines.append(
            f"Rows loaded twice: readings for the {e['multi_device_patients']} multi-device patient "
            "are duplicated once per device (known limitation, see docs/demo.md)"
        )
    d = e["duplicates"]
    lines += [
        "",
        f"Duplicates: {d['duplicate_reading_rows']} duplicate reading rows and "
        f"{d['duplicate_note_rows']} duplicate note row, loaded as-is (the pipeline does not merge them)",
        "",
        "Billing codes        applied   in report",
    ]
    applied = Counter()
    for (_, code, _), n in a.applied_codes.items():
        applied[code] += n
    reported = Counter()
    for r in a.report_rows:
        for code in cfg.BILLING_CODES:
            reported[code] += r[code]
    for code in cfg.BILLING_CODES:
        lines.append(f"  {code}              {applied[code]:>6}  {reported[code]:>9}")
    lines += [
        f"  report rows: {len(a.report_rows)}   report content hash: {a.report_hash}",
        "",
        "Checks against the manifest:",
    ]
    lines += _check_lines(checks, manifest, a)
    verdict = "PASS" if all(c.ok for c in checks) else "FAIL"
    lines += [
        "",
        f"RESULT: {verdict} ({sum(c.ok for c in checks)}/{len(checks)} checks)",
    ]
    return "\n".join(lines) + "\n"


def run_demo(data_dir: Path, output_dir: Path) -> DemoResult:
    started = time.monotonic()
    # The pipeline resolves its paths against the working directory, which run_pipeline
    # changes, so everything the runner touches has to be absolute.
    data_dir, output_dir = data_dir.resolve(), output_dir.resolve()
    manifest = json.loads((data_dir / FILES["manifest"]).read_text())
    if output_dir.exists():
        shutil.rmtree(output_dir)
    work = output_dir / "work"
    work.mkdir(parents=True)

    wait_for_server()
    prepare_databases(data_dir)
    errors = run_pipeline(data_dir, work)

    report = output_dir / "Billing_Report.xlsx"
    shutil.copy(work / "data" / "Billing_Report.xlsx", report)
    actual = collect_actual(data_dir, report, errors)
    checks, scenarios = compare(manifest, actual)
    summary = render_summary(manifest, actual, checks)
    (output_dir / "summary.txt").write_text(summary)
    (output_dir / "checks.json").write_text(
        json.dumps([c.__dict__ for c in checks], indent=2) + "\n"
    )
    return DemoResult(
        actual, checks, scenarios, summary, report, time.monotonic() - started, manifest
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m tools.synthetic_data.demo")
    parser.add_argument("--data-dir", type=Path, default=Path("demo_data"))
    parser.add_argument("--output-dir", type=Path, default=Path("demo_output"))
    args = parser.parse_args(argv)
    result = run_demo(args.data_dir, args.output_dir)
    print(result.summary, end="")
    report = result.report_path
    shown = (
        report.relative_to(Path.cwd())
        if report and report.is_relative_to(Path.cwd())
        else report
    )
    print(f"Report: {shown}")
    return 0 if result.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
