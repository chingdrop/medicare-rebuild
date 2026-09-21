"""Run every check and render the results."""

import json
from dataclasses import dataclass
from pathlib import Path

from tools.reconcile import checks, reasons
from tools.reconcile.collect import (
    load_expected_codes,
    load_loaded,
    load_source,
    settings,
)
from tools.reconcile.model import CheckResult, Loaded, Settings, Source
from tools.synthetic_data.generator import FILES


@dataclass
class Reconciliation:
    results: list[CheckResult]
    seed: int | None = None
    faults: list[str] | None = None

    @property
    def passed(self) -> bool:
        return all(r.ok for r in self.results)

    def failing(self) -> set[str]:
        return {r.name for r in self.results if not r.ok}


def reconcile(
    src: Source,
    loaded: Loaded,
    cfg: Settings,
    expected_codes: list[tuple[int, str, str]] | None,
    sample: int | None = None,
) -> Reconciliation:
    normalized = reasons.normalize(src.patients)
    by_index = reasons.rejection_reasons(normalized)
    by_id = reasons.reasons_by_patient_id(normalized, by_index)
    agrees = reasons.agrees_with_pipeline(normalized, by_index)
    derived = reasons.expected_derived_rows(reasons.kept_rows(normalized, by_index))
    results = [
        checks.check_row_conservation(src, loaded, cfg, by_id, derived),
        checks.check_key_uniqueness(loaded),
        checks.check_foreign_keys(loaded),
        checks.check_required_fields(loaded),
        checks.check_cross_source(src, loaded, set(by_id)),
        checks.check_rejection_accounting(src, loaded, by_id, agrees),
        checks.check_billing_lineage(src, loaded, cfg, expected_codes, sample),
        checks.check_report_totals(loaded, cfg),
    ]
    return Reconciliation(results)


def run(data_dir: Path, output_dir: Path, sample: int | None = None) -> Reconciliation:
    data_dir, output_dir = data_dir.resolve(), output_dir.resolve()
    manifest = json.loads((data_dir / FILES["manifest"]).read_text())
    rec = reconcile(
        load_source(data_dir),
        load_loaded(output_dir),
        settings(),
        load_expected_codes(data_dir),
        sample,
    )
    rec.seed = manifest["generator"]["seed"]
    faults_file = data_dir / "faults.json"
    rec.faults = (
        json.loads(faults_file.read_text())["faults"] if faults_file.exists() else []
    )
    (output_dir / "reconcile.txt").write_text(render_text(rec))
    (output_dir / "reconcile.json").write_text(render_json(rec))
    return rec


# -- output -----------------------------------------------------------------------


def _equation(name: str, s: dict) -> str:
    parts = [f"loaded {s['loaded']}"]
    if s["loaded_from_fanout"]:
        parts = [
            f"loaded {s['loaded']} (of which {s['loaded_from_fanout']} extra from device fan-out)"
        ]
    parts.append(f"duplicates merged {s['duplicates_merged']}")
    for code, n in s["dispositions"].items():
        parts.append(f"{code} {n}")
    tail = f"unexplained {s['unexplained']}"
    return (
        f"  {name:<24} source {s['source']:>5} = " + " + ".join(parts) + f"  [{tail}]"
    )


def render_text(rec: Reconciliation) -> str:
    lines = ["Reconciliation - counts and synthetic IDs only"]
    if rec.seed is not None:
        lines.append(f"Seed {rec.seed}")
    if rec.faults:
        lines.append(
            f"Injected fault(s) applied after the run: {', '.join(rec.faults)}"
        )
    lines.append("")
    conservation = next(r for r in rec.results if r.name.startswith("1 "))
    lines.append("Row conservation (source = loaded + merged + dispositions):")
    for name, s in conservation.details["sources"].items():
        lines.append(_equation(name, s))
        if s.get("rejected_by_reason"):
            reasons_txt = ", ".join(
                f"{k} {v}" for k, v in s["rejected_by_reason"].items()
            )
            lines.append(f"  {'':<24} rejected by reason: {reasons_txt}")
    for table, v in conservation.details["derived_tables"].items():
        lines.append(
            f"  {table:<24} expected {v['expected']:>5} = loaded {v['loaded']}  [unexplained {v['unexplained']}]"
        )
    lines += ["", "Checks:"]
    for r in rec.results:
        lines.append(f"  [{'PASS' if r.ok else 'FAIL'}] {r.name}: {r.summary}")
        if not r.ok and r.violations:
            lines.append(f"         IDs: {', '.join(str(v) for v in r.violations)}")
    verdict = "PASS" if rec.passed else "FAIL"
    lines += [
        "",
        f"RECONCILIATION: {verdict} ({sum(r.ok for r in rec.results)}/{len(rec.results)} checks)",
    ]
    return "\n".join(lines) + "\n"


def render_json(rec: Reconciliation) -> str:
    payload = {
        "counts_and_synthetic_ids_only": True,
        "seed": rec.seed,
        "injected_faults": rec.faults or [],
        "passed": rec.passed,
        "checks": [
            {
                "name": r.name,
                "ok": r.ok,
                "summary": r.summary,
                "details": r.details,
                "violating_ids": r.violations,
            }
            for r in rec.results
        ],
    }
    return json.dumps(payload, indent=2, default=int) + "\n"
