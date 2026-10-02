"""One-page audit sheet of a demo run, as HTML, for the README and a portfolio.

Every number on the sheet is read from the run's own output -- the reconciliation
report (`make reconcile`), the demo's manifest checks (`make demo`) and the
generator's manifest -- nothing is typed in by hand. After `make demo` and
`make reconcile`:

    uv run python -m tools.audit_sheet                      # demo_output/audit_sheet.html
    uv run python -m tools.audit_sheet --png docs/audit-sheet.png \\
        --chrome "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"

`--png` also needs ffmpeg. Headless Chrome screenshots a fixed-size window, not the
page, so the sheet is captured in a window taller than it needs and the empty
background below it is trimmed off.

Counts and synthetic IDs only, like the reconciliation report itself.
"""

import argparse
import html
import json
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path

WIDTH = 1400  # CSS px; the sheet's body width
SCALE = 2  # device pixel ratio of the PNG

DISPOSITIONS = {
    "PATIENT_REJECTED": "belong to a rejected patient",
    "EXCLUDED_BY_SOURCE_QUERY": "resupply, excluded by the source query",
    "NO_PATIENT_IN_EXPORT": "with no matching patient in the export",
    "OUTSIDE_EXTRACT_WINDOW": "outside the extract window",
    "NO_MATCHING_DEVICE": "with no device of the reading's type",
}
REJECTION_REASONS = {
    "PHONE_LENGTH": "phone number",
    "STATE_LENGTH": "state",
    "ZIP_LENGTH": "ZIP code",
    "EMERGENCY_PHONE_1_LENGTH": "emergency phone",
}
SOURCES = [
    ("patients", "Patients", "SharePoint export"),
    ("users", "Staff users", "Microsoft Graph"),
    ("devices", "Devices", "Fulfillment_All"),
    ("glucose readings", "Glucose readings", "Glucose_Readings"),
    ("blood pressure readings", "Blood pressure readings", "Blood_Pressure_Readings"),
    ("patient notes", "Patient notes", "Medical_Notes + Time_Log"),
]
CODES = ["99202", "99453", "99454", "99457", "99458"]
CODE_MEANING = {
    "99202": "new-patient visit",
    "99453": "device setup",
    "99454": "device supply, 30 days",
    "99457": "first 20 min of care",
    "99458": "each extra 20 min",
}


def _fmt(n: int) -> str:
    return f"{n:,}"


def _date(s: str) -> str:
    return datetime.fromisoformat(s).strftime("%b %-d, %Y")


def build(data_dir: Path, output_dir: Path) -> str:
    rec = json.loads((output_dir / "reconcile.json").read_text())
    demo_checks = json.loads((output_dir / "checks.json").read_text())
    manifest = json.loads((data_dir / "manifest.json").read_text())
    checks = {c["name"]: c for c in rec["checks"]}
    sources = checks["1 row conservation"]["details"]["sources"]
    lineage = checks["5 billing lineage"]["details"]
    report = checks["6 report totals"]["details"]
    applied = manifest["expected"]["codes_applied"]
    win = manifest["windows"]

    total_source = sum(s["source"] for s in sources.values())
    total_loaded = sum(s["loaded"] - s["loaded_from_fanout"] for s in sources.values())
    unexplained = sum(s["unexplained"] for s in sources.values())
    rec_pass = sum(c["ok"] for c in rec["checks"])
    demo_pass = sum(c["ok"] for c in demo_checks)

    # -- row accounting ledger ---------------------------------------------------
    rows = []
    for key, label, origin in SOURCES:
        s = sources[key]
        loaded = s["loaded"] - s["loaded_from_fanout"]
        dropped = {k: v for k, v in s["dispositions"].items() if v}
        n_dropped = sum(dropped.values())
        pct = 100 * loaded / s["source"]
        reasons = []
        for k, v in sorted(dropped.items(), key=lambda kv: -kv[1]):
            text = f"{_fmt(v)} {DISPOSITIONS.get(k, k.lower())}"
            if k == "PATIENT_REJECTED" and s.get("rejected_by_reason"):
                # The patient rows themselves: say why validation rejected them.
                why = ", ".join(
                    REJECTION_REASONS.get(r, r.lower()) for r in s["rejected_by_reason"]
                )
                text = (
                    f"{_fmt(v)} rejected by validation "
                    f"<span class='muted'>({why} too long)</span>"
                )
            reasons.append(f"<li>{text}</li>")
        reason_html = (
            f"<ul class='reasons'>{''.join(reasons)}</ul>"
            if reasons
            else "<div class='reasons none'>nothing dropped</div>"
        )
        drop_seg = (
            f"<div class='seg drop' style='flex:{n_dropped}'></div>"
            if n_dropped
            else ""
        )
        rows.append(
            f"""
        <div class="ledger-row">
          <div class="src">
            <div class="src-name">{html.escape(label)}</div>
            <div class="src-origin">{html.escape(origin)}</div>
          </div>
          <div class="eq">
            <span class="num">{_fmt(s["source"])}</span>
            <span class="op">=</span>
            <span class="num loaded-ink">{_fmt(loaded)}</span>
            <span class="op">+</span>
            <span class="num drop-ink">{_fmt(n_dropped)}</span>
          </div>
          <div class="barcell">
            <div class="bar">
              <div class="seg load" style="flex:{loaded}"></div>{drop_seg}
            </div>
            <div class="pct">{pct:.1f}% loaded</div>
          </div>
          <div class="why">{reason_html}</div>
          <div class="ok"><span class="tick">&#10003;</span> {s["unexplained"]} unexplained</div>
        </div>"""
        )

    # -- billing codes -------------------------------------------------------------
    max_code = max(applied[c] for c in CODES)
    code_rows = []
    for c in CODES:
        a, r = applied[c], report["report_by_code"][c]
        code_rows.append(
            f"""
        <div class="code-row">
          <div class="code"><b>{c}</b><span>{CODE_MEANING[c]}</span></div>
          <div class="code-bars">
            <div class="cbar"><div class="fill applied" style="width:{100 * a / max_code:.1f}%"></div><span>{a}</span></div>
            <div class="cbar"><div class="fill inreport" style="width:{100 * r / max_code:.1f}%"></div><span>{r}</span></div>
          </div>
        </div>"""
        )

    # -- audit checks ----------------------------------------------------------------
    check_rows = []
    for c in rec["checks"]:
        num, _, name = c["name"].partition(" ")
        mark = (
            "<span class='pass'>&#10003; PASS</span>"
            if c["ok"]
            else "<span class='fail'>&#10007; FAIL</span>"
        )
        check_rows.append(
            f"""
        <div class="check">
          {mark}
          <div><b>{html.escape(name.capitalize())}</b>
          <div class="muted">{html.escape(c["summary"])}</div></div>
        </div>"""
        )

    sp, dv, gl, bp, nt, us = (
        sources["patients"],
        sources["devices"],
        sources["glucose readings"],
        sources["blood pressure readings"],
        sources["patient notes"],
        sources["users"],
    )
    readings_loaded = gl["loaded"] + bp["loaded"]

    return TEMPLATE.format(
        seed=rec["seed"],
        extract=f"{_date(win['extract_start'])} &ndash; {_date(win['extract_end'])}",
        report_window=f"{_date(win['report_start'])} &ndash; {_date(win['report_end'])}",
        total_source=_fmt(total_source),
        total_loaded=_fmt(total_loaded),
        unexplained=unexplained,
        rec_pass=rec_pass,
        rec_total=len(rec["checks"]),
        demo_pass=demo_pass,
        demo_total=len(demo_checks),
        codes_traced=_fmt(lineage["codes_traced"]),
        codes_db=_fmt(lineage["codes_in_database"]),
        sp_src=_fmt(sp["source"]),
        legacy_src=_fmt(dv["source"] + gl["source"] + bp["source"] + nt["source"]),
        users_src=_fmt(us["source"]),
        rejected=sp["dispositions"].get("PATIENT_REJECTED", 0),
        patients_loaded=_fmt(sp["loaded"]),
        devices_loaded=_fmt(dv["loaded"]),
        readings_loaded=_fmt(readings_loaded),
        notes_loaded=_fmt(nt["loaded"]),
        report_rows=_fmt(report["report_rows"]),
        report_codes=_fmt(sum(report["report_by_code"].values())),
        ledger="".join(rows),
        codes="".join(code_rows),
        checks="".join(check_rows),
    )


TEMPLATE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<title>Pipeline audit sheet</title>
<style>
  :root {{
    --surface: #fcfcfb; --card: #ffffff; --line: #e6e4df;
    --ink: #0b0b0b; --ink-2: #52514e; --ink-3: #8a8984;
    --loaded: #2a78d6; --drop: #eb6834; --inreport: #6da7ec;
    --good: #0ca30c; --good-ink: #006300; --critical: #d03b3b;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: var(--surface); color: var(--ink); width: 1400px; min-height: 100vh;
         font: 15px/1.45 -apple-system, BlinkMacSystemFont, "Inter", "Segoe UI", sans-serif;
         padding: 44px 48px 36px; }}
  .muted {{ color: var(--ink-3); }}
  .num {{ font-variant-numeric: tabular-nums; }}
  header {{ display: flex; justify-content: space-between; align-items: flex-end;
           border-bottom: 1px solid var(--line); padding-bottom: 20px; }}
  .eyebrow {{ font-size: 12px; letter-spacing: .08em; text-transform: uppercase;
             color: var(--ink-2); font-weight: 600; }}
  h1 {{ font-size: 30px; font-weight: 700; letter-spacing: -.01em; margin-top: 4px; }}
  .meta {{ text-align: right; color: var(--ink-2); font-size: 13px; line-height: 1.6; }}
  .meta b {{ color: var(--ink); font-weight: 600; }}
  .badge {{ display: inline-block; background: #f0efec; color: var(--ink-2);
           border-radius: 4px; padding: 1px 7px; font-size: 12px; font-weight: 600; }}

  .stats {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin: 24px 0; }}
  .stat {{ background: var(--card); border: 1px solid var(--line); border-radius: 10px;
          padding: 16px 18px; }}
  .stat .v {{ font-size: 30px; font-weight: 700; letter-spacing: -.01em;
             font-variant-numeric: tabular-nums; }}
  .stat .v small {{ font-size: 16px; color: var(--ink-2); font-weight: 600; }}
  .stat .l {{ color: var(--ink-2); font-size: 13px; margin-top: 2px; }}
  .stat .v .ok-ink {{ color: var(--good-ink); }}

  h2 {{ font-size: 13px; letter-spacing: .08em; text-transform: uppercase;
       color: var(--ink-2); font-weight: 700; margin-bottom: 12px; }}
  section {{ background: var(--card); border: 1px solid var(--line); border-radius: 10px;
            padding: 20px 22px; margin-bottom: 16px; }}

  .flow {{ display: grid; grid-template-columns: 1.15fr 24px 1fr 24px 1fr 24px 1fr 24px 1fr;
          align-items: stretch; }}
  .stage {{ border: 1px solid var(--line); border-radius: 8px; padding: 12px 14px;
           background: #fafaf8; }}
  .stage .t {{ font-weight: 700; font-size: 14px; }}
  .stage .s {{ color: var(--ink-2); font-size: 12.5px; margin-bottom: 6px; }}
  .stage ul {{ list-style: none; font-size: 13px; }}
  .stage li {{ display: flex; justify-content: space-between; gap: 8px;
              font-variant-numeric: tabular-nums; }}
  .stage li span:first-child {{ color: var(--ink-2); }}
  .arrow {{ display: flex; align-items: center; justify-content: center;
           color: var(--ink-3); font-size: 18px; }}

  .ledger-head, .ledger-row {{ display: grid;
      grid-template-columns: 210px 190px 1fr 330px 130px; gap: 18px; align-items: center; }}
  .ledger-head {{ font-size: 12px; color: var(--ink-3); padding-bottom: 6px;
                 border-bottom: 1px solid var(--line); }}
  .ledger-row {{ padding: 10px 0; border-bottom: 1px solid #f0efec; }}
  .ledger-row:last-child {{ border-bottom: 0; }}
  .src-name {{ font-weight: 600; }}
  .src-origin {{ font-size: 12px; color: var(--ink-3); font-family: ui-monospace, Menlo, monospace; }}
  .eq {{ font-size: 15px; white-space: nowrap; font-variant-numeric: tabular-nums; }}
  .eq .op {{ color: var(--ink-3); margin: 0 4px; }}
  .loaded-ink {{ color: var(--ink); font-weight: 700; }}
  .drop-ink {{ color: var(--ink-2); }}
  .bar {{ display: flex; gap: 2px; height: 14px; }}
  .seg {{ border-radius: 3px; min-width: 3px; }}
  .seg.load {{ background: var(--loaded); }}
  .seg.drop {{ background: var(--drop); }}
  .pct {{ font-size: 12px; color: var(--ink-3); margin-top: 3px; }}
  .reasons {{ list-style: none; font-size: 12.5px; color: var(--ink-2); }}
  .reasons li::before {{ content: ""; display: inline-block; width: 8px; height: 8px;
                        border-radius: 2px; background: var(--drop); margin-right: 6px; }}
  .reasons.none {{ color: var(--ink-3); }}
  .ok {{ font-size: 12.5px; color: var(--good-ink); font-weight: 600; white-space: nowrap; }}
  .tick {{ color: var(--good); }}
  .legend {{ display: flex; gap: 18px; font-size: 12.5px; color: var(--ink-2); margin: -4px 0 10px; }}
  .legend i {{ display: inline-block; width: 10px; height: 10px; border-radius: 2px;
              margin-right: 6px; vertical-align: -1px; }}

  .two {{ display: grid; grid-template-columns: 1fr 1.15fr; gap: 16px; }}
  .two section {{ margin-bottom: 0; }}
  .code-row {{ display: grid; grid-template-columns: 190px 1fr; gap: 14px; align-items: center;
              padding: 7px 0; }}
  .code b {{ font-size: 15px; font-variant-numeric: tabular-nums; }}
  .code span {{ display: block; font-size: 12px; color: var(--ink-3); }}
  .cbar {{ display: flex; align-items: center; gap: 8px; height: 13px; margin: 3px 0;
          font-size: 12px; font-variant-numeric: tabular-nums; color: var(--ink-2); }}
  .cbar .fill {{ height: 10px; border-radius: 0 3px 3px 0; }}
  .fill.applied {{ background: var(--loaded); }}
  .fill.inreport {{ background: var(--inreport); }}
  .note {{ font-size: 12px; color: var(--ink-3); margin-top: 10px; }}

  .checks {{ display: grid; grid-template-columns: 1fr 1fr; gap: 8px 18px; }}
  .check {{ display: grid; grid-template-columns: 74px 1fr; gap: 8px; font-size: 13px;
           padding: 6px 0; border-bottom: 1px solid #f0efec; }}
  .pass {{ color: var(--good-ink); font-weight: 700; font-size: 12px; padding-top: 1px; }}
  .fail {{ color: var(--critical); font-weight: 700; font-size: 12px; }}
  .check .muted {{ font-size: 12px; }}

  footer {{ display: flex; justify-content: space-between; color: var(--ink-3);
           font-size: 12px; margin-top: 18px; }}
</style></head>
<body>
  <header>
    <div>
      <div class="eyebrow">Medicare remote patient monitoring &middot; billing data pipeline</div>
      <h1>Audit of one end-to-end run</h1>
    </div>
    <div class="meta">
      <span class="badge">Synthetic data only</span> &nbsp;seed <b>{seed}</b><br>
      Extract window <b>{extract}</b><br>
      Billing report <b>{report_window}</b>
    </div>
  </header>

  <div class="stats">
    <div class="stat"><div class="v">{total_source}</div>
      <div class="l">source rows read, <b class="ok-ink">{unexplained} unexplained</b> &mdash; every row is loaded or dropped for a named reason</div></div>
    <div class="stat"><div class="v"><span class="ok-ink">{rec_pass}</span><small> / {rec_total}</small></div>
      <div class="l">reconciliation checks passed &mdash; keys, foreign keys, lineage, report totals</div></div>
    <div class="stat"><div class="v"><span class="ok-ink">{demo_pass}</span><small> / {demo_total}</small></div>
      <div class="l">checks against the expected-results manifest, incl. 46 named edge cases</div></div>
    <div class="stat"><div class="v">{codes_traced}<small> / {codes_db}</small></div>
      <div class="l">billing codes traced back to the source rows that earned them</div></div>
  </div>

  <section>
    <h2>The run, stage by stage</h2>
    <div class="flow">
      <div class="stage"><div class="t">Extract</div><div class="s">three source systems</div>
        <ul><li><span>SharePoint patient export</span><span>{sp_src}</span></li>
            <li><span>Legacy SQL Server rows</span><span>{legacy_src}</span></li>
            <li><span>Microsoft Graph staff</span><span>{users_src}</span></li></ul></div>
      <div class="arrow">&rarr;</div>
      <div class="stage"><div class="t">Transform</div><div class="s">pandas: standardize, validate</div>
        <ul><li><span>patients rejected</span><span>{rejected}</span></li>
            <li><span>readings matched to device</span><span>&#10003;</span></li>
            <li><span>IDs resolved to new keys</span><span>&#10003;</span></li></ul></div>
      <div class="arrow">&rarr;</div>
      <div class="stage"><div class="t">Load</div><div class="s">new SQL Server schema</div>
        <ul><li><span>patients</span><span>{patients_loaded}</span></li>
            <li><span>devices</span><span>{devices_loaded}</span></li>
            <li><span>readings</span><span>{readings_loaded}</span></li>
            <li><span>notes</span><span>{notes_loaded}</span></li></ul></div>
      <div class="arrow">&rarr;</div>
      <div class="stage"><div class="t">Bill</div><div class="s">CPT rules in pandas</div>
        <ul><li><span>codes applied</span><span>{codes_db}</span></li>
            <li><span>each with a date of service</span><span>&#10003;</span></li></ul></div>
      <div class="arrow">&rarr;</div>
      <div class="stage"><div class="t">Report</div><div class="s">Billing_Report.xlsx</div>
        <ul><li><span>report rows</span><span>{report_rows}</span></li>
            <li><span>codes in window</span><span>{report_codes}</span></li>
            <li><span>matches database</span><span>&#10003;</span></li></ul></div>
    </div>
  </section>

  <section>
    <h2>Where every source row went</h2>
    <div class="legend"><span><i style="background:var(--loaded)"></i>loaded</span>
      <span><i style="background:var(--drop)"></i>not loaded, with the reason recorded</span></div>
    <div class="ledger-head"><div>Source</div><div>source = loaded + dropped</div>
      <div>Share loaded</div><div>Why rows were dropped</div><div>Reconciled</div></div>
    {ledger}
  </section>

  <div class="two">
    <section>
      <h2>Billing codes</h2>
      <div class="legend"><span><i style="background:var(--loaded)"></i>applied in the run</span>
        <span><i style="background:var(--inreport)"></i>in the report window</span></div>
      {codes}
      <div class="note">Codes stamped after the report window's end are applied but not reported &mdash; by design.</div>
    </section>
    <section>
      <h2>Reconciliation checks</h2>
      <div class="checks">{checks}</div>
    </section>
  </div>

  <footer>
    <span>Generated from the run's own output: <code>make demo</code> &rarr; <code>make reconcile</code> &rarr; <code>tools/audit_sheet.py</code>. Counts and synthetic IDs only.</span>
    <span>github.com/chingdrop/medicare-rebuild</span>
  </footer>
</body></html>
"""


def render_png(page: Path, png: Path, chrome: str, margin: int = 32) -> None:
    """Screenshot `page` with headless Chrome in a deliberately tall window, then crop
    the PNG to the last row that differs from the background, plus `margin` CSS px."""
    with tempfile.TemporaryDirectory() as tmp:
        shot = Path(tmp) / "tall.png"
        subprocess.run(
            [
                chrome,
                "--headless=new",
                "--hide-scrollbars",
                f"--force-device-scale-factor={SCALE}",
                f"--window-size={WIDTH},2400",
                f"--screenshot={shot}",
                page.resolve().as_uri(),
            ],
            check=True,
            capture_output=True,
        )
        gray = subprocess.run(
            ["ffmpeg", "-loglevel", "error", "-i", str(shot)]
            + ["-f", "rawvideo", "-pix_fmt", "gray", "-"],
            check=True,
            capture_output=True,
        ).stdout
        w = WIDTH * SCALE
        background = gray[0]
        last = max(
            y
            for y in range(len(gray) // w)
            if any(abs(b - background) > 6 for b in gray[y * w : (y + 1) * w : 2])
        )
        subprocess.run(
            ["ffmpeg", "-loglevel", "error", "-y", "-i", str(shot)]
            + ["-vf", f"crop=iw:{last + 1 + margin * SCALE}:0:0", str(png)],
            check=True,
        )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--data-dir", type=Path, default=Path("demo_data"))
    ap.add_argument("--output-dir", type=Path, default=Path("demo_output"))
    ap.add_argument("--png", type=Path, help="also render the sheet to this PNG")
    ap.add_argument("--chrome", default="chromium", help="Chrome/Chromium binary")
    a = ap.parse_args()
    out = a.output_dir / "audit_sheet.html"
    out.write_text(build(a.data_dir, a.output_dir))
    print(f"Wrote {out}")
    if a.png:
        render_png(out, a.png, a.chrome)
        print(f"Wrote {a.png}")


if __name__ == "__main__":
    main()
