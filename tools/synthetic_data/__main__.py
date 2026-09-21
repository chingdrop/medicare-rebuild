"""Command line entry point: python -m tools.synthetic_data --seed N --patients N"""

import argparse
import sys
from pathlib import Path

from tools.synthetic_data import config as cfg
from tools.synthetic_data.faults import FAULTS
from tools.synthetic_data.generator import generate, minimum_patients
from tools.synthetic_data.safety import scan_dir


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m tools.synthetic_data",
        description="Generate deterministic, obviously synthetic source data for the demo.",
    )
    parser.add_argument(
        "--seed", type=int, default=cfg.DEFAULT_SEED, help="same seed, same output"
    )
    parser.add_argument(
        "--patients",
        type=int,
        default=cfg.DEFAULT_PATIENTS,
        help=f"patients in the export (minimum {minimum_patients()})",
    )
    parser.add_argument(
        "--out", type=Path, default=Path("demo_data"), help="output directory"
    )
    parser.add_argument(
        "--inject-fault",
        action="append",
        choices=sorted(FAULTS),
        help="apply a named fault after the demo run, to prove reconcile catches it "
        "(default: none)",
    )
    args = parser.parse_args(argv)

    manifest = generate(args.seed, args.patients, args.out, args.inject_fault)
    problems = scan_dir(args.out)
    if problems:
        for name, line, kind in problems:
            print(f"synthetic-safety: {name}:{line}: {kind}", file=sys.stderr)
        return 2
    rows = manifest["expected"]["source_rows"]
    print(
        f"Generated {rows['patients']} synthetic patients (seed {args.seed}) in {args.out}/ "
        f"- {len(manifest['scenarios'])} named scenarios"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
