"""python -m tools.reconcile: check a finished demo run. Exits non-zero on failure."""

import argparse
from pathlib import Path

from tools.reconcile.run import render_text, run


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m tools.reconcile",
        description="Reconcile a finished demo run against its source files.",
    )
    parser.add_argument("--data-dir", type=Path, default=Path("demo_data"))
    parser.add_argument("--output-dir", type=Path, default=Path("demo_output"))
    parser.add_argument(
        "--sample",
        type=int,
        default=None,
        help="trace only N billed codes (default: all)",
    )
    args = parser.parse_args(argv)
    rec = run(args.data_dir, args.output_dir, args.sample)
    print(render_text(rec), end="")
    return 0 if rec.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
