"""Checks that generated files contain only obviously synthetic values.

Reports the file, line number and *kind* of problem, never the matching text.
"""

import re
from pathlib import Path

# Hyphenated SSN shape; area numbers 900-999 are never issued, so they are allowed.
_SSN = re.compile(r"(?<![\w-])(\d{3})-(\d{2})-(\d{4})(?![\w-])")
_SSN_BARE = re.compile(r"(?<![\w.-])(\d{9})(?![\w.-])")
# Medicare Beneficiary Identifier shape (CMS format), with or without dashes.
_MBI = re.compile(
    r"(?<![A-Za-z0-9])[1-9][AC-HJKMNP-RT-Yac-hjkmnp-rt-y][AC-HJKMNP-RT-Yac-hjkmnp-rt-y0-9][0-9]"
    r"-?[AC-HJKMNP-RT-Yac-hjkmnp-rt-y][AC-HJKMNP-RT-Yac-hjkmnp-rt-y0-9][0-9]"
    r"-?[AC-HJKMNP-RT-Yac-hjkmnp-rt-y]{2}[0-9]{2}(?![A-Za-z0-9])"
)
_PHONE = re.compile(
    r"(?<![\w.-])(?:\+?1[ .-]?)?\(?([2-9]\d{2})\)?[ .-]?(\d{3})[ .-]?(\d{4})(?![\w-])"
)
_EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+")


def scan_text(text: str) -> list[tuple[int, str]]:
    """Return (line number, problem kind) for every forbidden pattern found."""
    problems: list[tuple[int, str]] = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        for m in _SSN.finditer(line):
            if not m.group(1).startswith("9"):
                problems.append((lineno, "ssn_outside_9xx"))
        for m in _SSN_BARE.finditer(line):
            if not m.group(1).startswith("9"):
                problems.append((lineno, "nine_digit_run_outside_9xx"))
        if _MBI.search(line):
            problems.append((lineno, "mbi_shape"))
        for m in _PHONE.finditer(line):
            if not (m.group(2) == "555" and m.group(3).startswith("01")):
                problems.append((lineno, "phone_not_555_01xx"))
        for m in _EMAIL.finditer(line):
            if not m.group(0).lower().endswith("@example.com"):
                problems.append((lineno, "email_not_example_com"))
    return problems


def scan_dir(path: Path | str) -> list[tuple[str, int, str]]:
    """Scan every generated text file under `path`."""
    root = Path(path)
    found = []
    for file in sorted(root.rglob("*")):
        if file.is_file() and file.suffix in {".csv", ".json", ".txt"}:
            for lineno, kind in scan_text(file.read_text()):
                found.append((str(file.relative_to(root)), lineno, kind))
    return found
