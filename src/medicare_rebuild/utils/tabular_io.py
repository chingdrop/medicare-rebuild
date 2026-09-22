# Copied from py-shared-tools v1.3.1 (https://github.com/chingdrop/py-shared-tools,
# commit d54dcd6), copyright Craig Hurley. Only the symbols this repository uses were
# copied, and docstrings were trimmed where they referred to code that was not copied.
# The source repository is licensed under GPL-3.0 (see its LICENSE file).
# TODO(craig): the rest of this repository is MIT. As the sole author you can license
# these copies under MIT; confirm the intended licence and update this notice.

"""Extension-dispatch tabular file writing: write a DataFrame as CSV, Excel, JSON,
or HTML based on the file's extension, without the caller picking the right pandas
writer method itself.

Raises ``TabularIOError`` for an unsupported extension or a write failure.
Text-based formats (csv/txt/json/html) are written through
``medicare_rebuild.utils.atomic_io.atomic_write`` rather than pandas writing
directly to the target path, so a crash never leaves a partial file. Excel has no
equivalent in-memory round trip as cheap as the text formats', so it's written
directly and is not atomic.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pandas as pd

from medicare_rebuild.utils.atomic_io import atomic_write


class TabularIOError(Exception):
    """Reading or writing a structured data file failed, or its extension
    isn't one of the supported types."""


TextWriter = Callable[..., str]
TEXT_WRITERS: dict[str, TextWriter] = {
    "csv": lambda df, **kw: df.to_csv(**kw),
    "txt": lambda df, **kw: df.to_csv(**kw),
    "json": lambda df, **kw: df.to_json(**kw),
    "html": lambda df, **kw: df.to_html(**kw),
    "htm": lambda df, **kw: df.to_html(**kw),
}
_EXCEL_EXTENSIONS = {"xls", "xlsx"}
SUPPORTED_WRITE_EXTENSIONS = set(TEXT_WRITERS) | _EXCEL_EXTENSIONS


def write_structured_file(
    df: pd.DataFrame, file_path: str | Path, file_type: str | None = None, **kwargs: Any
) -> None:
    """Write a DataFrame to a CSV/Excel/JSON/HTML file.

    ``file_type`` overrides extension-based dispatch; ``**kwargs`` passes
    through to the underlying pandas writer (e.g. ``index=False``,
    ``sheet_name="Data"``, ``orient="records"``). Text-based formats are
    written atomically (see module docstring); Excel is not. Raises
    :class:`TabularIOError` for an unsupported extension or a write failure.
    """
    path = Path(file_path)
    ext = (file_type or path.suffix.lstrip(".")).lower()

    if ext not in SUPPORTED_WRITE_EXTENSIONS:
        raise TabularIOError(f"Unsupported file type for writing: .{ext} ({path})")

    try:
        if ext in _EXCEL_EXTENSIONS:
            kwargs.setdefault("engine", "openpyxl")
            df.to_excel(path, **kwargs)
        else:
            atomic_write(path, TEXT_WRITERS[ext](df, **kwargs))
    except Exception as exc:
        raise TabularIOError(f"Failed to write .{ext} file to {path}: {exc}") from exc
