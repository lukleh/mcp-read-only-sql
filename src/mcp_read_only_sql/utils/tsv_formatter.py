"""TSV formatter for database query results."""

import csv
import io
from typing import Any, TextIO


def format_tsv_line(values: list[Any]) -> str:
    """Render a single TSV line (without trailing newline)."""

    buffer = io.StringIO()
    writer = csv.writer(
        buffer, delimiter="\t", lineterminator="", quoting=csv.QUOTE_MINIMAL
    )
    normalized = ["" if value is None else str(value) for value in values]
    writer.writerow(normalized)
    return buffer.getvalue()


def write_tsv_text_line(handle: TextIO, line: str, wrote_content: bool) -> bool:
    """Write a preformatted TSV line without adding a trailing newline."""
    if wrote_content:
        handle.write("\n")
    handle.write(line)
    return True
