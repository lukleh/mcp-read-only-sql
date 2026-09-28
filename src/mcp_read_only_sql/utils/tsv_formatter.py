"""TSV formatter for database query results."""

import csv
import io
from typing import Any, TextIO


def format_as_tsv(rows: list[Any], columns: list[str]) -> str:
    """Format query results as TSV (Tab-Separated Values).

    Args:
        rows: List of rows (dicts, lists, tuples, or scalars)
        columns: Ordered list of column names

    Returns:
        TSV formatted string with headers (no trailing newline)
    """

    if not rows and not columns:
        return ""

    buffer = io.StringIO()
    writer = csv.writer(
        buffer, delimiter="\t", lineterminator="\n", quoting=csv.QUOTE_MINIMAL
    )

    if columns:
        writer.writerow([str(col) for col in columns])

    for row in rows:
        if isinstance(row, dict):
            if columns:
                values = [row.get(col, "") for col in columns]
            else:
                values = list(row.values())
        elif isinstance(row, (list, tuple)):
            values = list(row)
        else:
            values = [row]

        normalized = ["" if value is None else str(value) for value in values]
        writer.writerow(normalized)

    return buffer.getvalue().rstrip("\n")


# Characters that make psql's CSV mode quote a field when tab is the separator.
_QUOTE_TRIGGERS = ("\t", '"', "\r", "\n")


def format_tsv_field(value: Any) -> str:
    """Render one field the way ``psql --csv`` with a tab separator does.

    NULL and the empty string are both empty. A value containing the
    separator, a double quote, CR or LF is enclosed in double quotes with
    inner quotes doubled. Both connector implementations therefore produce
    the same bytes for the same row.
    """
    if value is None:
        return ""
    text = str(value)
    if any(trigger in text for trigger in _QUOTE_TRIGGERS):
        return '"' + text.replace('"', '""') + '"'
    return text


def format_tsv_line(values: list[Any]) -> str:
    """Render a single TSV line (without trailing newline)."""

    return "\t".join(format_tsv_field(value) for value in values)


def write_tsv_text_line(handle: TextIO, line: str, wrote_content: bool) -> bool:
    """Write a preformatted TSV line without adding a trailing newline."""
    if wrote_content:
        handle.write("\n")
    handle.write(line)
    return True
