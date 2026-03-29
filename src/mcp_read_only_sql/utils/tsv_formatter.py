"""TSV formatter for database query results."""

import csv
import io
from typing import Any, List


def format_as_tsv(rows: List[Any], columns: List[str]) -> str:
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
    writer = csv.writer(buffer, delimiter="\t", lineterminator="\n", quoting=csv.QUOTE_MINIMAL)

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


def format_tsv_line(values: List[Any]) -> str:
    """Render a single TSV line (without trailing newline)."""

    buffer = io.StringIO()
    writer = csv.writer(buffer, delimiter="\t", lineterminator="", quoting=csv.QUOTE_MINIMAL)
    normalized = ["" if value is None else str(value) for value in values]
    writer.writerow(normalized)
    return buffer.getvalue()

