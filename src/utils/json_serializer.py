"""
Simple JSON serializer for database query results.
Converts non-JSON types to strings.
"""

import json
from typing import Any


class DatabaseJSONEncoder(json.JSONEncoder):
    """Simple JSON encoder that converts unknown types to strings."""

    def default(self, obj: Any) -> Any:
        """Convert non-JSON-serializable objects to strings."""
        # Try to convert numeric types to float
        try:
            return float(obj)
        except (TypeError, ValueError):
            pass

        # For any other non-serializable type, convert to string
        return str(obj)


def serialize_result(result: dict) -> str:
    """
    Serialize a database query result to JSON.

    Args:
        result: Query result dictionary

    Returns:
        JSON string representation
    """
    return json.dumps(result, cls=DatabaseJSONEncoder, ensure_ascii=False)


