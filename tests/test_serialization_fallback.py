#!/usr/bin/env python3
"""
Test simple JSON serialization with string fallback.
Ensures the serializer never fails by converting unknown types to strings.
"""

import json
from src.utils.json_serializer import serialize_result, DatabaseJSONEncoder


class CustomClass:
    """A custom class for testing"""
    def __init__(self):
        self.data = "some data"

    def __str__(self):
        return f"CustomClass(data={self.data})"


def test_unknown_type_fallback():
    """Test that unknown types fall back to string representation"""
    encoder = DatabaseJSONEncoder()
    obj = CustomClass()
    serialized = encoder.default(obj)
    assert isinstance(serialized, str)
    assert "CustomClass" in serialized


def test_basic_types_passthrough():
    """Test that basic JSON types are handled correctly"""
    # Basic types should serialize correctly in JSON
    data = {
        "null": None,
        "int": 42,
        "float": 3.14,
        "str": "hello",
        "bool": True,
        "list": [1, 2, 3],
        "dict": {"key": "value"}
    }
    json_str = serialize_result(data)
    parsed = json.loads(json_str)
    assert parsed["null"] is None
    assert parsed["int"] == 42
    assert parsed["float"] == 3.14
    assert parsed["str"] == "hello"
    assert parsed["bool"] is True
    assert parsed["list"] == [1, 2, 3]
    assert parsed["dict"] == {"key": "value"}


def test_mixed_serializable_result():
    """Test a result with mixed serializable and non-serializable types"""
    import datetime
    import decimal

    result = {
        "success": True,
        "rows": [
            {
                "id": 1,
                "name": "test",
                "created": datetime.datetime.now(),
                "price": decimal.Decimal("123.45"),
                "custom_obj": CustomClass(),
            }
        ],
        "columns": ["id", "name", "created", "price", "custom_obj"],
        "row_count": 1
    }

    # This should not raise any exception
    json_str = serialize_result(result)
    assert json_str

    # Should be valid JSON
    parsed = json.loads(json_str)
    assert parsed["success"] is True
    assert parsed["row_count"] == 1

    # Check that all values were serialized
    row = parsed["rows"][0]
    assert row["id"] == 1
    assert row["name"] == "test"
    assert isinstance(row["created"], str)  # datetime -> string
    assert isinstance(row["price"], (float, int))  # decimal -> numeric
    assert isinstance(row["custom_obj"], str)  # custom -> string


def test_encoder_with_special_types():
    """Test the encoder handles various types by converting to string"""
    import datetime
    import decimal
    from uuid import UUID

    encoder = DatabaseJSONEncoder()

    # Test various non-JSON types
    test_values = [
        datetime.datetime.now(),
        datetime.date.today(),
        decimal.Decimal("999.99"),
        UUID("12345678-1234-5678-1234-567812345678"),
        CustomClass(),  # custom class
    ]

    for value in test_values:
        # Should not raise exception
        serialized = encoder.default(value)
        # Decimal should become float, others become strings
        if str(type(value).__name__) == 'Decimal':
            assert isinstance(serialized, float)
        else:
            assert isinstance(serialized, str)
        # Should be JSON serializable
        json.dumps(serialized)


def test_result_never_fails():
    """Test that serialize_result never fails regardless of input"""
    import datetime

    # Various edge cases
    test_cases = [
        {"simple": "value"},
        {"number": 42},
        {"nested": {"list": [1, 2, 3]}},
        {"datetime": datetime.datetime.now()},
        {"custom": CustomClass()},
        {"mixed": [1, "two", CustomClass(), None]},
    ]

    for data in test_cases:
        # Should never raise an exception
        json_str = serialize_result(data)
        # Should produce valid JSON
        parsed = json.loads(json_str)
        assert parsed is not None


