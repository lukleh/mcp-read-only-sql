#!/usr/bin/env python3
"""
Result serialization tests
Tests that query results with various data types are properly returned in TSV format
"""

import json
import datetime
import decimal
import pytest
from src.connectors.postgresql.python import PostgreSQLPythonConnector
from src.connectors.postgresql.cli import PostgreSQLCLIConnector
from src.connectors.clickhouse.python import ClickHousePythonConnector
from src.connectors.clickhouse.cli import ClickHouseCLIConnector


def parse_tsv(tsv_str):
    """Parse TSV string into columns and rows"""
    # Only strip trailing newlines, not tabs!
    lines = tsv_str.rstrip('\n').split('\n')
    if not lines:
        return [], []

    columns = lines[0].split('\t')
    rows = []
    for line in lines[1:]:
        if line:  # Skip empty lines
            rows.append(line.split('\t'))
    return columns, rows


@pytest.fixture
def postgres_python_conn():
    """PostgreSQL Python connector"""
    config = {
        "connection_name": "test_pg",
        "type": "postgresql",
        "servers": [{"host": "localhost", "port": 5432}],
        "db": "testdb",
        "username": "testuser",
        "password": "testpass"
    }
    return PostgreSQLPythonConnector(config)


@pytest.fixture
def postgres_cli_conn():
    """PostgreSQL CLI connector"""
    config = {
        "connection_name": "test_pg",
        "type": "postgresql",
        "servers": [{"host": "localhost", "port": 5432}],
        "db": "testdb",
        "username": "testuser",
        "password": "testpass"
    }
    return PostgreSQLCLIConnector(config)


@pytest.fixture
def clickhouse_python_conn():
    """ClickHouse Python connector"""
    config = {
        "connection_name": "test_ch",
        "type": "clickhouse",
        "servers": [{"host": "localhost", "port": 9000}],
        "db": "testdb",
        "username": "testuser",
        "password": "testpass"
    }
    return ClickHousePythonConnector(config)


@pytest.fixture
def clickhouse_cli_conn():
    """ClickHouse CLI connector"""
    config = {
        "connection_name": "test_ch",
        "type": "clickhouse",
        "servers": [{"host": "localhost", "port": 9000}],
        "db": "testdb",
        "username": "testuser",
        "password": "testpass"
    }
    return ClickHouseCLIConnector(config)


@pytest.mark.docker
@pytest.mark.anyio
class TestPostgreSQLSerialization:
    """Test PostgreSQL result serialization"""

    async def test_basic_types(self, postgres_python_conn):
        """Test serialization of basic data types"""
        result = await postgres_python_conn.execute_query("""
            SELECT
                1 as int_val,
                1.5 as float_val,
                'test' as string_val,
                true as bool_val,
                null as null_val
        """)

        # Result should be TSV string
        assert isinstance(result, str)
        columns, rows = parse_tsv(result)

        assert len(columns) == 5
        assert columns == ['int_val', 'float_val', 'string_val', 'bool_val', 'null_val']

        assert len(rows) == 1
        row = rows[0]

        # Check values (all as strings in TSV)
        assert row[0] == '1'
        assert row[1] == '1.5'
        assert row[2] == 'test'
        assert row[3] in ['t', 'true', 'True']  # PostgreSQL boolean representations
        assert row[4] in ['', '\\N', 'NULL']  # NULL representations

    async def test_datetime_serialization(self, postgres_python_conn):
        """Test that datetime values are returned in TSV"""
        result = await postgres_python_conn.execute_query("""
            SELECT
                CURRENT_DATE as date_val,
                CURRENT_TIMESTAMP as timestamp_val,
                CURRENT_TIME as time_val
        """)

        assert isinstance(result, str)
        columns, rows = parse_tsv(result)

        assert len(columns) == 3
        assert 'date_val' in columns
        assert 'timestamp_val' in columns
        assert 'time_val' in columns

        assert len(rows) == 1
        # Values should be date/time strings
        assert rows[0][0]  # date_val should not be empty
        assert rows[0][1]  # timestamp_val should not be empty
        assert rows[0][2]  # time_val should not be empty

    async def test_numeric_types(self, postgres_python_conn):
        """Test various numeric types in TSV"""
        result = await postgres_python_conn.execute_query("""
            SELECT
                CAST(123 as SMALLINT) as small_int,
                CAST(123456 as INTEGER) as regular_int,
                CAST(123456789 as BIGINT) as big_int,
                CAST(123.456 as DECIMAL(10,3)) as decimal_val,
                CAST(123.456 as NUMERIC(10,3)) as numeric_val,
                CAST(123.456 as REAL) as real_val,
                CAST(123.456 as DOUBLE PRECISION) as double_val
        """)

        assert isinstance(result, str)
        columns, rows = parse_tsv(result)

        assert len(columns) == 7
        assert len(rows) == 1
        row = rows[0]

        # All numeric values should be present
        assert row[0] == '123'
        assert row[1] == '123456'
        assert row[2] == '123456789'
        assert '123.456' in row[3]  # decimal_val
        assert '123.456' in row[4]  # numeric_val
        # real and double may have slight precision differences

    async def test_array_types(self, postgres_python_conn):
        """Test PostgreSQL array types in TSV"""
        result = await postgres_python_conn.execute_query("""
            SELECT
                ARRAY[1, 2, 3] as int_array,
                ARRAY['a', 'b', 'c'] as string_array,
                ARRAY[true, false, true] as bool_array
        """)

        assert isinstance(result, str)
        columns, rows = parse_tsv(result)

        assert len(columns) == 3
        assert len(rows) == 1
        row = rows[0]

        # Arrays can be represented as PostgreSQL array literals {1,2,3} or Python lists [1, 2, 3]
        assert ('{' in row[0] and '}' in row[0]) or ('[' in row[0] and ']' in row[0])  # int_array
        assert ('{' in row[1] and '}' in row[1]) or ('[' in row[1] and ']' in row[1])  # string_array
        assert ('{' in row[2] and '}' in row[2]) or ('[' in row[2] and ']' in row[2])  # bool_array

    async def test_json_types(self, postgres_python_conn):
        """Test JSON/JSONB types in TSV"""
        result = await postgres_python_conn.execute_query("""
            SELECT
                '{"key": "value"}'::json as json_val,
                '{"nested": {"key": "value"}}'::jsonb as jsonb_val,
                '[1, 2, 3]'::json as json_array
        """)

        assert isinstance(result, str)
        columns, rows = parse_tsv(result)

        assert len(columns) == 3
        assert len(rows) == 1
        row = rows[0]

        # JSON values should be present as strings
        assert 'key' in row[0] and 'value' in row[0]  # json_val
        assert 'nested' in row[1]  # jsonb_val
        assert '[1' in row[2] or '1' in row[2]  # json_array

    async def test_special_values(self, postgres_python_conn):
        """Test special PostgreSQL values in TSV"""
        result = await postgres_python_conn.execute_query("""
            SELECT
                'infinity'::float as pos_inf,
                '-infinity'::float as neg_inf,
                'NaN'::float as nan_val
        """)

        assert isinstance(result, str)
        columns, rows = parse_tsv(result)

        assert len(columns) == 3
        assert len(rows) == 1
        row = rows[0]

        # Special float values should be represented as strings
        assert 'infinity' in row[0].lower() or 'inf' in row[0].lower()
        assert '-infinity' in row[1].lower() or '-inf' in row[1].lower()
        assert 'nan' in row[2].lower()

    async def test_cli_connector_serialization(self, postgres_cli_conn):
        """Test that CLI connector returns TSV results"""
        result = await postgres_cli_conn.execute_query("""
            SELECT
                1 as id,
                'user1' as username,
                'user@example.com' as email,
                NOW() as created_at
        """)

        assert isinstance(result, str)
        columns, rows = parse_tsv(result)

        assert len(columns) == 4
        assert len(rows) == 1


@pytest.mark.docker
@pytest.mark.anyio
class TestClickHouseSerialization:
    """Test ClickHouse result serialization"""

    async def test_basic_types(self, clickhouse_python_conn):
        """Test basic ClickHouse data types in TSV"""
        result = await clickhouse_python_conn.execute_query("""
            SELECT
                toUInt32(1) as uint_val,
                toInt32(-1) as int_val,
                toFloat32(1.5) as float_val,
                toString('test') as string_val,
                1 = 1 as bool_val
        """)

        assert isinstance(result, str)
        columns, rows = parse_tsv(result)

        assert len(columns) == 5
        assert len(rows) == 1
        row = rows[0]

        assert row[0] == '1'  # uint_val
        assert row[1] == '-1'  # int_val
        assert row[2] == '1.5'  # float_val
        assert row[3] == 'test'  # string_val
        assert row[4] in ['1', 'true', 'True']  # bool_val

    async def test_datetime_types(self, clickhouse_python_conn):
        """Test ClickHouse datetime types in TSV"""
        result = await clickhouse_python_conn.execute_query("""
            SELECT
                today() as date_val,
                now() as datetime_val,
                toDateTime64(now(), 3) as datetime64_val
        """)

        assert isinstance(result, str)
        columns, rows = parse_tsv(result)

        assert len(columns) == 3
        assert len(rows) == 1
        row = rows[0]

        # Date/time values should be present
        assert row[0]  # date_val
        assert row[1]  # datetime_val
        assert row[2]  # datetime64_val

    async def test_clickhouse_arrays(self, clickhouse_python_conn):
        """Test ClickHouse arrays in TSV"""
        result = await clickhouse_python_conn.execute_query("""
            SELECT
                [1, 2, 3] as int_array,
                ['a', 'b', 'c'] as string_array,
                [toDate('2024-01-01'), toDate('2024-01-02')] as date_array
        """)

        assert isinstance(result, str)
        columns, rows = parse_tsv(result)

        assert len(columns) == 3
        assert len(rows) == 1
        row = rows[0]

        # Arrays are represented as ClickHouse array literals in TSV
        assert '[' in row[0] and ']' in row[0]  # int_array
        assert '[' in row[1] and ']' in row[1]  # string_array
        assert '[' in row[2] and ']' in row[2]  # date_array

    async def test_clickhouse_tuples(self, clickhouse_python_conn):
        """Test ClickHouse tuples in TSV"""
        result = await clickhouse_python_conn.execute_query("""
            SELECT
                tuple(1, 'a', 2.5) as mixed_tuple,
                tuple(1, 2, 3) as int_tuple
        """)

        assert isinstance(result, str)
        columns, rows = parse_tsv(result)

        assert len(columns) == 2
        assert len(rows) == 1
        row = rows[0]

        # Tuples are represented as ClickHouse tuple literals in TSV
        assert '(' in row[0] and ')' in row[0]  # mixed_tuple
        assert '(' in row[1] and ')' in row[1]  # int_tuple

    async def test_clickhouse_special_types(self, clickhouse_python_conn):
        """Test ClickHouse special types in TSV"""
        result = await clickhouse_python_conn.execute_query("""
            SELECT
                generateUUIDv4() as uuid_val,
                toIPv4('192.168.1.1') as ipv4_val,
                toIPv6('::1') as ipv6_val
        """)

        assert isinstance(result, str)
        columns, rows = parse_tsv(result)

        assert len(columns) == 3
        assert len(rows) == 1
        row = rows[0]

        # Special types should be present as strings
        assert row[0]  # uuid_val should be a UUID string
        assert '192.168.1.1' in row[1]  # ipv4_val
        assert '::1' in row[2] or '0000:0000' in row[2]  # ipv6_val

    async def test_cli_connector_serialization(self, clickhouse_cli_conn):
        """Test that ClickHouse CLI connector returns TSV"""
        result = await clickhouse_cli_conn.execute_query("""
            SELECT
                1 as event_id,
                now() as event_time,
                100 as user_id,
                'click' as event_type
        """)

        assert isinstance(result, str)
        columns, rows = parse_tsv(result)

        assert len(columns) == 4
        assert len(rows) == 1


@pytest.mark.docker
@pytest.mark.anyio
class TestEdgeCaseSerialization:
    """Test edge cases in result serialization"""

    async def test_empty_result(self, postgres_python_conn):
        """Test empty result set in TSV"""
        result = await postgres_python_conn.execute_query(
            "SELECT * FROM (VALUES (1), (2), (3)) AS t(id) WHERE id = -999"
        )

        assert isinstance(result, str)
        columns, rows = parse_tsv(result)

        # Should have columns but no rows
        assert len(columns) > 0
        assert len(rows) == 0

    async def test_large_numbers(self, postgres_python_conn):
        """Test very large numbers in TSV"""
        result = await postgres_python_conn.execute_query("""
            SELECT
                9223372036854775807 as max_bigint,
                -9223372036854775808 as min_bigint,
                99999999999999999999999999999.9999 as large_decimal
        """)

        assert isinstance(result, str)
        columns, rows = parse_tsv(result)

        assert len(columns) == 3
        assert len(rows) == 1
        row = rows[0]

        # Large numbers should be present as strings
        assert '9223372036854775807' in row[0]  # max_bigint
        assert '-9223372036854775808' in row[1]  # min_bigint
        assert '999999' in row[2]  # large_decimal

    async def test_unicode_strings(self, postgres_python_conn):
        """Test Unicode strings in TSV"""
        result = await postgres_python_conn.execute_query("""
            SELECT
                '你好世界' as chinese,
                '🚀🌟😊' as emojis,
                'Ñoño' as spanish,
                'Москва' as russian
        """)

        assert isinstance(result, str)
        columns, rows = parse_tsv(result)

        assert len(columns) == 4
        assert len(rows) == 1
        row = rows[0]

        # Unicode should be properly handled in TSV
        assert '你好世界' in row[0] or row[0] == '你好世界'  # chinese
        assert '🚀' in row[1] or '😊' in row[1]  # emojis

        # Values should be present in TSV
        # Note: TSV itself is a string format, no need for JSON serialization

    async def test_null_heavy_result(self, postgres_python_conn):
        """Test serialization with many NULL values"""
        result = await postgres_python_conn.execute_query("""
            SELECT
                NULL as col1,
                NULL as col2,
                'value' as col3,
                NULL as col4
        """)

        assert isinstance(result, str)
        columns, rows = parse_tsv(result)

        assert len(columns) == 4
        assert len(rows) == 1
        row = rows[0]

        # NULLs in TSV are represented as empty strings or \N
        assert row[0] in ['', '\\N', 'NULL']  # col1
        assert row[1] in ['', '\\N', 'NULL']  # col2
        assert row[2] == 'value'  # col3
        assert row[3] in ['', '\\N', 'NULL']  # col4


@pytest.mark.integration
@pytest.mark.docker
@pytest.mark.anyio
class TestMCPSerialization:
    """Test serialization through the full MCP protocol stack"""

    async def test_mcp_datetime_handling(self, integration_client):
        """Test that datetime values work through MCP protocol"""
        from tests.conftest import execute_query

        # This query has datetime columns
        result = await execute_query(
            integration_client,
            "test_postgres_python",
            "SELECT id, username, created_at FROM users LIMIT 2"
        )

        assert result["success"]
        assert len(result["rows"]) > 0

        # Results should have datetime values as strings in TSV
        for row in result["rows"]:
            assert len(row) == 3  # id, username, created_at
            # created_at should be a datetime string
            assert row[2]  # Should not be empty

    async def test_mcp_mixed_types(self, integration_client):
        """Test mixed data types through MCP"""
        from tests.conftest import execute_query

        result = await execute_query(
            integration_client,
            "test_postgres_python",
            """
            SELECT
                COUNT(*) as total_count,
                MAX(id) as max_id,
                MIN(created_at) as earliest_date
            FROM users
            """
        )

        assert result["success"]
        row = result["rows"][0]

        # Results should have mixed types as strings in TSV
        assert len(row) == 3  # total_count, max_id, earliest_date
        # All values should be strings in TSV format
        assert row[0]  # count
        assert row[1]  # max_id
        assert row[2]  # earliest_date