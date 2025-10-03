#!/usr/bin/env python3
"""
Docker environment tests for MCP SQL Server
Verifies that Docker databases are accessible and basic queries work.
The main security testing (read-only, timeouts, size limits) is in test_security.py
"""

import pytest
from src.config.parser import ConfigParser
from src.connectors.postgresql.python import PostgreSQLPythonConnector
from src.connectors.clickhouse.python import ClickHousePythonConnector


@pytest.fixture(scope="module")
def test_connections():
    """Load test connections configuration"""
    parser = ConfigParser("tests/connections-test.yaml")
    return parser.load_config()


@pytest.fixture
def postgres_conn(test_connections):
    """Get PostgreSQL test connection"""
    from conftest import make_connection
    config = next((c for c in test_connections if c["connection_name"] == "test_postgres"), None)
    if not config:
        pytest.skip("test_postgres connection not configured")
    # Ensure password is set for tests
    if not config.get("password"):
        config["password"] = "testpass"
    return PostgreSQLPythonConnector(make_connection(config))


@pytest.fixture
def clickhouse_conn(test_connections):
    """Get ClickHouse test connection"""
    from conftest import make_connection
    config = next((c for c in test_connections if c["connection_name"] == "test_clickhouse"), None)
    if not config:
        pytest.skip("test_clickhouse connection not configured")
    # Ensure password is set for tests
    if not config.get("password"):
        config["password"] = "testpass"
    return ClickHousePythonConnector(make_connection(config))


@pytest.mark.docker
@pytest.mark.anyio
class TestPostgreSQLData:
    """Test PostgreSQL connectivity and basic queries work"""

    async def test_database_has_data(self, postgres_conn):
        """Test that database has some data to query"""
        result = await postgres_conn.execute_query("SELECT COUNT(*) as count FROM users")
        assert isinstance(result, str)
        lines = result.strip().split('\n')
        assert len(lines) >= 2  # Header + data
        row_values = lines[1].split('\t')
        assert int(row_values[0]) > 0  # Just verify there's data

    async def test_select_queries_work(self, postgres_conn):
        """Test that SELECT queries work properly"""
        # Simple select
        result = await postgres_conn.execute_query("SELECT 1 as test")
        assert isinstance(result, str)
        lines = result.strip().split('\n')
        row_values = lines[1].split('\t')
        assert row_values[0] == '1'

        # Query with WHERE clause
        result = await postgres_conn.execute_query(
            "SELECT * FROM users WHERE id > 0 LIMIT 5"
        )
        assert isinstance(result, str)
        lines = result.strip().split('\n')
        assert len(lines) > 1  # Has rows


@pytest.mark.docker
@pytest.mark.anyio
class TestClickHouseData:
    """Test ClickHouse connectivity and basic queries work"""

    async def test_database_has_data(self, clickhouse_conn):
        """Test that database has some data to query"""
        result = await clickhouse_conn.execute_query(
            "SELECT COUNT(*) as count FROM testdb.events"
        )
        assert isinstance(result, str)
        lines = result.strip().split('\n')
        assert len(lines) >= 2  # Header + data
        row_values = lines[1].split('\t')
        assert int(row_values[0]) > 0  # Just verify there's data

    async def test_select_queries_work(self, clickhouse_conn):
        """Test that SELECT queries work properly"""
        # Simple select
        result = await clickhouse_conn.execute_query("SELECT 1 as test")
        assert isinstance(result, str)
        lines = result.strip().split('\n')
        row_values = lines[1].split('\t')
        assert row_values[0] == '1'

        # Query with WHERE and GROUP BY
        result = await clickhouse_conn.execute_query("""
            SELECT event_type, COUNT(*) as count
            FROM testdb.events
            GROUP BY event_type
            LIMIT 5
        """)
        assert isinstance(result, str)
        lines = result.strip().split('\n')
        assert len(lines) > 1  # Has rows


@pytest.mark.docker
@pytest.mark.anyio
class TestCrossDatabase:
    """Test both databases work together"""

    async def test_both_databases_accessible(self, postgres_conn, clickhouse_conn):
        """Test that both databases are accessible and queryable"""
        # PostgreSQL
        pg_result = await postgres_conn.execute_query("SELECT 1 as test")
        assert isinstance(pg_result, str)
        lines = pg_result.strip().split('\n')
        assert lines[1].split('\t')[0] == '1'

        # ClickHouse
        ch_result = await clickhouse_conn.execute_query("SELECT 1 as test")
        assert isinstance(ch_result, str)
        lines = ch_result.strip().split('\n')
        assert lines[1].split('\t')[0] == '1'

        # Both have data
        pg_data = await postgres_conn.execute_query("SELECT COUNT(*) as c FROM users")
        assert isinstance(pg_data, str)
        lines = pg_data.strip().split('\n')
        assert int(lines[1].split('\t')[0]) > 0

        ch_data = await clickhouse_conn.execute_query("SELECT COUNT(*) as c FROM testdb.events")
        assert isinstance(ch_data, str)
        lines = ch_data.strip().split('\n')
        assert int(lines[1].split('\t')[0]) > 0