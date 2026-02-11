#!/usr/bin/env python3
"""
Error handling tests
Tests various error conditions and ensures proper error messages are returned
"""

import pytest
from src.connectors.postgresql.python import PostgreSQLPythonConnector
from src.connectors.postgresql.cli import PostgreSQLCLIConnector
from src.connectors.clickhouse.python import ClickHousePythonConnector
from src.connectors.clickhouse.cli import ClickHouseCLIConnector


@pytest.fixture
def postgres_python_conn():
    """PostgreSQL Python connector with valid config"""
    from conftest import make_connection
    config = make_connection({
        "connection_name": "test_pg",
        "type": "postgresql",
        "servers": [{"host": "localhost", "port": 5432}],
        "db": "testdb",
        "allowed_databases": ["testdb", "nonexistent_db"],
        "username": "testuser",
        "password": "testpass"
    })
    return PostgreSQLPythonConnector(config)


@pytest.fixture
def postgres_cli_conn():
    """PostgreSQL CLI connector with valid config"""
    from conftest import make_connection
    config = make_connection({
        "connection_name": "test_pg_cli",
        "type": "postgresql",
        "servers": [{"host": "localhost", "port": 5432}],
        "db": "testdb",
        "allowed_databases": ["testdb", "nonexistent_db"],
        "username": "testuser",
        "password": "testpass"
    })
    return PostgreSQLCLIConnector(config)


@pytest.fixture
def clickhouse_python_conn():
    """ClickHouse Python connector with valid config"""
    from conftest import make_connection
    config = make_connection({
        "connection_name": "test_ch",
        "type": "clickhouse",
        "servers": [{"host": "localhost", "port": 9000}],
        "db": "testdb",
        "username": "testuser",
        "password": "testpass"
    })
    return ClickHousePythonConnector(config)


@pytest.fixture
def clickhouse_cli_conn():
    """ClickHouse CLI connector with valid config"""
    from conftest import make_connection
    config = make_connection({
        "connection_name": "test_ch_cli",
        "type": "clickhouse",
        "servers": [{"host": "localhost", "port": 9000}],
        "db": "testdb",
        "username": "testuser",
        "password": "testpass"
    })
    return ClickHouseCLIConnector(config)


@pytest.mark.docker
@pytest.mark.anyio
class TestConnectionErrors:
    """Test connection error handling"""

    async def test_wrong_host(self):
        """Test connection to non-existent host"""
        from conftest import make_connection

        config = make_connection({
            "connection_name": "bad_host",
            "type": "postgresql",
            "servers": [{"host": "non.existent.host", "port": 5432}],
            "db": "testdb",
            "username": "testuser",
            "password": "testpass",
            "connection_timeout": 2
        })
        connector = PostgreSQLPythonConnector(config)

        with pytest.raises(RuntimeError) as exc_info:
            await connector.execute_query("SELECT 1")

        error_msg = str(exc_info.value).lower()
        # Error message should indicate connection issue
        assert any(word in error_msg for word in ["connection", "connect", "host", "resolve"])

    async def test_wrong_port(self):
        """Test connection to wrong port"""
        from conftest import make_connection

        config = make_connection({
            "connection_name": "bad_port",
            "type": "postgresql",
            "servers": [{"host": "localhost", "port": 9999}],  # Wrong port
            "db": "testdb",
            "username": "testuser",
            "password": "testpass",
            "connection_timeout": 2
        })
        connector = PostgreSQLPythonConnector(config)

        with pytest.raises(RuntimeError) as exc_info:
            await connector.execute_query("SELECT 1")

        error_msg = str(exc_info.value).lower()
        assert any(word in error_msg for word in ["connection", "refused", "port"])

    async def test_wrong_credentials(self):
        """Test connection with wrong credentials"""
        from conftest import make_connection

        config = make_connection({
            "connection_name": "bad_creds",
            "type": "postgresql",
            "servers": [{"host": "localhost", "port": 5432}],
            "db": "testdb",
            "username": "wronguser",
            "password": "wrongpass"
        })
        connector = PostgreSQLPythonConnector(config)

        with pytest.raises(RuntimeError) as exc_info:
            await connector.execute_query("SELECT 1")

        error_msg = str(exc_info.value).lower()
        # PostgreSQL typically returns "authentication failed" or "connection refused" for local tests
        assert any(word in error_msg for word in ["authentication", "password", "user", "connection", "refused"])

    async def test_cli_connection_error(self, postgres_cli_conn):
        """Test CLI connector handles connection errors"""
        # Temporarily break the connection by using wrong port
        from src.config.connection import Server
        postgres_cli_conn.servers = [Server(host="localhost", port=9999)]

        with pytest.raises(RuntimeError) as exc_info:
            await postgres_cli_conn.execute_query("SELECT 1")

        error_msg = str(exc_info.value).lower()
        assert "connection" in error_msg or "refused" in error_msg


@pytest.mark.docker
@pytest.mark.anyio
class TestDatabaseErrors:
    """Test database-related errors"""

    async def test_database_not_found_postgres(self, postgres_python_conn):
        """Test querying non-existent database in PostgreSQL"""
        with pytest.raises(RuntimeError) as exc_info:
            await postgres_python_conn.execute_query(
                "SELECT 1",
                database="nonexistent_db"
            )

        error_msg = str(exc_info.value).lower()
        assert any(word in error_msg for word in ["database", "does not exist", "not found"])

    async def test_database_not_found_clickhouse(self, clickhouse_python_conn):
        """Test querying non-existent database in ClickHouse"""
        with pytest.raises(RuntimeError) as exc_info:
            await clickhouse_python_conn.execute_query(
                "SELECT 1 FROM nonexistent_db.some_table"
            )

        error_msg = str(exc_info.value).lower()
        assert any(word in error_msg for word in ["database", "doesn't exist", "not exist", "unknown"])

    async def test_cli_database_not_found(self, postgres_cli_conn):
        """Test CLI connector with non-existent database"""
        with pytest.raises(RuntimeError) as exc_info:
            await postgres_cli_conn.execute_query(
                "SELECT 1",
                database="nonexistent_db"
            )

        error_msg = str(exc_info.value).lower()
        assert "database" in error_msg or "does not exist" in error_msg


@pytest.mark.docker
@pytest.mark.anyio
class TestTableErrors:
    """Test table-related errors"""

    async def test_table_not_found_postgres(self, postgres_python_conn):
        """Test querying non-existent table in PostgreSQL"""
        with pytest.raises(RuntimeError) as exc_info:
            await postgres_python_conn.execute_query(
                "SELECT * FROM nonexistent_table"
            )

        error_msg = str(exc_info.value).lower()
        assert any(word in error_msg for word in ["relation", "does not exist", "table"])

    async def test_table_not_found_clickhouse(self, clickhouse_python_conn):
        """Test querying non-existent table in ClickHouse"""
        with pytest.raises(RuntimeError) as exc_info:
            await clickhouse_python_conn.execute_query(
                "SELECT * FROM testdb.nonexistent_table"
            )

        error_msg = str(exc_info.value).lower()
        assert any(word in error_msg for word in ["table", "doesn't exist", "not exist", "unknown"])

    async def test_cli_table_not_found(self, postgres_cli_conn):
        """Test CLI connector with non-existent table"""
        with pytest.raises(RuntimeError) as exc_info:
            await postgres_cli_conn.execute_query(
                "SELECT * FROM nonexistent_table"
            )

        error_msg = str(exc_info.value).lower()
        assert any(word in error_msg for word in ["relation", "does not exist", "table"])

    async def test_column_not_found_postgres(self, postgres_python_conn):
        """Test querying non-existent column in PostgreSQL"""
        with pytest.raises(RuntimeError) as exc_info:
            await postgres_python_conn.execute_query(
                "SELECT nonexistent_column FROM users"
            )

        error_msg = str(exc_info.value).lower()
        assert "column" in error_msg or "does not exist" in error_msg or "nonexistent_column" in error_msg


@pytest.mark.docker
@pytest.mark.anyio
class TestSyntaxErrors:
    """Test SQL syntax error handling"""

    async def test_syntax_error_postgres(self, postgres_python_conn):
        """Test invalid SQL syntax in PostgreSQL"""
        with pytest.raises(RuntimeError) as exc_info:
            await postgres_python_conn.execute_query(
                "SELCT * FROM users"  # Typo: SELCT instead of SELECT
            )

        error_msg = str(exc_info.value).lower()
        assert any(word in error_msg for word in ["syntax", "error", "selct"])

    async def test_syntax_error_clickhouse(self, clickhouse_python_conn):
        """Test invalid SQL syntax in ClickHouse"""
        with pytest.raises(RuntimeError) as exc_info:
            await clickhouse_python_conn.execute_query(
                "SELCT * FROM testdb.events"  # Typo
            )

        error_msg = str(exc_info.value).lower()
        assert any(word in error_msg for word in ["syntax", "error", "unknown", "selct"])

    async def test_cli_syntax_error(self, postgres_cli_conn):
        """Test CLI connector with syntax error"""
        with pytest.raises(RuntimeError) as exc_info:
            await postgres_cli_conn.execute_query(
                "INVALID SQL QUERY"
            )

        error_msg = str(exc_info.value).lower()
        assert "syntax" in error_msg or "error" in error_msg


@pytest.mark.docker
@pytest.mark.anyio
class TestErrorMessageQuality:
    """Test that error messages are helpful and descriptive"""

    async def test_error_includes_query_context(self, postgres_python_conn):
        """Test that errors include helpful context"""
        query = "SELECT * FROM this_table_definitely_does_not_exist"
        with pytest.raises(RuntimeError) as exc_info:
            await postgres_python_conn.execute_query(query)

        error_msg = str(exc_info.value)
        # Error should mention the table name or relation
        assert "this_table_definitely_does_not_exist" in error_msg or "relation" in error_msg.lower()

    async def test_error_format_consistency(self, postgres_python_conn):
        """Test that errors are raised consistently"""
        with pytest.raises(RuntimeError) as exc_info:
            await postgres_python_conn.execute_query(
                "SELECT * FROM nonexistent"
            )

        # Error should be a RuntimeError with a message
        assert exc_info.type == RuntimeError
        assert str(exc_info.value)  # Should have a message
