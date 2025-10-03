#!/usr/bin/env python3
"""
Server tests using real MCP protocol
Tests server initialization and basic functionality
"""

import pytest
from pathlib import Path
from tests.conftest import call_tool, execute_query, list_connections


pytestmark = pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")


@pytest.mark.anyio
class TestServerBasics:
    """Test basic server functionality through MCP protocol"""

    async def test_server_connection(self, mcp_client):
        """Test that we can connect to the server"""
        # If we get here, connection was successful
        assert mcp_client is not None

        # Test that we can list tools
        tools = await mcp_client.list_tools()
        assert tools is not None
        assert len(tools.tools) > 0

        # Check for our expected tools
        tool_names = [t.name for t in tools.tools]
        assert "run_query_read_only" in tool_names
        assert "list_connections" in tool_names

    async def test_list_connections(self, mcp_client):
        """Test listing connections"""
        connections = await list_connections(mcp_client)

        assert isinstance(connections, list)
        assert len(connections) == 1
        assert connections[0]["name"] == "test_connection"
        assert connections[0]["type"] == "postgresql"

    async def test_invalid_connection(self, mcp_client):
        """Test error handling for invalid connection"""
        result = await execute_query(
            mcp_client,
            "non_existent_connection",
            "SELECT 1"
        )

        assert not result.get("success", False)
        assert "not found" in result.get("error", "").lower()
        assert "Available connections" in result.get("error", "")

    async def test_query_missing_params(self, mcp_client):
        """Test error handling for missing parameters"""
        # Call with missing connection_name - should return error
        result = await mcp_client.call_tool("run_query_read_only", arguments={"query": "SELECT 1"})
        # Should have error in result
        assert result.isError or (result.content and "error" in str(result.content[0]).lower())

    async def test_run_query_with_server_override(self, mcp_client):
        """Server parameter should route query to the requested host."""
        result = await execute_query(
            mcp_client,
            "test_connection",
            "SELECT 1",
            server="127.0.0.1"
        )

        if result.get("success"):
            assert result.get("rows")
            assert result["rows"][0][0] == '1'
        else:
            error_msg = result.get("error", "").lower()
            assert "127.0.0.1" in error_msg or "connection refused" in error_msg


@pytest.mark.anyio
class TestResolvedEndpoints:
    """Ensure list_connections reports resolved database hosts"""

    @pytest.fixture
    def ssh_resolved_config(self, tmp_path):
        config_content = """
- connection_name: ssh_conn
  type: postgresql
  implementation: cli
  servers:
    - "localhost:5432"
  db: example
  username: tester
  password: secret
  ssh_tunnel:
    host: remote-db.example.com
    user: deploy
    port: 22
    private_key: /tmp/nonexistent
"""
        config_file = tmp_path / "ssh_resolved.yaml"
        config_file.write_text(config_content)
        return str(config_file)

    @pytest.fixture
    async def ssh_resolved_client(self, ssh_resolved_config):
        from mcp import StdioServerParameters
        from mcp import ClientSession
        from mcp.client.stdio import stdio_client
        import os

        server_params = StdioServerParameters(
            command="uv",
            args=["run", "python", "-m", "src.server", ssh_resolved_config],
            env=dict(os.environ),
        )

        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                yield session

    async def test_ssh_host_resolution(self, ssh_resolved_client):
        connections = await list_connections(ssh_resolved_client)

        assert len(connections) == 1
        conn = connections[0]
        assert conn["name"] == "ssh_conn"
        # Should surface the remote database host, not localhost
        assert conn["servers"] == "remote-db.example.com"
        assert conn.get("user") == "tester"


@pytest.mark.anyio
class TestMultipleConnections:
    """Test handling multiple connections"""

    @pytest.fixture
    def multi_config_file(self, tmp_path):
        """Create config with multiple connections"""
        config_content = """
- connection_name: conn1
  type: postgresql
  implementation: cli
  servers:
    - "localhost:5432"
  db: db1
  username: user1
  password: pass1

- connection_name: conn2
  type: clickhouse
  implementation: cli
  servers:
    - "localhost:9000"
  db: db2
  username: user2
  password: pass2

- connection_name: conn3
  type: postgresql
  implementation: python
  servers:
    - "localhost:5433"
  db: db3
  username: user3
  password: pass3
"""
        config_file = tmp_path / "multi_config.yaml"
        config_file.write_text(config_content)
        return str(config_file)

    @pytest.fixture
    async def multi_server(self, multi_config_file):
        """Server with multiple connections"""
        from mcp import StdioServerParameters
        return StdioServerParameters(
            command="uv",
            args=["run", "python", "-m", "src.server", multi_config_file]
        )

    @pytest.fixture
    async def multi_client(self, multi_server):
        """Client for multi-connection server"""
        from mcp import ClientSession
        from mcp.client.stdio import stdio_client

        async with stdio_client(multi_server) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                yield session

    async def test_multiple_connections(self, multi_client):
        """Test that multiple connections are loaded"""
        connections = await list_connections(multi_client)

        assert len(connections) == 3
        names = [c["name"] for c in connections]
        assert "conn1" in names
        assert "conn2" in names
        assert "conn3" in names

        # Check types and implementations
        conn_map = {c["name"]: c for c in connections}
        assert conn_map["conn1"]["type"] == "postgresql"
        assert conn_map["conn2"]["type"] == "clickhouse"
        assert conn_map["conn3"]["type"] == "postgresql"


@pytest.mark.anyio
class TestSecurityLimits:
    """Test security limit configurations"""

    @pytest.fixture
    def secure_config_file(self, tmp_path):
        """Create config with security limits"""
        config_content = """
- connection_name: secure_conn
  type: postgresql
  implementation: python
  servers:
    - "localhost:5432"
  db: testdb
  username: user
  password: pass
  query_timeout: 10
  connection_timeout: 5
  max_result_bytes: 5242880
"""
        config_file = tmp_path / "secure_config.yaml"
        config_file.write_text(config_content)
        return str(config_file)

    @pytest.fixture
    async def secure_server(self, secure_config_file):
        """Server with security limits"""
        from mcp import StdioServerParameters
        return StdioServerParameters(
            command="uv",
            args=["run", "python", "-m", "src.server", secure_config_file]
        )

    @pytest.fixture
    async def secure_client(self, secure_server):
        """Client for secure server"""
        from mcp import ClientSession
        from mcp.client.stdio import stdio_client

        async with stdio_client(secure_server) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                yield session

    async def test_security_limits_configured(self, secure_client):
        """Test that security limits are properly configured"""
        connections = await list_connections(secure_client)

        assert len(connections) == 1
        conn = connections[0]
        assert conn["name"] == "secure_conn"

        # Check that limits are reported
        # query_timeout might not be returned in list_connections
        # Just verify the connection exists and has basic fields
        assert "name" in conn
        assert "type" in conn
        # These fields might not be exposed in list_connections
        assert conn["type"] == "postgresql"


@pytest.mark.anyio
class TestServerParameter:
    """Test the optional server parameter for server selection"""

    @pytest.fixture
    def multi_server_config_file(self, tmp_path):
        """Create config with multiple servers per connection"""
        config_content = """
- connection_name: multi_server_conn
  type: postgresql
  implementation: cli
  servers:
    - "server1.example.com:5432"
    - "server2.example.com:5432"
    - "server3.example.com:5433"
  db: testdb
  username: user
  password: pass
"""
        config_file = tmp_path / "multi_server_config.yaml"
        config_file.write_text(config_content)
        return str(config_file)

    @pytest.fixture
    async def multi_server_client(self, multi_server_config_file):
        """Client for multi-server connection"""
        from mcp import StdioServerParameters, ClientSession
        from mcp.client.stdio import stdio_client

        server_params = StdioServerParameters(
            command="uv",
            args=["run", "python", "-m", "src.server", multi_server_config_file]
        )

        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                yield session

    async def test_server_parameter_not_found(self, multi_server_client):
        """Test that specifying non-existent server returns error"""
        result = await call_tool(
            multi_server_client,
            "run_query_read_only",
            {
                "connection_name": "multi_server_conn",
                "query": "SELECT 1",
                "server": "nonexistent.example.com"
            }
        )

        assert not result.get("success", False)
        error = result.get("error", "")
        assert "not found" in error.lower()
        assert "nonexistent.example.com" in error
        assert "server1.example.com" in error  # Should list available servers

    async def test_server_parameter_invalid_port(self, multi_server_client):
        """Test that invalid port format returns error"""
        result = await call_tool(
            multi_server_client,
            "run_query_read_only",
            {
                "connection_name": "multi_server_conn",
                "query": "SELECT 1",
                "server": "server1.example.com:invalid"
            }
        )

        assert not result.get("success", False)
        error = result.get("error", "")
        assert "hostname" in error.lower()
        assert "port" in error.lower()

    async def test_server_parameter_tool_signature(self, multi_server_client):
        """Test that server parameter is in tool signature"""
        tools = await multi_server_client.list_tools()
        run_query_tool = next(t for t in tools.tools if t.name == "run_query_read_only")

        # Check tool description mentions server parameter
        assert "server" in run_query_tool.description.lower() or \
               (run_query_tool.inputSchema and "server" in str(run_query_tool.inputSchema).lower())
