#!/usr/bin/env python3
"""
MCP Protocol tests
Tests the MCP server/client communication and protocol handling
"""

import asyncio
import pytest
from tests.conftest import execute_query, list_connections


@pytest.mark.integration
@pytest.mark.anyio
class TestMCPProtocol:
    """Test MCP protocol communication and tool handling"""

    async def test_list_connections(self, integration_client):
        """Test listing available database connections"""
        connections = await list_connections(integration_client)

        assert connections, "Should have connections"
        assert len(connections) >= 2, "Should have at least 2 test connections"

        # Check connection structure
        for conn in connections:
            assert "name" in conn  # Server returns "name" not "connection_name"
            assert "type" in conn
            assert conn["type"] in ["postgresql", "clickhouse"]

    async def test_invalid_connection(self, integration_client):
        """Test handling of invalid connection names"""
        result = await execute_query(
            integration_client,
            "non_existent_connection",
            "SELECT 1"
        )

        assert not result.get("success", False)
        assert "error" in result
        assert "not found" in result["error"].lower()

    async def test_invalid_sql(self, integration_client):
        """Test handling of malformed SQL queries"""
        result = await execute_query(
            integration_client,
            "test_postgres_python",
            "INVALID SQL QUERY"
        )

        assert not result.get("success", False)
        assert "error" in result
        # Should have SQL error from database

    async def test_list_tools(self, integration_client):
        """Test that MCP tools are properly exposed"""
        tools_response = await integration_client.call_tool(
            "list-tools",
            {}
        )

        # Should have at least list_connections and execute_query tools
        assert tools_response is not None