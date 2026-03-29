"""
Alternative conftest with fixed async fixture handling.
This avoids the anyio/pytest-asyncio incompatibility.
"""

import os
from pathlib import Path

import pytest_asyncio
from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest_asyncio.fixture
async def integration_client_fixed(integration_config_file, docker_check):
    """
    Fixed client fixture that avoids the cancel scope issue.
    Instead of using nested async context managers with yield,
    we manage the lifecycle explicitly.
    """
    server_params = StdioServerParameters(
        command="uv",
        args=[
            "--directory",
            str(PROJECT_ROOT),
            "run",
            "python",
            "-m",
            "mcp_read_only_sql.server",
            "--config-dir",
            str(Path(integration_config_file).parent),
        ],
        env=dict(os.environ)
    )

    # Store references to clean up later
    client_ctx = None
    session = None

    try:
        # Enter the stdio_client context
        client_ctx = stdio_client(server_params)
        read, write = await client_ctx.__aenter__()

        # Create and initialize session
        session = ClientSession(read, write)
        await session.__aenter__()
        await session.initialize()

        # Yield the session for the test to use
        yield session

    finally:
        # Clean up in reverse order
        if session:
            try:
                await session.__aexit__(None, None, None)
            except Exception as e:
                # Log but don't raise - we're in cleanup
                print(f"Session cleanup error: {e}")

        if client_ctx:
            try:
                await client_ctx.__aexit__(None, None, None)
            except Exception as e:
                # Log but don't raise - we're in cleanup
                print(f"Client cleanup error: {e}")


@pytest_asyncio.fixture
async def mcp_client_fixed(test_config_file):
    """
    Fixed MCP client fixture that avoids the cancel scope issue.
    """
    server_params = StdioServerParameters(
        command="uv",
        args=[
            "--directory",
            str(PROJECT_ROOT),
            "run",
            "python",
            "-m",
            "mcp_read_only_sql.server",
            "--config-dir",
            str(Path(test_config_file).parent),
        ],
        env=dict(os.environ)
    )

    client_ctx = None
    session = None

    try:
        client_ctx = stdio_client(server_params)
        read, write = await client_ctx.__aenter__()

        session = ClientSession(read, write)
        await session.__aenter__()
        await session.initialize()

        yield session

    finally:
        if session:
            try:
                await session.__aexit__(None, None, None)
            except Exception:
                pass

        if client_ctx:
            try:
                await client_ctx.__aexit__(None, None, None)
            except Exception:
                pass
