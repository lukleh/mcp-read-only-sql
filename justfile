# MCP Read-Only SQL Server
set dotenv-load

# Show available commands
default:
    @just --list

# Install dependencies
install:
    uv sync

# Run the server
run config="connections.yaml":
    uv run -- python -m src.server {{config}}

# Import DBeaver connections
import-dbeaver path="$HOME/Library/DBeaverData/workspace6/Clickhouse/.dbeaver":
    uv run -- python -m src.config.dbeaver_import {{path}}

# Import DBeaver without merging clusters
import-dbeaver-no-merge path="$HOME/Library/DBeaverData/workspace6/Clickhouse/.dbeaver":
    uv run -- python -m src.config.dbeaver_import --no-merge {{path}}

# Validate configuration file
validate config="connections.yaml":
    uv run -- python -m src.tools.validate_config {{config}}

# Test database connection(s)
test-connection connection="":
    #!/usr/bin/env bash
    if [ -z "{{connection}}" ]; then
        uv run -- python -m src.tools.test_connection
    else
        uv run -- python -m src.tools.test_connection {{connection}}
    fi

# Test SSH tunnel(s) connectivity only
test-ssh-tunnel connection="":
    #!/usr/bin/env bash
    if [ -z "{{connection}}" ]; then
        uv run -- python -m src.tools.test_ssh_tunnel
    else
        uv run -- python -m src.tools.test_ssh_tunnel {{connection}}
    fi

# Run tests with Docker isolation
test:
    ./run_tests.sh

