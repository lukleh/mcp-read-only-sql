# MCP Read-Only SQL Server
# Show available commands
default:
    @just --list

# Install dependencies
install:
    uv sync

# Run the server
run:
    uv run -- python -m src.server

# Import DBeaver connections (supports name=value options)
# Examples:
#   just import-dbeaver
#   just import-dbeaver only="clickhouse-1.example.com grafana"
#   just import-dbeaver merge=false dry_run=true
# Options: path, only, merge, dry_run, output
import-dbeaver path="$HOME/Library/DBeaverData/workspace6/General/.dbeaver" only="" merge="true" dry_run="false" output="":
    #!/usr/bin/env bash
    set -euo pipefail
    args=()
    if [ -n "{{only}}" ]; then
        args+=(--only "{{only}}")
    fi
    case "{{merge}}" in
        false|0|no) args+=(--no-merge) ;;
    esac
    case "{{dry_run}}" in
        true|1|yes) args+=(--dry-run) ;;
    esac
    if [ -n "{{output}}" ]; then
        args+=(--output "{{output}}")
    fi
    uv run -- python -m src.config.dbeaver_import "{{path}}" "${args[@]}"

# Validate configuration file
validate:
    uv run -- python -m src.tools.validate_config

# Show resolved paths
print-paths:
    uv run -- python -m src.server --print-paths

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
