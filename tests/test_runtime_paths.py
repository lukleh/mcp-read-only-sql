from mcp_read_only_sql.runtime_paths import resolve_runtime_paths


def test_resolve_runtime_paths_env_overrides(monkeypatch, tmp_path):
    config_dir = tmp_path / "config"
    state_dir = tmp_path / "state"
    cache_dir = tmp_path / "cache"

    monkeypatch.setenv("MCP_READ_ONLY_SQL_CONFIG_DIR", str(config_dir))
    monkeypatch.setenv("MCP_READ_ONLY_SQL_STATE_DIR", str(state_dir))
    monkeypatch.setenv("MCP_READ_ONLY_SQL_CACHE_DIR", str(cache_dir))

    runtime_paths = resolve_runtime_paths()

    assert runtime_paths.config_dir == config_dir
    assert runtime_paths.state_dir == state_dir
    assert runtime_paths.cache_dir == cache_dir
    assert runtime_paths.connections_file == config_dir / "connections.yaml"
