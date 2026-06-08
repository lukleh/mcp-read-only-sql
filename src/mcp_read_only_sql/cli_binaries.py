"""Resolution of external CLI client binaries (psql, clickhouse-client).

The CLI connectors shell out to a database client. Relying on a bare command
name only works when the client happens to be on ``PATH``; a common failure is
macOS Homebrew, where ``psql`` ships with the keg-only ``libpq`` formula and is
not symlinked onto the default ``PATH`` (the server then fails with
"psql: command not found").

``resolve_cli_binary`` locates the client without hardcoding install locations
as the primary mechanism. Resolution order:

1. Explicit override via ``MCP_READ_ONLY_SQL_<BINARY>_PATH`` (matches the
   project's existing ``MCP_READ_ONLY_SQL_*`` env convention).
2. :func:`shutil.which` -- honors ``PATH`` (the normal case on Linux and any
   correctly configured environment).
3. OS-aware fallback directories, queried dynamically where possible:
   - macOS: ask Homebrew (``brew --prefix``) where keg-only ``libpq`` / the
     ``postgresql`` formulae live, plus the standard Homebrew opt dirs.
   - Linux: standard packaged PostgreSQL layouts (Debian/Ubuntu, PGDG/RHEL).

If every step fails, a :class:`FileNotFoundError` is raised naming the override
env var so the user can point at the binary explicitly.
"""

from __future__ import annotations

import glob
import os
import shutil
import subprocess
import sys

from .runtime_paths import ENV_PREFIX

# Upper bound on the optional `brew --prefix` probe so a misbehaving brew can
# never hang query execution.
_BREW_PROBE_TIMEOUT = 5  # seconds

# Homebrew formulae that provide each client, in preference order.
_BREW_FORMULAE: dict[str, list[str]] = {
    "psql": ["libpq", "postgresql@18", "postgresql@17", "postgresql@16", "postgresql"],
    "clickhouse-client": ["clickhouse"],
}


def env_var_for(binary: str) -> str:
    """Return the override env var name for ``binary``.

    ``psql`` -> ``MCP_READ_ONLY_SQL_PSQL_PATH``;
    ``clickhouse-client`` -> ``MCP_READ_ONLY_SQL_CLICKHOUSE_CLIENT_PATH``.
    """
    slug = binary.upper().replace("-", "_")
    return f"{ENV_PREFIX}_{slug}_PATH"


def _is_executable(path: str) -> bool:
    return bool(path) and os.path.isfile(path) and os.access(path, os.X_OK)


def _brew_prefix(formula: str) -> str | None:
    """Return the Homebrew prefix for ``formula``, or ``None`` if unavailable.

    Note: this uses a blocking ``subprocess.run``. ``resolve_cli_binary`` is
    synchronous but is called from within the connectors' async ``_run_query``,
    so this probe blocks the event loop for its duration (bounded by
    ``_BREW_PROBE_TIMEOUT`` per call). It only runs on the fallback path (binary
    not pinned and not on PATH, macOS only) and ``BaseCLIConnector`` caches the
    resolved path per connector, so in practice it runs at most once per
    connector rather than per query. If that ever becomes a problem (e.g. many
    connectors resolving concurrently), move the call onto a thread via
    ``asyncio.to_thread`` or lower ``_BREW_PROBE_TIMEOUT``.
    """
    brew = shutil.which("brew")
    if not brew:
        return None
    try:
        result = subprocess.run(
            [brew, "--prefix", formula],
            capture_output=True,
            text=True,
            timeout=_BREW_PROBE_TIMEOUT,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    prefix = result.stdout.strip()
    return prefix or None


def _candidate_dirs(binary: str) -> list[str]:
    """OS-aware fallback directories to search for ``binary``.

    Evaluated only when the binary is neither pinned via env var nor found on
    ``PATH``. Homebrew prefixes are resolved dynamically; the static entries are
    last-resort best effort.
    """
    dirs: list[str] = []

    if sys.platform == "darwin":
        for formula in _BREW_FORMULAE.get(binary, []):
            prefix = _brew_prefix(formula)
            if prefix:
                dirs.append(os.path.join(prefix, "bin"))
        if binary == "psql":
            # Standard Homebrew keg-only libpq locations (Apple Silicon, Intel).
            dirs += ["/opt/homebrew/opt/libpq/bin", "/usr/local/opt/libpq/bin"]
    elif sys.platform.startswith("linux"):
        if binary == "psql":
            # Debian/Ubuntu (postgresql-client-NN) and PGDG/RHEL layouts;
            # newest version dir first. clickhouse-client has no equivalent
            # fallback: its distro package installs onto PATH, so which() above
            # already covers it.
            dirs += sorted(glob.glob("/usr/lib/postgresql/*/bin"), reverse=True)
            dirs += sorted(glob.glob("/usr/pgsql-*/bin"), reverse=True)

    return dirs


def resolve_cli_binary(binary: str) -> str:
    """Resolve the absolute path to a CLI client binary.

    Args:
        binary: The client command name, e.g. ``"psql"`` or
            ``"clickhouse-client"``.

    Returns:
        Absolute path to an executable.

    Raises:
        FileNotFoundError: If the binary cannot be located. The message names
            the override env var.
    """
    env_var = env_var_for(binary)

    override = os.environ.get(env_var)
    if override:
        expanded = os.path.expanduser(override)
        if _is_executable(expanded):
            return expanded
        raise FileNotFoundError(
            f"{binary}: {env_var} is set to '{override}', but that is not an "
            f"executable file."
        )

    found = shutil.which(binary)
    if found:
        return found

    for directory in _candidate_dirs(binary):
        candidate = os.path.join(directory, binary)
        if _is_executable(candidate):
            return candidate

    raise FileNotFoundError(
        f"{binary}: command not found. Install the client tools, add them to "
        f"PATH, or set {env_var} to the full path of the {binary} executable."
    )
