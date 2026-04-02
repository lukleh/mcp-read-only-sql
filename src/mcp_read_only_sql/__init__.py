"""Package metadata for mcp-read-only-sql."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("mcp-read-only-sql")
except PackageNotFoundError:
    __version__ = "0+unknown"
