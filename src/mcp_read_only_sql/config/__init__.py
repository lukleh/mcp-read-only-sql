"""
Configuration module for connection management
"""

from .connection import Connection, Server, SSHTunnelConfig
from .loader import load_connections, load_connections_from_text

__all__ = [
    "Connection",
    "Server",
    "SSHTunnelConfig",
    "load_connections",
    "load_connections_from_text",
]
