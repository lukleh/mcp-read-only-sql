"""Hard timeout wrapper for all database operations"""
import asyncio
import logging
from typing import Any, Dict, Optional, Callable
from functools import wraps

logger = logging.getLogger(__name__)


class HardTimeoutError(Exception):
    """Raised when a hard timeout is reached"""
    pass


async def with_hard_timeout(
    coro,
    timeout_seconds: float,
    operation_name: str = "operation"
) -> Any:
    """
    Execute a coroutine with a hard timeout.

    This provides an absolute guarantee that the operation will not run longer
    than the specified timeout, preventing the MCP server from getting stuck.

    Args:
        coro: The coroutine to execute
        timeout_seconds: Maximum time allowed for the operation
        operation_name: Name of the operation for logging

    Returns:
        The result of the coroutine

    Raises:
        HardTimeoutError: If the hard timeout is exceeded
        Other exceptions: Propagated from the coroutine if they occur before hard timeout
    """
    try:
        return await asyncio.wait_for(coro, timeout=timeout_seconds)
    except TimeoutError as e:
        # Check if this is a database timeout (has specific prefixes) vs asyncio timeout
        error_msg = str(e)
        if any(prefix in error_msg for prefix in ["PostgreSQL:", "ClickHouse:", "psql:", "clickhouse-client:", "SSH:"]):
            # This is a database/connector timeout, not the hard timeout
            raise
        # This is the hard timeout from asyncio.wait_for
        logger.error(f"Hard timeout exceeded for {operation_name} after {timeout_seconds} seconds")
        raise HardTimeoutError(
            f"Operation '{operation_name}' exceeded hard timeout of {timeout_seconds} seconds"
        )
    except Exception:
        # Let other exceptions propagate
        raise


def hard_timeout(timeout_seconds: float = 30.0):
    """
    Decorator to add hard timeout to async methods.

    Usage:
        @hard_timeout(60.0)
        async def execute_query(self, query):
            ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Try to get operation name from function
            operation_name = func.__name__

            # Create the coroutine
            coro = func(*args, **kwargs)

            # Execute with hard timeout
            try:
                return await with_hard_timeout(coro, timeout_seconds, operation_name)
            except HardTimeoutError as e:
                # Return error response in the expected format
                return {
                    "success": False,
                    "error": str(e),
                    "rows": [],
                    "columns": [],
                    "row_count": 0
                }

        return wrapper
    return decorator


class HardTimeoutMixin:
    """
    Mixin class to add hard timeout capability to connectors.

    This should be mixed into the base connector class to ensure
    ALL database operations have hard timeout protection.
    """

    # Default hard timeout for all operations (can be overridden)
    DEFAULT_HARD_TIMEOUT = 30.0  # 30 seconds absolute maximum

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Get hard timeout from config or use default
        if hasattr(self, 'config'):
            self.hard_timeout = self.config.get('hard_timeout', self.DEFAULT_HARD_TIMEOUT)
        else:
            self.hard_timeout = self.DEFAULT_HARD_TIMEOUT

    async def execute_with_timeout(self, coro, operation_name: str = "query") -> Dict[str, Any]:
        """
        Execute a coroutine with hard timeout protection.

        This method wraps any database operation with an absolute timeout
        to prevent the MCP server from hanging.
        """
        try:
            return await with_hard_timeout(coro, self.hard_timeout, operation_name)
        except HardTimeoutError as e:
            logger.error(f"Hard timeout in connector: {e}")
            return {
                "success": False,
                "error": str(e),
                "rows": [],
                "columns": [],
                "row_count": 0
            }