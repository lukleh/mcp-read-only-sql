"""Hard timeout wrapper for all database operations"""

import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)


class HardTimeoutError(Exception):
    """Raised when a hard timeout is reached"""



async def with_hard_timeout(
    coro, timeout_seconds: float, operation_name: str = "operation"
) -> Any:
    """
    Execute a coroutine with a hard timeout.

    The deadline begins cancellation; returning also waits for the operation's
    cleanup. Owned SSH processes get a short termination allowance before forced
    shutdown, and process construction/reaping can add scheduling time.

    Args:
        coro: The coroutine to execute
        timeout_seconds: Time allowed before requesting cancellation
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
        if any(
            prefix in error_msg
            for prefix in [
                "PostgreSQL:",
                "ClickHouse:",
                "psql:",
                "clickhouse-client:",
                "SSH:",
            ]
        ):
            # This is a database/connector timeout, not the hard timeout
            raise
        # This is the hard timeout from asyncio.wait_for
        logger.error(
            f"Hard timeout exceeded for {operation_name} after {timeout_seconds} seconds"
        )
        raise HardTimeoutError(
            f"Operation '{operation_name}' exceeded hard timeout of {timeout_seconds} seconds"
        )
    except Exception:
        # Let other exceptions propagate
        raise

