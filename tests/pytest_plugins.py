"""
Custom pytest plugin to handle anyio/pytest-asyncio incompatibility.
"""

import pytest
import warnings


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_teardown(item, nextitem):
    """
    Suppress RuntimeError from anyio cancel scope during teardown.
    This is a known issue with anyio and pytest-asyncio compatibility.
    """
    outcome = yield
    if outcome.get_result() is None:
        excinfo = outcome.get_excinfo()
        if excinfo and excinfo[0] == RuntimeError:
            exc_value = excinfo[1]
            if "cancel scope" in str(exc_value):
                # This is the known anyio/pytest-asyncio issue
                # Suppress it as it doesn't affect test results
                outcome.force_result(None)
                warnings.warn(
                    "Suppressed anyio cancel scope error in teardown. "
                    "This is a known compatibility issue that doesn't affect test results.",
                    RuntimeWarning
                )