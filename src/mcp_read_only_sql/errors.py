"""Exceptions shared between the connectors and the MCP tool boundary."""


class ConnectorError(RuntimeError):
    """An anticipated database, CLI, or SSH failure whose message is for the caller.

    Connectors raise this for driver errors, non-zero client exits, and SSH
    tunnel failures. The MCP tool boundary forwards its text to the caller.
    Any other ``RuntimeError`` is a bug and keeps the SDK's crash handling.
    """
