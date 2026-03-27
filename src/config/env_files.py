from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path

from dotenv import dotenv_values


def load_env_values(env_path: str | Path | None) -> dict[str, str]:
    if env_path is None:
        return {}

    path = Path(env_path).expanduser()
    if not path.exists():
        return {}

    return {
        key: value
        for key, value in dotenv_values(path).items()
        if key and value is not None
    }


def build_runtime_env(
    env_path: str | Path | None,
    runtime_env: Mapping[str, str] | None = None,
) -> dict[str, str]:
    merged = load_env_values(env_path)
    merged.update(runtime_env or os.environ)
    return merged
