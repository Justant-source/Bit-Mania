"""YAML configuration loader with environment-variable substitution."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml

# Default config directory sits next to the cryptoengine package root.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_CONFIG_DIR = _PROJECT_ROOT / "config"

# Pattern: ${VAR_NAME} or ${VAR_NAME:-default}
_ENV_RE = re.compile(r"\$\{(?P<var>[A-Za-z_][A-Za-z0-9_]*)(?::-(?P<default>[^}]*))?\}")


def _substitute_env(value: str) -> str:
    """Replace ``${VAR}`` / ``${VAR:-fallback}`` tokens in a string."""

    def _replace(match: re.Match[str]) -> str:
        var = match.group("var")
        default = match.group("default")
        env_val = os.environ.get(var)
        if env_val is not None:
            return env_val
        if default is not None:
            return default
        raise EnvironmentError(
            f"Environment variable '{var}' is not set and no default was provided"
        )

    return _ENV_RE.sub(_replace, value)


def _walk(obj: Any) -> Any:
    """Recursively walk a parsed YAML tree and substitute env vars in strings."""
    if isinstance(obj, str):
        return _substitute_env(obj)
    if isinstance(obj, dict):
        return {k: _walk(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_walk(item) for item in obj]
    return obj


def load_config(
    name: str = "default",
    config_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Load and merge a YAML config file.

    Resolution order:
      1. ``<config_dir>/<name>.yaml``
      2. ``<config_dir>/<name>.yml``

    Environment variables in values are expanded (``${VAR:-fallback}``).
    """
    base = Path(config_dir) if config_dir else _CONFIG_DIR

    for suffix in (".yaml", ".yml"):
        path = base / f"{name}{suffix}"
        if path.is_file():
            raw_text = path.read_text(encoding="utf-8")
            parsed = yaml.safe_load(raw_text) or {}
            return _walk(parsed)  # type: ignore[return-value]

    raise FileNotFoundError(
        f"No config file found for name='{name}' in {base}"
    )


def load_all_configs(
    config_dir: str | Path | None = None,
) -> dict[str, dict[str, Any]]:
    """Load every YAML file in *config_dir* and return ``{stem: data}``."""
    base = Path(config_dir) if config_dir else _CONFIG_DIR
    result: dict[str, dict[str, Any]] = {}
    for path in sorted(base.glob("*.y*ml")):
        result[path.stem] = load_config(path.stem, config_dir=base)
    return result
