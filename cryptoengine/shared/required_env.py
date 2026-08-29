"""Fail-closed secret loading. Never put credentials in source."""

from __future__ import annotations

import os
import re


def require_env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        raise RuntimeError(f"{name} is required (fail-closed; set in .env, not source)")
    return val


def redact_url(url: str) -> str:
    """Strip userinfo from redis:// / postgresql:// URLs for logs."""
    return re.sub(r"(://)[^@]*@", r"\1***@", url, count=1)
