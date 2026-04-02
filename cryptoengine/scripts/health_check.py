#!/usr/bin/env python3
"""CryptoEngine health check script.

Checks:
  1. PostgreSQL connection
  2. Redis connection
  3. Docker containers running
  4. Bybit API reachable
  5. Disk space
  6. Memory usage

Exit 0 if ALL checks pass, exit 1 if ANY check fails.

Usage:
  python scripts/health_check.py
  python scripts/health_check.py --json         # JSON output
  python scripts/health_check.py --skip docker  # skip docker check
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import structlog

structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(structlog.INFO),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)
log = structlog.get_logger("health_check")

# Thresholds
MIN_DISK_FREE_GB = 5.0
MAX_MEMORY_USAGE_PCT = 90.0

DB_DSN = (
    f"postgresql://{os.getenv('DB_USER', 'cryptoengine')}"
    f":{os.getenv('DB_PASSWORD', 'cryptoengine')}"
    f"@{os.getenv('DB_HOST', 'localhost')}"
    f":{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME', 'cryptoengine')}"
)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

EXPECTED_CONTAINERS = [
    "postgres",
    "redis",
    "market-data",
    "strategy-orchestrator",
    "execution-engine",
    "funding-arb",
    "grid-trading",
    "telegram-bot",
    "dashboard",
]


@dataclass
class CheckResult:
    name: str
    passed: bool
    message: str
    details: dict[str, Any] = field(default_factory=dict)


# ------------------------------------------------------------------
# Individual checks
# ------------------------------------------------------------------

async def check_postgres() -> CheckResult:
    """Verify PostgreSQL connectivity and basic query."""
    try:
        import asyncpg
        conn = await asyncio.wait_for(asyncpg.connect(DB_DSN), timeout=10.0)
        try:
            version = await conn.fetchval("SELECT version()")
            table_count = await conn.fetchval(
                "SELECT count(*) FROM pg_tables WHERE schemaname = 'public'"
            )
            return CheckResult(
                name="PostgreSQL",
                passed=True,
                message=f"Connected. {table_count} tables.",
                details={"version": version, "tables": table_count},
            )
        finally:
            await conn.close()
    except Exception as exc:
        return CheckResult(
            name="PostgreSQL",
            passed=False,
            message=f"Connection failed: {exc}",
        )


async def check_redis() -> CheckResult:
    """Verify Redis connectivity."""
    try:
        import redis.asyncio as aioredis
        r = aioredis.from_url(REDIS_URL, decode_responses=True)
        try:
            pong = await asyncio.wait_for(r.ping(), timeout=5.0)
            info = await r.info("memory")
            used_mb = info.get("used_memory_human", "?")
            return CheckResult(
                name="Redis",
                passed=bool(pong),
                message=f"Connected. Memory: {used_mb}",
                details={"memory_used": used_mb},
            )
        finally:
            await r.aclose()
    except Exception as exc:
        return CheckResult(
            name="Redis",
            passed=False,
            message=f"Connection failed: {exc}",
        )


def check_docker_containers() -> CheckResult:
    """Check if expected Docker containers are running."""
    try:
        result = subprocess.run(
            ["docker", "compose", "ps", "--format", "json"],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode != 0:
            # Try legacy docker-compose
            result = subprocess.run(
                ["docker-compose", "ps", "--format", "json"],
                capture_output=True,
                text=True,
                timeout=15,
            )

        running: set[str] = set()
        if result.returncode == 0 and result.stdout.strip():
            for line in result.stdout.strip().split("\n"):
                try:
                    container = json.loads(line)
                    name = container.get("Name", container.get("Service", ""))
                    state = container.get("State", container.get("Status", ""))
                    if "running" in state.lower() or "up" in state.lower():
                        running.add(name.lower())
                except json.JSONDecodeError:
                    continue

        if not running:
            # Fallback: just check docker ps
            result2 = subprocess.run(
                ["docker", "ps", "--format", "{{.Names}}"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result2.returncode == 0:
                running = {n.strip().lower() for n in result2.stdout.strip().split("\n") if n.strip()}

        # Check for expected containers (partial name match)
        not_running = []
        for expected in EXPECTED_CONTAINERS:
            found = any(expected.lower() in name for name in running)
            if not found:
                not_running.append(expected)

        if not_running:
            return CheckResult(
                name="Docker Containers",
                passed=False,
                message=f"Missing: {', '.join(not_running)}",
                details={"running": sorted(running), "missing": not_running},
            )

        return CheckResult(
            name="Docker Containers",
            passed=True,
            message=f"All {len(EXPECTED_CONTAINERS)} containers running.",
            details={"running": sorted(running)},
        )

    except FileNotFoundError:
        return CheckResult(
            name="Docker Containers",
            passed=False,
            message="Docker CLI not found",
        )
    except Exception as exc:
        return CheckResult(
            name="Docker Containers",
            passed=False,
            message=f"Check failed: {exc}",
        )


async def check_bybit_api() -> CheckResult:
    """Check Bybit API reachability via a simple server time request."""
    try:
        import aiohttp
        url = "https://api.bybit.com/v5/market/time"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    server_time = data.get("result", {}).get("timeSecond", "?")
                    return CheckResult(
                        name="Bybit API",
                        passed=True,
                        message=f"Reachable. Server time: {server_time}",
                        details={"server_time": server_time},
                    )
                return CheckResult(
                    name="Bybit API",
                    passed=False,
                    message=f"HTTP {resp.status}",
                )
    except Exception as exc:
        return CheckResult(
            name="Bybit API",
            passed=False,
            message=f"Unreachable: {exc}",
        )


def check_disk_space() -> CheckResult:
    """Check available disk space."""
    try:
        usage = shutil.disk_usage("/")
        free_gb = usage.free / (1024 ** 3)
        total_gb = usage.total / (1024 ** 3)
        used_pct = (usage.used / usage.total) * 100

        passed = free_gb >= MIN_DISK_FREE_GB
        return CheckResult(
            name="Disk Space",
            passed=passed,
            message=f"{free_gb:.1f} GB free / {total_gb:.1f} GB total ({used_pct:.1f}% used)",
            details={
                "free_gb": round(free_gb, 2),
                "total_gb": round(total_gb, 2),
                "used_pct": round(used_pct, 2),
            },
        )
    except Exception as exc:
        return CheckResult(
            name="Disk Space",
            passed=False,
            message=f"Check failed: {exc}",
        )


def check_memory_usage() -> CheckResult:
    """Check system memory usage."""
    try:
        with open("/proc/meminfo") as f:
            meminfo = {}
            for line in f:
                parts = line.split(":")
                if len(parts) == 2:
                    key = parts[0].strip()
                    val = parts[1].strip().split()[0]
                    meminfo[key] = int(val)

        total_kb = meminfo.get("MemTotal", 0)
        available_kb = meminfo.get("MemAvailable", 0)

        if total_kb == 0:
            return CheckResult(
                name="Memory",
                passed=False,
                message="Cannot read memory info",
            )

        used_pct = ((total_kb - available_kb) / total_kb) * 100
        total_gb = total_kb / (1024 ** 2)
        available_gb = available_kb / (1024 ** 2)

        passed = used_pct <= MAX_MEMORY_USAGE_PCT
        return CheckResult(
            name="Memory",
            passed=passed,
            message=f"{available_gb:.1f} GB free / {total_gb:.1f} GB total ({used_pct:.1f}% used)",
            details={
                "total_gb": round(total_gb, 2),
                "available_gb": round(available_gb, 2),
                "used_pct": round(used_pct, 2),
            },
        )
    except FileNotFoundError:
        # Non-Linux: try psutil
        try:
            import psutil
            mem = psutil.virtual_memory()
            used_pct = mem.percent
            total_gb = mem.total / (1024 ** 3)
            available_gb = mem.available / (1024 ** 3)
            passed = used_pct <= MAX_MEMORY_USAGE_PCT
            return CheckResult(
                name="Memory",
                passed=passed,
                message=f"{available_gb:.1f} GB free / {total_gb:.1f} GB ({used_pct:.1f}% used)",
            )
        except ImportError:
            return CheckResult(
                name="Memory",
                passed=True,
                message="Cannot read memory info (non-Linux, psutil not installed)",
            )


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

async def run_health_checks(
    skip: set[str] | None = None,
    json_output: bool = False,
) -> bool:
    """Run all health checks. Returns True if all pass."""
    skip = skip or set()
    results: list[CheckResult] = []

    # Async checks
    checks: list[tuple[str, Any]] = [
        ("postgres", check_postgres),
        ("redis", check_redis),
        ("bybit", check_bybit_api),
    ]

    for name, check_fn in checks:
        if name in skip:
            log.info("check_skipped", check=name)
            continue
        results.append(await check_fn())

    # Sync checks
    if "docker" not in skip:
        results.append(check_docker_containers())
    if "disk" not in skip:
        results.append(check_disk_space())
    if "memory" not in skip:
        results.append(check_memory_usage())

    # Output
    all_passed = all(r.passed for r in results)

    if json_output:
        output = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "status": "healthy" if all_passed else "unhealthy",
            "checks": [
                {
                    "name": r.name,
                    "passed": r.passed,
                    "message": r.message,
                    "details": r.details,
                }
                for r in results
            ],
        }
        print(json.dumps(output, indent=2))
    else:
        print("\n" + "=" * 60)
        print("  CryptoEngine Health Check")
        print("=" * 60)
        for r in results:
            icon = "PASS" if r.passed else "FAIL"
            print(f"  [{icon}] {r.name}: {r.message}")
        print("=" * 60)
        status = "HEALTHY" if all_passed else "UNHEALTHY"
        print(f"  Status: {status}")
        print("=" * 60 + "\n")

    return all_passed


def main() -> None:
    parser = argparse.ArgumentParser(description="CryptoEngine health check")
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results as JSON",
    )
    parser.add_argument(
        "--skip",
        nargs="*",
        default=[],
        choices=["postgres", "redis", "docker", "bybit", "disk", "memory"],
        help="Checks to skip",
    )
    args = parser.parse_args()

    ok = asyncio.run(run_health_checks(set(args.skip), args.json))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
