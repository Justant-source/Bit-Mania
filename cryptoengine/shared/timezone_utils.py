"""
shared/timezone_utils.py — UTC/KST 시간 변환 공통 유틸리티

저장: 항상 UTC (datetime with timezone.utc)
표시: KST (UTC+9)

사용 예시:
    from shared.timezone_utils import now_utc, now_kst, to_kst, format_kst

    # 현재 시각
    dt_utc = now_utc()   # 저장용
    dt_kst = now_kst()   # 표시용

    # UTC datetime → KST datetime
    dt_kst = to_kst(dt_utc)

    # KST 문자열 포맷 (로그 출력용)
    print(format_kst(dt_utc))  # "2026-04-06T14:30:00+09:00"

structlog 프로세서 사용 예시:
    from shared.timezone_utils import kst_timestamper
    shared_processors = [
        ...
        kst_timestamper,   # TimeStamper(fmt="iso") 대신 사용
        ...
    ]
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

# ── 상수 ─────────────────────────────────────────────────────────────────

UTC = timezone.utc
KST = timezone(timedelta(hours=9), name="KST")

_KST_FMT = "%Y-%m-%dT%H:%M:%S+09:00"


# ── 변환 함수 ─────────────────────────────────────────────────────────────


def now_utc() -> datetime:
    """현재 UTC 시각 반환 (저장용)."""
    return datetime.now(UTC)


def now_kst() -> datetime:
    """현재 KST 시각 반환 (표시용)."""
    return datetime.now(KST)


def to_kst(dt: datetime) -> datetime:
    """UTC datetime을 KST datetime으로 변환.

    naive datetime은 UTC로 간주한다.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(KST)


def to_utc(dt: datetime) -> datetime:
    """KST(또는 임의 tz) datetime을 UTC datetime으로 변환.

    naive datetime은 KST로 간주한다.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=KST)
    return dt.astimezone(UTC)


def format_kst(dt: datetime, fmt: str = _KST_FMT) -> str:
    """datetime을 KST 문자열로 포맷.

    Parameters
    ----------
    dt  : UTC 또는 임의 timezone의 datetime
    fmt : strftime 포맷 (기본: ISO 8601 KST)
    """
    return to_kst(dt).strftime(fmt)


# ── structlog 프로세서 ────────────────────────────────────────────────────


def kst_timestamper(
    logger: Any,
    method_name: str,
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    """structlog 프로세서: timestamp를 KST ISO 문자열로 삽입.

    DB 저장은 log_writer.py에서 datetime.now(UTC)로 별도 처리하므로
    여기서는 표시용 KST 문자열만 기록한다.

    TimeStamper(fmt="iso") 대신 이 함수를 shared_processors에 추가한다.
    """
    event_dict["timestamp"] = format_kst(now_utc())
    return event_dict


# ── 스크립트/백테스터용 독립 헬퍼 ─────────────────────────────────────────
# shared 패키지 없이도 동작하는 독립 함수 (scripts/, backtester/ 공용)
#
# 사용법:
#   from shared.timezone_utils import configure_kst_structlog
#   configure_kst_structlog(log_level=logging.INFO, json_output=False)


def configure_kst_structlog(
    log_level: int = 20,   # logging.INFO
    json_output: bool = False,
    *,
    extra_processors: list | None = None,
) -> None:
    """scripts / backtester용 경량 structlog 설정.

    TimeStamper(fmt="iso") 대신 kst_timestamper를 사용하여
    콘솔 출력을 KST 기준으로 표시한다.

    Parameters
    ----------
    log_level   : logging 수준 (logging.INFO=20, logging.DEBUG=10 ...)
    json_output : True → JSONRenderer, False → ConsoleRenderer
    extra_processors : kst_timestamper 뒤에 추가할 프로세서 목록
    """
    import structlog
    import logging as _logging

    renderer = (
        structlog.processors.JSONRenderer()
        if json_output
        else structlog.dev.ConsoleRenderer()
    )

    processors: list = [
        structlog.processors.add_log_level,
        kst_timestamper,
        *(extra_processors or []),
        renderer,
    ]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
