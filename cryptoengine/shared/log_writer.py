"""
shared/log_writer.py — 비동기 로그 DB Writer

asyncio.Queue 기반 버퍼링으로 서비스 성능에 영향 없이 DB에 로그를 저장한다.
- 최대 큐 사이즈: 1000 (초과 시 가장 오래된 항목 드롭)
- 배치 INSERT: 50개 또는 5초 간격 중 먼저 도달하는 조건
- DB 연결 실패 시 로그 드롭 (stderr에 오류 출력)
- close() 호출 시 큐 잔여 항목 flush
"""

import asyncio
import json
import sys
import time
from datetime import datetime, timezone
from typing import Optional, Any

_log_writer: Optional["LogWriter"] = None


class LogWriter:
    """비동기 배치 로그 DB Writer."""

    MAX_QUEUE_SIZE = 1000
    BATCH_SIZE = 50
    FLUSH_INTERVAL = 5.0  # seconds

    INSERT_SQL = """
        INSERT INTO service_logs
            (timestamp, service, level, level_no, event, message, context, trace_id, error_type, error_stack)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
    """

    def __init__(self, service_name: str, db_pool):
        self.service_name = service_name
        self.db_pool = db_pool
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=self.MAX_QUEUE_SIZE)
        self._flush_task: Optional[asyncio.Task] = None
        self._closed = False

    async def start(self):
        """백그라운드 flush 태스크 시작."""
        self._flush_task = asyncio.create_task(self._flush_loop())

    async def write_log(
        self,
        level: str,
        level_no: int,
        event: str,
        message: Optional[str],
        context: Optional[dict],
        trace_id: Optional[str],
        error_type: Optional[str],
        error_stack: Optional[str],
    ):
        """로그를 큐에 넣는다. 큐가 가득 찼으면 가장 오래된 항목을 드롭."""
        if self._closed:
            return

        entry = (
            datetime.now(timezone.utc),
            self.service_name,
            level,
            level_no,
            event,
            message,
            json.dumps(context, default=str) if context else None,
            trace_id,
            error_type,
            error_stack,
        )

        if self._queue.full():
            try:
                self._queue.get_nowait()  # 가장 오래된 항목 드롭
                print(
                    f"[log_writer] WARNING: 큐 오버플로우, 오래된 로그 드롭 (service={self.service_name})",
                    file=sys.stderr,
                )
            except asyncio.QueueEmpty:
                pass

        try:
            self._queue.put_nowait(entry)
        except asyncio.QueueFull:
            pass  # 매우 드문 경우의 race condition 무시

    async def _flush_loop(self):
        """배치 조건(50개 또는 5초)을 만족할 때마다 DB에 INSERT."""
        batch = []
        last_flush = time.monotonic()

        while not self._closed or not self._queue.empty():
            try:
                timeout = self.FLUSH_INTERVAL - (time.monotonic() - last_flush)
                if timeout <= 0:
                    timeout = 0.01

                entry = await asyncio.wait_for(self._queue.get(), timeout=timeout)
                batch.append(entry)
                self._queue.task_done()

                elapsed = time.monotonic() - last_flush
                if len(batch) >= self.BATCH_SIZE or elapsed >= self.FLUSH_INTERVAL:
                    await self._flush_batch(batch)
                    batch = []
                    last_flush = time.monotonic()

            except asyncio.TimeoutError:
                if batch:
                    await self._flush_batch(batch)
                    batch = []
                last_flush = time.monotonic()
            except asyncio.CancelledError:
                if batch:
                    await self._flush_batch(batch)
                break
            except Exception as e:
                print(f"[log_writer] 예기치 않은 오류: {e}", file=sys.stderr)

        # 종료 시 잔여 배치 flush
        if batch:
            await self._flush_batch(batch)

    async def _flush_batch(self, batch: list):
        """배치 INSERT를 DB에 실행한다."""
        if not batch:
            return
        try:
            async with self.db_pool.acquire() as conn:
                await conn.executemany(self.INSERT_SQL, batch)
        except Exception as e:
            print(
                f"[log_writer] DB INSERT 실패 ({len(batch)}개 드롭): {e}",
                file=sys.stderr,
            )

    async def close(self):
        """남은 큐 항목을 flush하고 종료."""
        self._closed = True
        if self._flush_task and not self._flush_task.done():
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        # 잔여 항목 직접 flush
        remaining = []
        while not self._queue.empty():
            try:
                remaining.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        if remaining:
            await self._flush_batch(remaining)


async def init_log_writer(service_name: str, db_pool) -> "LogWriter":
    """싱글턴 LogWriter를 초기화하고 반환한다."""
    global _log_writer
    _log_writer = LogWriter(service_name, db_pool)
    await _log_writer.start()
    return _log_writer


def get_log_writer() -> Optional["LogWriter"]:
    """현재 LogWriter 인스턴스를 반환한다."""
    return _log_writer


async def close_log_writer():
    """LogWriter를 종료하고 큐를 flush한다."""
    global _log_writer
    if _log_writer:
        await _log_writer.close()
        _log_writer = None
