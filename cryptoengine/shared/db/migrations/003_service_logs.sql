-- 003_service_logs.sql — 서비스 로그 영구 저장 테이블

CREATE TABLE IF NOT EXISTS service_logs (
    id          BIGSERIAL PRIMARY KEY,
    timestamp   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    service     VARCHAR(50) NOT NULL,
    level       VARCHAR(10) NOT NULL,
    level_no    SMALLINT NOT NULL,
    event       VARCHAR(500) NOT NULL,
    message     TEXT,
    context     JSONB,
    trace_id    VARCHAR(36),
    error_type  VARCHAR(200),
    error_stack TEXT
);

CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON service_logs (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_logs_service_level ON service_logs (service, level_no, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_logs_event ON service_logs (event, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_logs_trace ON service_logs (trace_id) WHERE trace_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_logs_errors ON service_logs (service, timestamp DESC) WHERE level_no >= 40;
