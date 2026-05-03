CREATE TABLE IF NOT EXISTS customer_events (
    user_id TEXT,
    event_type TEXT,
    product_id TEXT,
    event_time DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS event_type_window_metrics (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    event_type TEXT,
    event_count BIGINT
);