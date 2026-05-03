CREATE TABLE customer_events (
    id SERIAL PRIMARY KEY,
    user_id TEXT,
    product_id TEXT,
    event_type TEXT,
    event_time DOUBLE PRECISION
);