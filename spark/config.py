KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "customer_events"

POSTGRES_URL = "jdbc:postgresql://postgres:5432/customer_db"
POSTGRES_USER = "user"
POSTGRES_PASSWORD = "password"
POSTGRES_DRIVER = "org.postgresql.Driver"

RAW_EVENTS_TABLE = "customer_events"
WINDOW_METRICS_TABLE = "event_type_window_metrics"

RAW_CHECKPOINT = "/app/checkpoints/raw_events"
WINDOW_CHECKPOINT = "/app/checkpoints/window_metrics"