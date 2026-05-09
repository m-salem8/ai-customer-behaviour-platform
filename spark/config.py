# =============================================================================
# Configuration module for the Spark streaming pipeline.
# All connection strings, topic names, table names, and checkpoint paths
# are centralised here so they can be maintained in one place.
# =============================================================================

# Kafka connection settings
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"   # Kafka broker address (Docker service name)
KAFKA_TOPIC = "customer_events"          # Topic the producer writes events to

# PostgreSQL connection settings
POSTGRES_URL = "jdbc:postgresql://postgres:5432/customer_db"
POSTGRES_USER = "user"
POSTGRES_PASSWORD = "password"
POSTGRES_DRIVER = "org.postgresql.Driver"   # JDBC driver class

# Target tables in PostgreSQL
RAW_EVENTS_TABLE = "customer_events"               # Stores every raw event
WINDOW_METRICS_TABLE = "event_type_window_metrics"  # Stores aggregated window counts

# Checkpoint directories – used by Spark Structured Streaming for fault-tolerance
RAW_CHECKPOINT = "/tmp/spark-checkpoints/raw_events"         # Checkpoint for raw events stream
WINDOW_CHECKPOINT = "/tmp/spark-checkpoints/window_metrics"  # Checkpoint for windowed aggregation stream
