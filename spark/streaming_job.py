# =============================================================================
# Main entry-point for the Spark Structured Streaming pipeline.
#
# This script is launched when the Spark container starts. It:
#
#   1. Reads a continuous stream of customer events from Kafka.
#   2. Parses raw JSON events into structured Spark DataFrames.
#   3. Computes tumbling-window aggregations (5 min) grouped by event type.
#   4. Writes streaming data into a Lakehouse-style architecture:
#
#        Bronze Layer → raw Kafka JSON events stored as Parquet
#        Silver Layer → cleaned structured events stored as Parquet
#
#   5. Persists serving-layer tables into PostgreSQL for dashboard analytics:
#
#        customer_events
#        event_type_window_metrics
#
# Note:
#   Gold metrics are currently persisted to PostgreSQL only.
#   Gold Parquet output is intentionally disabled for now because Parquet does
#   not support Spark Streaming "update" output mode directly for aggregations.
#
# The streaming queries run concurrently and continuously using Spark
# Structured Streaming with checkpointing for fault tolerance and recovery.
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from config import REJECTED_CHECKPOINT
from transformations import build_rejected_events
from sinks import write_rejected_events_to_postgres


from config import (
    RAW_CHECKPOINT,
    WINDOW_CHECKPOINT,
    BRONZE_CHECKPOINT,
    SILVER_CHECKPOINT,
)
from readers import read_kafka_stream
from transformations import (
    parse_raw_events,
    parse_events_with_timestamp,
    build_event_type_window_metrics,
)
from sinks import (
    write_raw_events_to_postgres,
    write_window_metrics_to_postgres,
    write_bronze_to_parquet,
    write_silver_to_parquet,
)

# ---------------------------------------------------------------------------
# 1. Initialise Spark session
# ---------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("CustomerEventsStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# 2. Read streaming data from Kafka
# ---------------------------------------------------------------------------
raw_df = read_kafka_stream(spark)

# ---------------------------------------------------------------------------
# 3. Parse events
# ---------------------------------------------------------------------------

# Silver-ready event format with numeric timestamp.
# This is used for PostgreSQL raw event storage and Silver Parquet output.
raw_events_df = parse_raw_events(raw_df)

# Timestamp-cast event format.
# This is required for Spark window aggregations.
events_with_timestamp_df = parse_events_with_timestamp(raw_df)

# ---------------------------------------------------------------------------
# 4. Build windowed aggregations
# ---------------------------------------------------------------------------
window_metrics_df = build_event_type_window_metrics(events_with_timestamp_df)
rejected_events_df = build_rejected_events(raw_df)
rejected_Query = rejected_events_df.writeStream \
    .foreachBatch(write_rejected_events_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", REJECTED_CHECKPOINT) \
    .start()
# ---------------------------------------------------------------------------
# 5. Define streaming queries
# ---------------------------------------------------------------------------

# Query 1: Persist cleaned raw events to PostgreSQL.
raw_query = raw_events_df.writeStream \
    .foreachBatch(write_raw_events_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", RAW_CHECKPOINT) \
    .start()

# Query 2: Persist windowed Gold metrics to PostgreSQL.
metrics_query = window_metrics_df.writeStream \
    .foreachBatch(write_window_metrics_to_postgres) \
    .outputMode("update") \
    .option("checkpointLocation", WINDOW_CHECKPOINT) \
    .trigger(processingTime="10 seconds") \
    .start()

# Query 3: Bronze layer - raw Kafka JSON as Parquet.
bronze_df = raw_df.select(
    col("topic").alias("kafka_topic"),
    col("partition").alias("kafka_partition"),
    col("offset").alias("kafka_offset"),
    col("timestamp").alias("kafka_timestamp"),
    current_timestamp().alias("ingestion_time"),
    col("value").cast("string").alias("raw_value")
)

bronze_query = write_bronze_to_parquet(bronze_df) \
    .outputMode("append") \
    .option("checkpointLocation", BRONZE_CHECKPOINT) \
    .start()

# Query 4: Silver layer - cleaned structured events as Parquet.
silver_query = write_silver_to_parquet(raw_events_df) \
    .outputMode("append") \
    .option("checkpointLocation", SILVER_CHECKPOINT) \
    .start()

# ---------------------------------------------------------------------------
# 6. Keep the application running
# ---------------------------------------------------------------------------
spark.streams.awaitAnyTermination()