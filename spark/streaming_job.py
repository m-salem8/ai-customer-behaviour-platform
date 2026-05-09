# =============================================================================
# Main entry-point for the Spark Structured Streaming job.
#
# This script is launched when the Spark container starts.  It:
#   1. Reads a continuous stream of customer events from Kafka.
#   2. Parses the raw JSON into structured columns.
#   3. Computes tumbling-window aggregations (5 min) grouped by event type.
#   4. Writes both raw events and window metrics to PostgreSQL via foreachBatch.
#
# The two streaming queries run concurrently and indefinitely.
# =============================================================================

from pyspark.sql import SparkSession

from config import RAW_CHECKPOINT, WINDOW_CHECKPOINT
from readers import read_kafka_stream
from transformations import (
    parse_raw_events,
    parse_events_with_timestamp,
    build_event_type_window_metrics,
)
from sinks import (
    write_raw_events_to_postgres,
    write_window_metrics_to_postgres,
)

# ---------------------------------------------------------------------------
# 1. Initialise Spark session
# ---------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("CustomerEventsStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")          # reduce noise in logs

# ---------------------------------------------------------------------------
# 2. Read streaming data from Kafka
# ---------------------------------------------------------------------------
raw_df = read_kafka_stream(spark)

# ---------------------------------------------------------------------------
# 3. Parse events (two variants for different downstream uses)
# ---------------------------------------------------------------------------
raw_events_df = parse_raw_events(raw_df)            # keep numeric timestamp

events_with_timestamp_df = parse_events_with_timestamp(raw_df)  # proper timestamp for windows

# ---------------------------------------------------------------------------
# 4. Build windowed aggregations
# ---------------------------------------------------------------------------
window_metrics_df = build_event_type_window_metrics(events_with_timestamp_df)

# ---------------------------------------------------------------------------
# 5. Define the two streaming queries
# ---------------------------------------------------------------------------

# Query 1: persist every raw event to PostgreSQL
raw_query = raw_events_df.writeStream \
    .foreachBatch(write_raw_events_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", RAW_CHECKPOINT) \
    .start()

# Query 2: persist windowed aggregation results to PostgreSQL
#   outputMode = "update"  → only emit rows that changed (new windows)
#   trigger    = 10 sec    → micro-batch interval (check for new data every 10 s)
metrics_query = window_metrics_df.writeStream \
    .foreachBatch(write_window_metrics_to_postgres) \
    .outputMode("update") \
    .option("checkpointLocation", WINDOW_CHECKPOINT) \
    .trigger(processingTime="10 seconds") \
    .start()

# ---------------------------------------------------------------------------
# 6. Keep the application running
# ---------------------------------------------------------------------------
raw_query.awaitTermination()
metrics_query.awaitTermination()