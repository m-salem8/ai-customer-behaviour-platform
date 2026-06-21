# =============================================================================
# Transformation module.
#
# This module contains all Spark transformation logic:
#
#   Bronze preparation:
#       Kafka binary value → raw JSON string
#
#   Silver preparation:
#       Raw JSON string → parsed, typed, structured event columns
#
#   Gold preparation:
#       Structured events → 5-minute windowed business metrics
#
# Keeping transformations in one module makes the code easier to test,
# reuse, and maintain.
# =============================================================================

from pyspark.sql.functions import col, from_json, from_unixtime, window
from pyspark.sql.functions import current_timestamp, lit, when
from config import VALID_EVENT_TYPES
from schemas import customer_event_schema


def parse_raw_events(raw_df):
    """
    Build the Silver event DataFrame.

    This function parses Kafka JSON messages into structured columns using
    the expected customer event schema.

    Input:
        raw_df:
            Kafka streaming DataFrame.
            The Kafka 'value' column is binary.

    Transformation:
        1. Cast Kafka value from binary to string.
        2. Parse JSON using customer_event_schema.
        3. Extract business columns.
        4. Rename timestamp to event_time.

    Output columns:
        user_id      STRING
        event_type   STRING
        product_id   STRING
        event_time   DOUBLE

    Note:
        event_time remains a numeric epoch timestamp here because this version
        is used for PostgreSQL storage and dashboard filtering.
    """

    return raw_df.selectExpr("CAST(value AS STRING) AS json_value") \
        .select(from_json(col("json_value"), customer_event_schema).alias("data")) \
        .select(
            col("data.user_id"),
            col("data.event_type"),
            col("data.product_id"),
            col("data.timestamp").alias("event_time")
        )


def parse_events_with_timestamp(raw_df):
    """
    Build the timestamp-based event DataFrame.

    This function is similar to parse_raw_events(), but converts the numeric
    epoch timestamp into a Spark timestamp type.

    Why?
        Spark window aggregations and watermarks require a timestamp column.

    Input:
        raw_df:
            Kafka streaming DataFrame.

    Transformation:
        1. Cast Kafka value from binary to string.
        2. Parse JSON using customer_event_schema.
        3. Extract event fields.
        4. Convert numeric epoch timestamp to Spark timestamp.

    Output columns:
        user_id          STRING
        event_type       STRING
        product_id       STRING
        event_time_ts    TIMESTAMP
    """

    return raw_df.selectExpr("CAST(value AS STRING) AS json_value") \
        .select(from_json(col("json_value"), customer_event_schema).alias("data")) \
        .select(
            col("data.user_id"),
            col("data.event_type"),
            col("data.product_id"),
            from_unixtime(col("data.timestamp")).cast("timestamp").alias("event_time_ts")
        )


def build_event_type_window_metrics(events_df):
    """
    Build the Gold windowed metrics DataFrame.

    This function aggregates events into 5-minute tumbling windows grouped
    by event_type.

    Input:
        events_df:
            DataFrame with:
                event_time_ts TIMESTAMP
                event_type STRING

    Transformation:
        1. Apply a 1-minute watermark for late-arriving events.
        2. Group records into 5-minute tumbling windows.
        3. Count events per event_type per window.
        4. Flatten the Spark window struct into window_start and window_end.

    Output columns:
        window_start    TIMESTAMP
        window_end      TIMESTAMP
        event_type      STRING
        event_count     LONG

    Example output:
        10:00 | 10:05 | product_view | 120
        10:00 | 10:05 | purchase     | 25
    """

    return events_df \
        .withWatermark("event_time_ts", "1 minute") \
        .groupBy(
            window(col("event_time_ts"), "5 minutes"),
            col("event_type")
        ) \
        .count() \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("event_type"),
            col("count").alias("event_count")
        )

def build_rejected_events(raw_df):
    parsed_df = raw_df.selectExpr("CAST(value AS STRING) AS raw_value") \
        .select(
            col("raw_value"),
            from_json(col("raw_value"), customer_event_schema).alias("data")
        )

    return parsed_df \
        .withColumn(
            "error_reason",
            when(col("data.user_id").isNull(), lit("missing_user_id"))
            .when(col("data.product_id").isNull(), lit("missing_product_id"))
            .when(col("data.timestamp").isNull(), lit("missing_timestamp"))
            .when(~col("data.event_type").isin(VALID_EVENT_TYPES), lit("invalid_event_type"))
        ) \
        .filter(col("error_reason").isNotNull()) \
        .select(
            col("raw_value"),
            col("error_reason"),
            current_timestamp().alias("rejected_at")
        )