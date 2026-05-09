# =============================================================================
# Transformation module.
#
# Contains all the logic for parsing raw Kafka JSON payloads and building
# derived aggregations (windowed metrics).  Keeping transformations separate
# makes them testable and reusable across both streaming and batch contexts.
# =============================================================================

from pyspark.sql.functions import col, from_json, from_unixtime, window
from schemas import customer_event_schema


def parse_raw_events(raw_df):
    """
    Parse the raw Kafka 'value' binary column into a structured DataFrame
    with meaningful column names.

    The resulting DataFrame keeps the original `timestamp` as a numeric field
    (double) and renames it to `event_time`.

    Parameters
    ----------
    raw_df : DataFrame
        Streaming DataFrame from Kafka reader (contains a 'value' binary column).

    Returns
    -------
    DataFrame with columns: user_id, event_type, product_id, event_time
    """
    return raw_df.selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), customer_event_schema).alias("data")) \
        .select(
            col("data.user_id"),
            col("data.event_type"),
            col("data.product_id"),
            col("data.timestamp").alias("event_time")      # keep as raw double (epoch)
        )


def parse_events_with_timestamp(raw_df):
    """
    Similar to parse_raw_events, but converts the numeric timestamp into a
    proper Spark timestamp type.  This is required for watermark-based window
    aggregations (tumbling/sliding windows).

    Parameters
    ----------
    raw_df : DataFrame
        Streaming DataFrame from Kafka reader.

    Returns
    -------
    DataFrame with columns: user_id, event_type, product_id, event_time_ts
    """
    return raw_df.selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), customer_event_schema).alias("data")) \
        .select(
            col("data.user_id"),
            col("data.event_type"),
            col("data.product_id"),
            from_unixtime(col("data.timestamp")).cast("timestamp").alias("event_time_ts")
        )


def build_event_type_window_metrics(events_df):
    """
    Aggregate events by 5-minute tumbling windows, grouped by event_type.

    Uses a 1-minute watermark to handle late-arriving data (events delayed
    by up to 60 seconds are still included in the correct window).

    The output table has one row per (window_start, window_end, event_type)
    with a count of how many events of that type occurred in the window.

    Parameters
    ----------
    events_df : DataFrame
        DataFrame with event_time_ts (timestamp) and event_type columns.

    Returns
    -------
    DataFrame with columns: window_start, window_end, event_type, event_count
    """
    return events_df \
        .withWatermark("event_time_ts", "1 minute") \
        .groupBy(
            window(col("event_time_ts"), "5 minutes"),   # tumbling window every 5 min
            col("event_type")
        ) \
        .count() \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("event_type"),
            col("count").alias("event_count")
        )