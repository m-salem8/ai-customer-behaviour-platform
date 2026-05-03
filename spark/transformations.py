from pyspark.sql.functions import col, from_json, from_unixtime, window
from schemas import customer_event_schema

def parse_raw_events(raw_df):
    return raw_df.selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), customer_event_schema).alias("data")) \
        .select(
            col("data.user_id"),
            col("data.event_type"),
            col("data.product_id"),
            col("data.timestamp").alias("event_time")
        )

def parse_events_with_timestamp(raw_df):
    return raw_df.selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), customer_event_schema).alias("data")) \
        .select(
            col("data.user_id"),
            col("data.event_type"),
            col("data.product_id"),
            from_unixtime(col("data.timestamp")).cast("timestamp").alias("event_time_ts")
        )

def build_event_type_window_metrics(events_df):
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