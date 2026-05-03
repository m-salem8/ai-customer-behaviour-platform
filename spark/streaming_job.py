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

spark = SparkSession.builder \
    .appName("CustomerEventsStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

raw_df = read_kafka_stream(spark)

raw_events_df = parse_raw_events(raw_df)

events_with_timestamp_df = parse_events_with_timestamp(raw_df)

window_metrics_df = build_event_type_window_metrics(events_with_timestamp_df)

raw_query = raw_events_df.writeStream \
    .foreachBatch(write_raw_events_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", RAW_CHECKPOINT) \
    .start()

metrics_query = window_metrics_df.writeStream \
    .foreachBatch(write_window_metrics_to_postgres) \
    .outputMode("update") \
    .option("checkpointLocation", WINDOW_CHECKPOINT) \
    .trigger(processingTime="10 seconds") \
    .start()

raw_query.awaitTermination()
metrics_query.awaitTermination()