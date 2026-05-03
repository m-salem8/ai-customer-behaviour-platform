from config import (
    POSTGRES_URL,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    POSTGRES_DRIVER,
    RAW_EVENTS_TABLE,
    WINDOW_METRICS_TABLE,
)

def write_raw_events_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", RAW_EVENTS_TABLE) \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", POSTGRES_DRIVER) \
        .mode("append") \
        .save()

def write_window_metrics_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", WINDOW_METRICS_TABLE) \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", POSTGRES_DRIVER) \
        .mode("append") \
        .save()