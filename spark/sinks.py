# =============================================================================
# Sink / writer module.
#
# Defines the foreachBatch functions that write streaming micro-batches into
# PostgreSQL via JDBC.  By using foreachBatch we gain full control over the
# write mode (append) and can add transaction logic in the future if needed.
# =============================================================================

from config import (
    POSTGRES_URL,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    POSTGRES_DRIVER,
    RAW_EVENTS_TABLE,
    WINDOW_METRICS_TABLE,
)


def write_raw_events_to_postgres(batch_df, batch_id):
    """
    Sink function for the raw event stream.
    Appends every incoming micro-batch to the `customer_events` table
    in PostgreSQL.
    """
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
    """
    Sink function for the windowed-aggregation stream.
    Appends the aggregated (window_start, window_end, event_type, event_count)
    rows to the `event_type_window_metrics` table in PostgreSQL.

    Uses 'update' output mode upstream, so only new/changed windows are
    written.  'append' mode here ensures we never accidentally overwrite
    previous windows.
    """
    batch_df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", WINDOW_METRICS_TABLE) \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", POSTGRES_DRIVER) \
        .mode("append") \
        .save()