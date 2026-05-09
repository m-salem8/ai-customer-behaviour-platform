# =============================================================================
# Schema definitions for incoming Kafka events.
#
# By defining the schema explicitly (rather than relying on inferSchema),
# we ensure type safety and avoid schema drift issues during streaming.
# =============================================================================

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Schema for a single customer event produced by the Python Kafka producer.
# Fields match the JSON keys sent by producer.py.
customer_event_schema = StructType([
    StructField("user_id",    StringType(), True),   # Unique identifier for the customer
    StructField("event_type", StringType(), True),   # e.g. "product_view", "add_to_cart", "purchase"
    StructField("product_id", StringType(), True),   # Identifier of the product involved
    StructField("timestamp",  DoubleType(), True),   # Unix epoch timestamp (seconds)
])