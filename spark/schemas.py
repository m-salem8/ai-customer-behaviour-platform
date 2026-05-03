from pyspark.sql.types import StructType, StructField, StringType, DoubleType

customer_event_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("timestamp", DoubleType(), True),
])