from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

spark = SparkSession.builder \
    .appName("CustomerEventsStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("timestamp", DoubleType(), True),
])

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "customer_events") \
    .option("startingOffsets", "latest") \
    .load()

events_df = raw_df.selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), schema).alias("data")) \
    .select("data.*")

query = events_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()