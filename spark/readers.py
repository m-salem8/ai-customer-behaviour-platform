# =============================================================================
# Data reader module.
#
# Centralises the logic for reading streaming data from Kafka into a Spark
# DataFrame.  The consumer options (bootstrap servers, topic) are pulled from
# the shared config module so there is no duplication.
# =============================================================================

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC


def read_kafka_stream(spark):
    """
    Create a streaming DataFrame that continuously reads from the configured
    Kafka topic.

    The DataFrame has the following default columns from the Kafka source:
      - key (binary)
      - value (binary)       <-- the raw JSON bytes we need to parse
      - topic (string)
      - partition (int)
      - offset (long)
      - timestamp (timestamp)
      - timestampType (int)

    Returns
    -------
    DataFrame (streaming)
    """
    return spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()