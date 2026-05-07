from pyspark.sql import SparkSession
from src.spark_session import create_spark_session
from src.config import settings
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
)


KAFKA_BOOTSTRAP_SERVERS = settings.KAFKA_BOOTSTRAP_SERVERS
KAFKA_TOPIC = settings.KAFKA_TOPIC

BRONZE_TABLE = "demo.bronze.inventory_events"
CHECKPOINT_LOCATION = "s3a://warehouse/checkpoints/bronze/inventory_events"


spark = create_spark_session("KafkaToIcebergBronzeInventory")


def main():
   

    spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.bronze")

    schema = StructType(
        [
            StructField("event_id", IntegerType(), True),
            StructField("movement_id", IntegerType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("warehouse_id", IntegerType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("event_type", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("status", StringType(), True),
        ]
    )

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_df = (
        raw_df
        .selectExpr(
            "CAST(key AS STRING) AS kafka_key",
            "CAST(value AS STRING) AS json_value",
            "topic",
            "partition",
            "offset",
            "timestamp AS kafka_timestamp",
        )
        .withColumn("data", from_json(col("json_value"), schema))
        .select(
            col("data.event_id"),
            col("data.movement_id"),
            col("data.product_id"),
            col("data.warehouse_id"),
            col("data.quantity"),
            col("data.event_type"),
            col("data.created_at"),
            col("data.status"),
            col("kafka_key"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_timestamp"),
            current_timestamp().alias("bronze_ingestion_timestamp"),
        )
    )

    query = (
        parsed_df.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .toTable(BRONZE_TABLE)
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()