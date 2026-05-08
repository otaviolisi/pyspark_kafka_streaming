import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, row_number
from pyspark.sql.window import Window

from src.spark_session import create_spark_session
from src.config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BRONZE_TABLE     = "demo.bronze.inventory_events"
SILVER_TABLE     = "demo.silver.inventory_events"
QUARANTINE_TABLE = "demo.silver.inventory_quarantine"

CHECKPOINT_SILVER = "s3a://warehouse/checkpoints/silver/inventory_events"

TRIGGER_INTERVAL = "1 minute"

# Fields that must not be null — rows missing any of these go to quarantine
REQUIRED_FIELDS = ["product_id", "warehouse_id", "quantity"]


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

spark: SparkSession = create_spark_session("BronzeToSilverInventory")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_rejection_reason():
    """Returns a column expression describing which required fields are null."""
    from pyspark.sql.functions import expr
    parts = [f"CASE WHEN {f} IS NULL THEN '{f}' ELSE NULL END" for f in REQUIRED_FIELDS]
    null_fields_expr = "concat_ws(', ', " + ", ".join(parts) + ")"
    return expr(f"concat('Null fields: ', {null_fields_expr})")


# ---------------------------------------------------------------------------
# Batch processing
# ---------------------------------------------------------------------------

def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    """
    Called by Spark for every micro-batch (1-minute trigger).

    Steps:
      1. Deduplication by event_id (keep the row with the latest kafka_timestamp)
      2. Split into valid and invalid rows (null check on required fields)
      3. Upsert valid rows into Silver: DELETE existing event_ids then INSERT
      4. Append invalid rows to the quarantine table
    """

    if batch_df.isEmpty():
        logger.info(f"Batch {batch_id}: empty, nothing to process.")
        return

    logger.info(f"Batch {batch_id}: {batch_df.count()} rows received from Bronze.")

    # ------------------------------------------------------------------
    # 1. Deduplication
    # ------------------------------------------------------------------
    window_dedup = Window.partitionBy("event_id").orderBy(col("kafka_timestamp").desc())

    deduped_df = (
        batch_df
        .withColumn("_rn", row_number().over(window_dedup))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    removed = batch_df.count() - deduped_df.count()
    if removed > 0:
        logger.info(f"Batch {batch_id}: {removed} duplicates removed.")

    # ------------------------------------------------------------------
    # 2. Validation
    # ------------------------------------------------------------------
    null_condition = " OR ".join([f"{f} IS NULL" for f in REQUIRED_FIELDS])

    invalid_df = (
        deduped_df
        .filter(null_condition)
        .withColumn("rejection_reason", _build_rejection_reason())
        .withColumn("rejected_at", current_timestamp())
        .withColumn("source_batch_id", lit(batch_id))
    )

    valid_df = (
        deduped_df
        .filter(f"NOT ({null_condition})")
        .withColumn("silver_ingestion_timestamp", current_timestamp())
    )

    invalid_count = invalid_df.count()
    valid_count   = valid_df.count()
    logger.info(f"Batch {batch_id}: {valid_count} valid | {invalid_count} invalid.")

    # ------------------------------------------------------------------
    # 3. Upsert into Silver: DELETE + INSERT
    #
    #    Iceberg SQL MERGE cannot use Spark temp views as the USING source
    #    when multiple catalogs are active — the Iceberg planner resolves
    #    names against its own catalog and the view is invisible.
    #    Solution: collect event_ids to the driver, run a DELETE by id list,
    #    then append the fresh rows. This is idempotent: reprocessing the
    #    same batch simply re-deletes and re-inserts the same rows.
    # ------------------------------------------------------------------
    if valid_count > 0:
        event_ids = [row.event_id for row in valid_df.select("event_id").collect()]
        ids_str   = ", ".join(str(i) for i in event_ids)

        spark.sql(f"DELETE FROM {SILVER_TABLE} WHERE event_id IN ({ids_str})")

        valid_df.writeTo(SILVER_TABLE).append()

        logger.info(f"Batch {batch_id}: upsert completed on Silver ({valid_count} rows).")

    # ------------------------------------------------------------------
    # 4. Quarantine
    # ------------------------------------------------------------------
    if invalid_count > 0:
        invalid_df.writeTo(QUARANTINE_TABLE).append()
        logger.info(f"Batch {batch_id}: {invalid_count} rows sent to quarantine.")


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

def setup_namespaces() -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.silver")
    logger.info("Namespace demo.silver ready.")


def create_tables_if_not_exist() -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
            event_id                   INT,
            movement_id                INT,
            product_id                 INT,
            warehouse_id               INT,
            quantity                   INT,
            event_type                 STRING,
            created_at                 TIMESTAMP,
            status                     STRING,
            kafka_key                  STRING,
            topic                      STRING,
            partition                  INT,
            offset                     BIGINT,
            kafka_timestamp            TIMESTAMP,
            bronze_ingestion_timestamp TIMESTAMP,
            silver_ingestion_timestamp TIMESTAMP
        )
        USING iceberg
    """)
    logger.info(f"Table {SILVER_TABLE} ready.")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {QUARANTINE_TABLE} (
            event_id                   INT,
            movement_id                INT,
            product_id                 INT,
            warehouse_id               INT,
            quantity                   INT,
            event_type                 STRING,
            created_at                 TIMESTAMP,
            status                     STRING,
            kafka_key                  STRING,
            topic                      STRING,
            partition                  INT,
            offset                     BIGINT,
            kafka_timestamp            TIMESTAMP,
            bronze_ingestion_timestamp TIMESTAMP,
            rejection_reason           STRING,
            rejected_at                TIMESTAMP,
            source_batch_id            BIGINT
        )
        USING iceberg
    """)
    logger.info(f"Table {QUARANTINE_TABLE} ready.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    setup_namespaces()
    create_tables_if_not_exist()

    bronze_stream = (
        spark.readStream
        .format("iceberg")
        .option("stream-from-timestamp", "0")
        .load(BRONZE_TABLE)
    )

    query = (
        bronze_stream
        .writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_SILVER)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    logger.info(f"Silver stream started. Trigger: {TRIGGER_INTERVAL}.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
