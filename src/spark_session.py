from pyspark.sql import SparkSession
from src.config import settings


def create_spark_session(app_name: str) -> SparkSession:
    catalog = settings.ICEBERG_CATALOG

    return (
        SparkSession.builder
        .appName(app_name)

        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )

        # Iceberg REST catalog pointing to MinIO
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "rest")
        .config(f"spark.sql.catalog.{catalog}.uri", settings.ICEBERG_REST_URI)
        .config(f"spark.sql.catalog.{catalog}.warehouse", settings.ICEBERG_WAREHOUSE)
        .config(f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

        .config(f"spark.sql.catalog.{catalog}.s3.endpoint", settings.MINIO_ENDPOINT)
        .config(f"spark.sql.catalog.{catalog}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{catalog}.s3.access-key-id", settings.MINIO_ACCESS_KEY)
        .config(f"spark.sql.catalog.{catalog}.s3.secret-access-key", settings.MINIO_SECRET_KEY)

        # Hadoop FileSystem API (s3a://) — used by Spark for general reads/writes
        .config("spark.hadoop.fs.s3a.endpoint", settings.MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", settings.MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", settings.MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        # Hadoop FileContext API — a separate interface also used by Hadoop internally.
        # Required by Spark Structured Streaming's checkpoint manager and by the
        # Iceberg SparkMicroBatchStream when resolving offset files on s3a://.
        # Without this, streaming jobs fail with:
        #   "ClassNotFoundException: Class org.apache.hadoop.fs.s3a.S3A not found"
        .config(
            "spark.hadoop.fs.AbstractFileSystem.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3A",
        )

        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )

        .getOrCreate()
    )