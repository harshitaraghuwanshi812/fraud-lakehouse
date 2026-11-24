"""Shared settings for the Spark jobs."""
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType, DoubleType, StringType, StructField, StructType,
)

BRONZE_PATH = "/data/bronze/transactions"
BRONZE_CHECKPOINT = "/data/checkpoints/bronze_transactions"

# Must match the event schema in producer/generator.py
TXN_SCHEMA = StructType([
    StructField("txn_id", StringType()),
    StructField("card_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("merchant_id", StringType()),
    StructField("merchant_category", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("device_id", StringType()),
    StructField("event_time", StringType()),
    StructField("is_fraud", BooleanType()),
    StructField("fraud_type", StringType()),
])


def build_spark(app_name):
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
