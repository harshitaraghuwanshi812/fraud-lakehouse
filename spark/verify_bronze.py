"""Check the bronze table: no duplicates, and nothing missing vs Kafka.

Run:
  docker compose run --rm spark verify_bronze.py
"""
import argparse

from pyspark.sql import functions as F

from common import BRONZE_PATH, build_spark


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="kafka:19092")
    p.add_argument("--topic", default="transactions")
    return p.parse_args()


def main():
    args = parse_args()
    spark = build_spark("verify_bronze")

    bronze = spark.read.format("delta").load(BRONZE_PATH)
    total = bronze.count()
    distinct_offsets = bronze.select("kafka_partition", "kafka_offset").distinct().count()
    distinct_txn = bronze.select("txn_id").distinct().count()
    bad_json = bronze.filter(F.col("txn_id").isNull()).count()

    kafka_total = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .count()
    )

    dup_rows = total - distinct_offsets
    missing = kafka_total - distinct_offsets

    print("-" * 44)
    print(f"messages in Kafka        : {kafka_total}")
    print(f"rows in bronze           : {total}")
    print(f"distinct kafka offsets   : {distinct_offsets}")
    print(f"distinct txn_id          : {distinct_txn}")
    print(f"unparseable records      : {bad_json}")
    print(f"DUPLICATE ROWS           : {dup_rows}")
    print(f"MISSING ROWS             : {missing}")
    print("-" * 44)
    if dup_rows == 0 and missing == 0:
        print("RESULT: PASS (zero duplicates, nothing missing)")
    elif dup_rows == 0 and missing > 0:
        print("RESULT: NOT CAUGHT UP YET (no duplicates). Run the stream a bit longer, then verify again.")
    else:
        print("RESULT: FAIL (duplicates found)")


if __name__ == "__main__":
    main()
