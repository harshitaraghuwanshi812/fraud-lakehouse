"""Bronze layer: Kafka topic -> Delta table, with checkpointing.

Bronze keeps the raw message as it arrived (raw_value) plus Kafka position
(partition + offset), and a parsed copy of the fields. Nothing is dropped,
so bad records can be inspected later instead of disappearing.

Exactly-once: the checkpoint remembers which Kafka offsets are done, and the
Delta sink commits each micro-batch transactionally. If the job is killed and
restarted, it continues from the checkpoint and does not write a batch twice.

Run:
  docker compose run --rm spark bronze_stream.py
"""
import argparse

from pyspark.sql import functions as F

from common import BRONZE_CHECKPOINT, BRONZE_PATH, TXN_SCHEMA, build_spark


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="kafka:19092")
    p.add_argument("--topic", default="transactions")
    p.add_argument("--trigger-seconds", type=int, default=5)
    p.add_argument("--available-now", action="store_true",
                   help="process everything currently in Kafka, then stop")
    return p.parse_args()


def main():
    args = parse_args()
    spark = build_spark("bronze_stream")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    text = F.col("value").cast("string")
    bronze = (
        raw.select(
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            text.alias("raw_value"),
            F.from_json(text, TXN_SCHEMA).alias("p"),
        )
        .select("kafka_topic", "kafka_partition", "kafka_offset",
                "kafka_timestamp", "raw_value", "p.*")
        .withColumn("event_ts", F.to_timestamp("event_time"))
        .withColumn("ingest_ts", F.current_timestamp())
    )

    writer = (
        bronze.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", BRONZE_CHECKPOINT)
    )
    if args.available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=f"{args.trigger_seconds} seconds")

    query = writer.start(BRONZE_PATH)
    print(f"bronze stream started -> {BRONZE_PATH}", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
