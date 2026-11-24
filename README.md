# Real-Time Fraud Detection Lakehouse

Flags suspicious card transactions within seconds, for a fraud-ops team.

Pipeline: Kafka -> Spark Structured Streaming -> Delta Lake (bronze/silver/gold) -> dbt -> dashboard.

**Status: Week 1 done (producer + Kafka).** Next: Week 2 (bronze stream).

## What is in Week 1

- `docker-compose.yml`: single-node Kafka (KRaft mode, no Zookeeper)
- `producer/generator.py`: synthetic transactions with 4 fraud scenarios
- `producer/producer.py`: sends events to Kafka, keyed by `card_id`
- `tests/`: pytest checks for every scenario

## Fraud scenarios (labelled in `fraud_type`)

| fraud_type | What it looks like |
|---|---|
| velocity | 6-10 txns on one card within a few minutes |
| amount_anomaly | one txn 15-40x the customer's normal amount |
| impossible_travel | txn in a city 1000+ km away, 1-5 minutes after one at home |
| new_device_high_amount | unseen device + amount 8-20x normal |

## Event schema

`txn_id, card_id, customer_id, merchant_id, merchant_category, amount, currency, lat, lon, device_id, event_time, is_fraud, fraud_type`

`event_time` is UTC ISO-8601. `is_fraud` and `fraud_type` are ground-truth labels for measuring precision/recall later.

## Run it

1. Install Docker and Python 3.10+.
2. `pip install -r requirements.txt`
3. `make test` (no Kafka needed)
4. `make dry` (prints 10 events, no Kafka needed)
5. `make up` (starts Kafka and creates topic `transactions` with 3 partitions)
6. `make seed` (sends 50 events/sec for 60 seconds)
7. Check messages arrived:
   `docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transactions --from-beginning --max-messages 5`
8. `make down` to stop and clean up.

## Producer options

`--rate` (events/sec), `--duration` (seconds), `--max-events`, `--fraud-rate` (default 0.02), `--seed`, `--bootstrap`, `--topic`, `--dry-run`.
