"""Send synthetic card transactions to Kafka (keyed by card_id).

Examples:
  python3 producer/producer.py --rate 50 --duration 60
  python3 producer/producer.py --dry-run --max-events 10   # no Kafka needed
"""
import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from generator import TransactionGenerator, utc_now  # noqa: E402


def parse_args():
    p = argparse.ArgumentParser(description="Transaction producer")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="transactions")
    p.add_argument("--rate", type=float, default=50, help="new events per second")
    p.add_argument("--duration", type=float, default=0, help="seconds to run (0 = no limit)")
    p.add_argument("--max-events", type=int, default=0, help="stop after N events (0 = no limit)")
    p.add_argument("--fraud-rate", type=float, default=0.02)
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--dry-run", action="store_true", help="print JSON lines, do not use Kafka")
    return p.parse_args()


def make_sender(args):
    """Return (send, flush). send(key, value_dict)."""
    if args.dry_run:
        def send(key, value):
            print(json.dumps(value))
        return send, (lambda: None)

    try:
        from confluent_kafka import Producer
    except ImportError:
        sys.exit("confluent-kafka is not installed. Run: pip install -r requirements.txt")

    producer = Producer({"bootstrap.servers": args.bootstrap, "linger.ms": 5})
    errors = {"count": 0}

    def on_delivery(err, msg):
        if err is not None:
            errors["count"] += 1
            print(f"delivery failed: {err}", file=sys.stderr)

    def send(key, value):
        producer.produce(args.topic, key=key, value=json.dumps(value), on_delivery=on_delivery)
        producer.poll(0)

    def flush():
        producer.flush(10)
        if errors["count"]:
            print(f"{errors['count']} messages failed", file=sys.stderr)

    return send, flush


def main():
    args = parse_args()
    gen = TransactionGenerator(seed=args.seed, fraud_rate=args.fraud_rate)
    send, flush = make_sender(args)

    interval = 1.0 / args.rate
    start = time.monotonic()
    next_tick = start
    sent = 0
    fraud = 0
    log = sys.stderr

    try:
        while True:
            now_m = time.monotonic()
            if args.duration and now_m - start >= args.duration:
                break
            if args.max_events and sent >= args.max_events:
                break
            if now_m < next_tick:
                time.sleep(min(next_tick - now_m, 0.01))
                continue
            next_tick += interval

            now = utc_now()
            gen.arrive(now)
            for event in gen.due(now):
                send(event["card_id"], event)
                sent += 1
                fraud += 1 if event["is_fraud"] else 0
            if sent and sent % 1000 == 0:
                print(f"sent={sent} fraud={fraud}", file=log)
    except KeyboardInterrupt:
        pass
    finally:
        flush()
        print(f"done: sent={sent} fraud={fraud}", file=log)


if __name__ == "__main__":
    main()
