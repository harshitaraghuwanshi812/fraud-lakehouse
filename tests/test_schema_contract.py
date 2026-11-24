"""The Spark bronze schema must always match what the producer sends."""
import os
import re
from datetime import datetime, timezone

import sys

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.insert(0, os.path.join(ROOT, "producer"))

from generator import TransactionGenerator  # noqa: E402


def test_spark_schema_matches_producer_fields():
    now = datetime(2026, 11, 23, 8, 0, 0, tzinfo=timezone.utc)
    gen = TransactionGenerator(seed=1, fraud_rate=0.0)
    gen.arrive(now)
    event = gen.due(now)[0]

    with open(os.path.join(ROOT, "spark", "common.py"), encoding="utf-8") as f:
        schema_fields = re.findall(r'StructField\("(\w+)"', f.read())

    assert schema_fields == list(event)
