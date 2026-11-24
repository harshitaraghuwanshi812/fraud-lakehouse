import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "producer"))

from generator import FRAUD_TYPES, TransactionGenerator, haversine_km  # noqa: E402

NOW = datetime(2026, 11, 23, 8, 0, 0, tzinfo=timezone.utc)
FIELDS = {
    "txn_id", "card_id", "customer_id", "merchant_id", "merchant_category",
    "amount", "currency", "lat", "lon", "device_id", "event_time",
    "is_fraud", "fraud_type",
}


def parse(ts):
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def drain(gen, now=None):
    """Return every pending event, regardless of due time."""
    return gen.due(now or NOW + timedelta(days=1))


def test_event_has_all_fields():
    gen = TransactionGenerator(seed=1, fraud_rate=0.0)
    gen.arrive(NOW)
    events = gen.due(NOW)
    assert len(events) == 1
    assert set(events[0]) == FIELDS
    assert events[0]["is_fraud"] is False
    assert events[0]["fraud_type"] is None


def test_same_seed_same_output():
    def run():
        gen = TransactionGenerator(seed=7, fraud_rate=0.1)
        out = []
        for i in range(200):
            t = NOW + timedelta(seconds=i)
            gen.arrive(t)
            out.extend(gen.due(t))
        return out

    assert run() == run()


def test_velocity_scenario():
    gen = TransactionGenerator(seed=3)
    gen.start_scenario("velocity", NOW)
    events = drain(gen)
    assert len(events) >= 6
    assert len({e["card_id"] for e in events}) == 1
    span = parse(events[-1]["event_time"]) - parse(events[0]["event_time"])
    assert span <= timedelta(minutes=10)
    assert all(e["fraud_type"] == "velocity" for e in events)


def test_amount_anomaly_scenario():
    gen = TransactionGenerator(seed=4)
    cust = gen.customers[0]
    gen.start_scenario("amount_anomaly", NOW, cust=cust)
    (event,) = drain(gen)
    assert event["amount"] >= cust["mean_amount"] * 15
    assert event["fraud_type"] == "amount_anomaly"


def test_impossible_travel_scenario():
    gen = TransactionGenerator(seed=5)
    for cust in gen.customers[:25]:
        gen.start_scenario("impossible_travel", NOW, cust=cust)
        first, second = drain(gen)
        assert first["card_id"] == second["card_id"]
        assert first["is_fraud"] is False
        assert second["fraud_type"] == "impossible_travel"
        km = haversine_km(first["lat"], first["lon"], second["lat"], second["lon"])
        hours = (parse(second["event_time"]) - parse(first["event_time"])).total_seconds() / 3600
        assert km / hours > 900  # faster than a plane


def test_new_device_scenario():
    gen = TransactionGenerator(seed=6)
    cust = gen.customers[0]
    gen.start_scenario("new_device_high_amount", NOW, cust=cust)
    (event,) = drain(gen)
    assert event["device_id"] not in cust["devices"]
    assert event["amount"] >= cust["mean_amount"] * 8


def test_fraud_rate_roughly_matches():
    gen = TransactionGenerator(seed=9, fraud_rate=0.05)
    fraud_events = 0
    total_arrivals = 4000
    for i in range(total_arrivals):
        gen.arrive(NOW + timedelta(seconds=i))
    events = drain(gen)
    fraud_events = sum(1 for e in events if e["is_fraud"])
    # fraud scenarios may emit several events; just check it is present and not dominant
    assert 0 < fraud_events < len(events) * 0.4


def test_due_returns_in_time_order():
    gen = TransactionGenerator(seed=8)
    gen.start_scenario("velocity", NOW)
    times = [e["event_time"] for e in drain(gen)]
    assert times == sorted(times)


def test_all_scenarios_are_known():
    gen = TransactionGenerator(seed=2)
    for name in FRAUD_TYPES:
        gen.start_scenario(name, NOW)
    assert len(drain(gen)) >= 4
