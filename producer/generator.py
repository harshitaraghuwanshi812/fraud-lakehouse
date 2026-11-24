"""Synthetic card-transaction generator with 4 fraud scenarios.

Scenarios (fraud_type):
  velocity               - many txns on one card within a few minutes
  amount_anomaly         - one txn far above the customer's normal amount
  impossible_travel      - txn in a far city minutes after one at home
  new_device_high_amount - unseen device + high amount

Usage: call arrive(now) to create new activity, then due(now) to get
every event whose event_time is <= now.
"""
import heapq
import itertools
import math
import random
from datetime import datetime, timedelta, timezone

CITIES = {
    "Mumbai": (19.0760, 72.8777),
    "Delhi": (28.6139, 77.2090),
    "Bengaluru": (12.9716, 77.5946),
    "Chennai": (13.0827, 80.2707),
    "Kolkata": (22.5726, 88.3639),
    "Hyderabad": (17.3850, 78.4867),
    "Pune": (18.5204, 73.8567),
    "Bhopal": (23.2599, 77.4126),
}

CATEGORIES = ["grocery", "fuel", "electronics", "travel", "dining", "fashion", "pharmacy", "utilities"]

FRAUD_TYPES = ["velocity", "amount_anomaly", "impossible_travel", "new_device_high_amount"]


def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = p2 - p1
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def iso(dt):
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class TransactionGenerator:
    def __init__(self, seed=42, n_customers=500, fraud_rate=0.02):
        self.rng = random.Random(seed)
        self.fraud_rate = fraud_rate
        self._heap = []
        self._counter = itertools.count()
        self.customers = []
        city_names = list(CITIES)
        for i in range(n_customers):
            city = self.rng.choice(city_names)
            lat, lon = CITIES[city]
            self.customers.append({
                "card_id": f"card_{i:05d}",
                "customer_id": f"cust_{i:05d}",
                "city": city,
                "lat": lat + self.rng.uniform(-0.05, 0.05),
                "lon": lon + self.rng.uniform(-0.05, 0.05),
                "mean_amount": max(200.0, self.rng.lognormvariate(math.log(1500), 0.5)),
                "devices": [f"dev_{i:05d}_{k}" for k in range(self.rng.randint(1, 2))],
            })
        self.merchants = [
            (f"m_{j:04d}", self.rng.choice(CATEGORIES)) for j in range(200)
        ]

    # ---------- helpers ----------
    def _txn_id(self):
        return f"txn_{self.rng.getrandbits(64):016x}"

    def _event(self, cust, when, amount, lat, lon, device, fraud_type=None):
        merchant_id, category = self.rng.choice(self.merchants)
        return {
            "txn_id": self._txn_id(),
            "card_id": cust["card_id"],
            "customer_id": cust["customer_id"],
            "merchant_id": merchant_id,
            "merchant_category": category,
            "amount": round(amount, 2),
            "currency": "INR",
            "lat": round(lat, 5),
            "lon": round(lon, 5),
            "device_id": device,
            "event_time": iso(when),
            "is_fraud": fraud_type is not None,
            "fraud_type": fraud_type,
        }

    def _schedule(self, when, event):
        heapq.heappush(self._heap, (when, next(self._counter), event))

    def _home_point(self, cust):
        return (
            cust["lat"] + self.rng.uniform(-0.05, 0.05),
            cust["lon"] + self.rng.uniform(-0.05, 0.05),
        )

    # ---------- normal + fraud ----------
    def _normal(self, cust, now):
        amount = max(10.0, self.rng.gauss(cust["mean_amount"], cust["mean_amount"] * 0.35))
        lat, lon = self._home_point(cust)
        device = self.rng.choice(cust["devices"])
        self._schedule(now, self._event(cust, now, amount, lat, lon, device))

    def start_scenario(self, name, now, cust=None):
        cust = cust or self.rng.choice(self.customers)
        if name == "velocity":
            n = self.rng.randint(6, 10)
            t = now
            for _ in range(n):
                amount = cust["mean_amount"] * self.rng.uniform(0.5, 1.5)
                lat, lon = self._home_point(cust)
                device = self.rng.choice(cust["devices"])
                self._schedule(t, self._event(cust, t, amount, lat, lon, device, "velocity"))
                t = t + timedelta(seconds=self.rng.uniform(5, 40))
        elif name == "amount_anomaly":
            amount = cust["mean_amount"] * self.rng.uniform(15, 40)
            lat, lon = self._home_point(cust)
            device = self.rng.choice(cust["devices"])
            self._schedule(now, self._event(cust, now, amount, lat, lon, device, "amount_anomaly"))
        elif name == "impossible_travel":
            lat, lon = self._home_point(cust)
            device = self.rng.choice(cust["devices"])
            amount = cust["mean_amount"] * self.rng.uniform(0.7, 1.3)
            self._schedule(now, self._event(cust, now, amount, lat, lon, device))
            far = [c for c in CITIES.values()
                   if haversine_km(cust["lat"], cust["lon"], c[0], c[1]) > 1000]
            flat, flon = self.rng.choice(far)
            later = now + timedelta(seconds=self.rng.uniform(60, 300))
            amount2 = cust["mean_amount"] * self.rng.uniform(0.7, 1.5)
            self._schedule(later, self._event(cust, later, amount2, flat, flon, device, "impossible_travel"))
        elif name == "new_device_high_amount":
            amount = cust["mean_amount"] * self.rng.uniform(8, 20)
            lat, lon = self._home_point(cust)
            device = f"dev_new_{self.rng.getrandbits(32):08x}"
            self._schedule(now, self._event(cust, now, amount, lat, lon, device, "new_device_high_amount"))
        else:
            raise ValueError(f"unknown scenario: {name}")

    # ---------- public API ----------
    def arrive(self, now):
        """Create one new piece of activity (normal txn or fraud scenario)."""
        if self.rng.random() < self.fraud_rate:
            self.start_scenario(self.rng.choice(FRAUD_TYPES), now)
        else:
            self._normal(self.rng.choice(self.customers), now)

    def due(self, now):
        """Return all events whose event_time <= now, oldest first."""
        out = []
        while self._heap and self._heap[0][0] <= now:
            out.append(heapq.heappop(self._heap)[2])
        return out

    def pending(self):
        return len(self._heap)


def utc_now():
    return datetime.now(timezone.utc)
