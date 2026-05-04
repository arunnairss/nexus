"""
seed_data.py — Generate sample user activity data (CSV + JSON)
Run: python data/seed_data.py
"""

import csv, json, random, uuid
from datetime import datetime, timedelta

PAGES = ["/home", "/products", "/checkout", "/dashboard", "/blog",
         "/pricing", "/login", "/signup", "/search", "/profile"]
EVENT_TYPES = ["click", "page_view", "form_submit", "search", "add_to_cart", "purchase"]
DEVICES = ["desktop", "mobile", "tablet"]
BROWSERS = ["Chrome", "Firefox", "Safari", "Edge"]
USERS = [f"usr_{1000 + i}" for i in range(50)]

def random_event(ts=None):
    if ts is None:
        ts = datetime.now() - timedelta(seconds=random.randint(0, 86400))
    return {
        "event_id":   str(uuid.uuid4()),
        "user_id":    random.choice(USERS),
        "session_id": str(uuid.uuid4())[:8],
        "event_type": random.choice(EVENT_TYPES),
        "page_url":   random.choice(PAGES),
        "device":     random.choice(DEVICES),
        "browser":    random.choice(BROWSERS),
        "timestamp":  ts.isoformat(),
        "duration_ms": random.randint(50, 4000),
        "metadata":   json.dumps({"ref": random.choice(["google", "direct", "twitter", "email"])})
    }

# ── Write CSV ────────────────────────────────────────────────
rows = [random_event() for _ in range(500)]
fieldnames = list(rows[0].keys())

with open("data/sample_events.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

# ── Write JSON (last 50 for stream demo) ────────────────────
with open("data/sample_events.json", "w") as f:
    json.dump(rows[:50], f, indent=2)

print(f"✅  Generated {len(rows)} events → data/sample_events.csv + data/sample_events.json")
