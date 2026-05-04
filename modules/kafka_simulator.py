"""
modules/kafka_simulator.py
Simulates a Kafka producer + consumer using an in-memory queue.
No actual Kafka installation needed — drop-in replacement for demo/dev.

In a real setup, swap SimulatedKafka with:
    from kafka import KafkaProducer, KafkaConsumer
"""

import json
import queue
import threading
import time
import uuid
import random
from datetime import datetime

PAGES       = ["/home", "/products", "/checkout", "/dashboard",
               "/blog", "/pricing", "/search", "/profile"]
EVENT_TYPES = ["click", "page_view", "form_submit", "search",
               "add_to_cart", "purchase", "logout"]
DEVICES     = ["desktop", "mobile", "tablet"]
BROWSERS    = ["Chrome", "Firefox", "Safari", "Edge"]
USERS       = [f"usr_{1000 + i}" for i in range(100)]


class SimulatedKafka:
    """Thread-safe in-memory message bus that mimics Kafka topics."""

    def __init__(self):
        self._topics: dict[str, queue.Queue] = {}
        self._lock = threading.Lock()

    def create_topic(self, topic: str):
        with self._lock:
            if topic not in self._topics:
                self._topics[topic] = queue.Queue()

    def produce(self, topic: str, message: dict):
        self.create_topic(topic)
        self._topics[topic].put(json.dumps(message))

    def consume(self, topic: str, timeout: float = 0.5):
        """Yield messages from a topic. Blocks up to `timeout` seconds."""
        self.create_topic(topic)
        try:
            raw = self._topics[topic].get(timeout=timeout)
            return json.loads(raw)
        except queue.Empty:
            return None

    def qsize(self, topic: str) -> int:
        return self._topics.get(topic, queue.Queue()).qsize()


# ── Shared bus (import this in other modules) ─────────────────
kafka_bus = SimulatedKafka()
TOPIC = "user-events"


def generate_event() -> dict:
    return {
        "event_id":    str(uuid.uuid4()),
        "user_id":     random.choice(USERS),
        "session_id":  str(uuid.uuid4())[:8],
        "event_type":  random.choice(EVENT_TYPES),
        "page_url":    random.choice(PAGES),
        "device":      random.choice(DEVICES),
        "browser":     random.choice(BROWSERS),
        "timestamp":   datetime.now().isoformat(),
        "duration_ms": random.randint(50, 4000),
        "metadata": {
            "ref":    random.choice(["google", "direct", "twitter", "email"]),
            "region": random.choice(["IN", "US", "EU", "APAC"])
        }
    }


class EventProducer(threading.Thread):
    """
    Simulates a JS web tracker sending events to Kafka.
    Runs in a background thread — call .start() then .stop().
    """

    def __init__(self, rate_per_sec: float = 5.0):
        super().__init__(daemon=True)
        self.rate     = rate_per_sec
        self._running = threading.Event()
        self._running.set()
        self.produced = 0

    def run(self):
        interval = 1.0 / self.rate
        while self._running.is_set():
            event = generate_event()
            kafka_bus.produce(TOPIC, event)
            self.produced += 1
            time.sleep(interval)

    def stop(self):
        self._running.clear()


# ── Quick sanity check ────────────────────────────────────────
if __name__ == "__main__":
    print("🚀 Starting producer at 5 events/sec for 3 seconds...")
    p = EventProducer(rate_per_sec=5)
    p.start()
    time.sleep(3)
    p.stop()

    print(f"📨 Produced: {p.produced} events | Queue depth: {kafka_bus.qsize(TOPIC)}")
    msg = kafka_bus.consume(TOPIC)
    print(f"📥 Sample message:\n{json.dumps(msg, indent=2)}")
