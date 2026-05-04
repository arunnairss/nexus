"""
modules/cassandra_simulator.py
Simulates Cassandra writes using an in-memory store (dict + deque).

Real Cassandra equivalent uses cassandra-driver:
    from cassandra.cluster import Cluster
    cluster = Cluster(['localhost'])
    session = cluster.connect('activity_tracker')
    session.execute("INSERT INTO user_events (...) VALUES (...)")

Schema mirrors a real Cassandra table:
    CREATE TABLE user_events (
        user_id     TEXT,
        event_time  TIMESTAMP,
        session_id  TEXT,
        event_type  TEXT,
        page_url    TEXT,
        device      TEXT,
        browser     TEXT,
        duration_ms INT,
        metadata    MAP<TEXT, TEXT>,
        PRIMARY KEY (user_id, event_time)
    ) WITH CLUSTERING ORDER BY (event_time DESC);
"""

import threading
from collections import defaultdict, deque
from datetime import datetime
from typing import List


class CassandraStore:
    """
    Thread-safe in-memory store that mirrors Cassandra's
    partition-key design: events are grouped by user_id.
    """

    MAX_ROWS_PER_USER = 200   # cap to avoid unbounded memory
    MAX_SUMMARIES     = 100

    def __init__(self):
        self._lock     = threading.Lock()
        # Partitioned by user_id (like Cassandra partition key)
        self._partitions: dict[str, deque] = defaultdict(
            lambda: deque(maxlen=self.MAX_ROWS_PER_USER)
        )
        # Aggregated summaries from Spark
        self._summaries: deque = deque(maxlen=self.MAX_SUMMARIES)
        self._row_count = 0

    # ── Writes ───────────────────────────────────────────────

    def write_events(self, events: List[dict]):
        """Batch INSERT equivalent."""
        with self._lock:
            for e in events:
                uid = e.get("user_id", "unknown")
                self._partitions[uid].append(e)
                self._row_count += 1

    def write_summary(self, summary: dict):
        """Write aggregated Spark output."""
        with self._lock:
            self._summaries.append(summary)

    # ── Reads ────────────────────────────────────────────────

    def get_events_for_user(self, user_id: str) -> list:
        """SELECT * FROM user_events WHERE user_id = ?"""
        with self._lock:
            return list(self._partitions.get(user_id, []))

    def get_recent_events(self, n: int = 20) -> list:
        """Scan recent events across all partitions (for dashboard)."""
        with self._lock:
            all_events = []
            for partition in self._partitions.values():
                all_events.extend(list(partition))
            all_events.sort(key=lambda e: e.get("timestamp", ""), reverse=True)
            return all_events[:n]

    def latest_summary(self) -> dict:
        with self._lock:
            return dict(self._summaries[-1]) if self._summaries else {}

    def all_summaries(self) -> list:
        with self._lock:
            return list(self._summaries)

    def total_rows(self) -> int:
        return self._row_count

    def active_users(self) -> int:
        with self._lock:
            return len(self._partitions)

    def top_pages(self, n: int = 5) -> dict:
        """Aggregate page views across all summaries."""
        with self._lock:
            totals: dict = defaultdict(int)
            for s in self._summaries:
                for page, count in s.get("page_counts", {}).items():
                    totals[page] += count
            return dict(sorted(totals.items(), key=lambda x: x[1], reverse=True)[:n])

    def top_events(self, n: int = 5) -> dict:
        with self._lock:
            totals: dict = defaultdict(int)
            for s in self._summaries:
                for etype, count in s.get("event_counts", {}).items():
                    totals[etype] += count
            return dict(sorted(totals.items(), key=lambda x: x[1], reverse=True)[:n])

    def event_rate_series(self) -> list:
        """Returns per-batch event counts for sparkline chart."""
        with self._lock:
            return [s.get("batch_size", 0) for s in self._summaries]


# ── Quick test ───────────────────────────────────────────────
if __name__ == "__main__":
    store = CassandraStore()
    store.write_events([
        {"user_id": "usr_1001", "event_type": "click", "page_url": "/home",
         "timestamp": datetime.now().isoformat(), "device": "desktop",
         "browser": "Chrome", "duration_ms": 300, "session_id": "abc123"}
    ])
    store.write_summary({"page_counts": {"/home": 5}, "event_counts": {"click": 3},
                         "batch_size": 8, "mobile_pct": 40.0, "bounce_pct": 15.0,
                         "unique_users": 3, "device_counts": {"desktop": 5},
                         "processed_at": datetime.now().isoformat()})

    print(f"✅ Total rows: {store.total_rows()}")
    print(f"📄 Latest summary: {store.latest_summary()}")
    print(f"🔝 Top pages: {store.top_pages()}")
