"""
modules/spark_processor.py
Simulates Spark Structured Streaming micro-batch processing.
Uses pandas for transformations (same logic you'd write in PySpark).

Real PySpark equivalent comments included for each step.
"""

import threading
import time
import json
from collections import defaultdict, deque
from datetime import datetime
import pandas as pd

from kafka_simulator import kafka_bus, TOPIC


class SparkProcessor(threading.Thread):
    """
    Consumes events from the Kafka bus in micro-batches,
    applies enrichment + aggregation, and writes to the
    simulated Cassandra store.

    Real PySpark equivalent:
        spark.readStream.format("kafka")
             .option("kafka.bootstrap.servers", "localhost:9092")
             .option("subscribe", "user-events")
             .load()
    """

    BATCH_INTERVAL = 2.0   # seconds between batches (micro-batch window)

    def __init__(self, cassandra_store):
        super().__init__(daemon=True)
        self._running       = threading.Event()
        self._running.set()
        self.cassandra      = cassandra_store
        self.batches_done   = 0
        self.total_processed = 0

        # Rolling stats (last 60 batches)
        self.batch_sizes    = deque(maxlen=60)
        self.event_rate     = deque(maxlen=60)

    # ── Transformations ──────────────────────────────────────

    def _enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        PySpark equivalent:
            df.withColumn("hour", hour("timestamp"))
              .withColumn("is_mobile", col("device") == "mobile")
        """
        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["hour"]      = df["timestamp"].dt.hour
        df["is_mobile"] = df["device"] == "mobile"
        df["is_bounce"]  = df["duration_ms"] < 200
        return df

    def _aggregate(self, df: pd.DataFrame) -> dict:
        """
        PySpark equivalent:
            df.groupBy("page_url", "event_type").count()
        """
        page_counts  = df["page_url"].value_counts().to_dict()
        event_counts = df["event_type"].value_counts().to_dict()
        device_counts = df["device"].value_counts().to_dict()
        mobile_pct   = round(df["is_mobile"].mean() * 100, 1)
        bounce_pct   = round(df["is_bounce"].mean() * 100, 1)
        unique_users = df["user_id"].nunique()

        return {
            "page_counts":   page_counts,
            "event_counts":  event_counts,
            "device_counts": device_counts,
            "mobile_pct":    mobile_pct,
            "bounce_pct":    bounce_pct,
            "unique_users":  unique_users,
            "batch_size":    len(df),
            "processed_at":  datetime.now().isoformat(),
        }

    # ── Main loop ────────────────────────────────────────────

    def run(self):
        while self._running.is_set():
            batch = []
            deadline = time.time() + self.BATCH_INTERVAL

            # Drain queue until batch window closes
            while time.time() < deadline:
                msg = kafka_bus.consume(TOPIC, timeout=0.1)
                if msg:
                    batch.append(msg)

            if batch:
                df      = pd.DataFrame(batch)
                df      = self._enrich(df)
                summary = self._aggregate(df)

                # Write raw events + aggregated summary to Cassandra
                self.cassandra.write_events(df.to_dict("records"))
                self.cassandra.write_summary(summary)

                self.batches_done     += 1
                self.total_processed  += len(batch)
                self.batch_sizes.append(len(batch))
                self.event_rate.append(len(batch) / self.BATCH_INTERVAL)

    def stop(self):
        self._running.clear()

    def stats(self) -> dict:
        sizes = list(self.batch_sizes)
        return {
            "batches_done":    self.batches_done,
            "total_processed": self.total_processed,
            "avg_batch_size":  round(sum(sizes) / len(sizes), 1) if sizes else 0,
            "avg_event_rate":  round(sum(self.event_rate) / len(self.event_rate), 2)
                               if self.event_rate else 0,
        }


# ── Quick test ───────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    sys.path.insert(0, ".")
    from cassandra_simulator import CassandraStore
    from kafka_simulator import EventProducer

    store     = CassandraStore()
    processor = SparkProcessor(store)
    producer  = EventProducer(rate_per_sec=10)

    print("▶  Running producer + Spark processor for 6 seconds...")
    producer.start()
    processor.start()
    time.sleep(6)
    producer.stop()
    processor.stop()
    time.sleep(1)

    print(f"\n📊 Spark Stats: {json.dumps(processor.stats(), indent=2)}")
    print(f"🗄️  Cassandra rows: {store.total_rows()}")
    print(f"📄 Latest summary:\n{json.dumps(store.latest_summary(), indent=2)}")
