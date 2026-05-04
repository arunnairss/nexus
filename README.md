# ⚡ NEXUS — Real-Time User Activity Tracking System

> A mini big-data project simulating a Netflix/Uber-style clickstream pipeline.
> Built with Python · Kafka · Spark · Cassandra · Streamlit

---

## 📁 Project Structure

```
nexus/
├── app.py                      ← Streamlit dashboard (run this)
├── requirements.txt
├── data/
│   ├── seed_data.py            ← Generate 500 sample events (CSV + JSON)
│   ├── sample_events.csv       ← Generated sample data
│   └── sample_events.json
└── modules/
    ├── kafka_simulator.py      ← Simulated Kafka producer/consumer
    ├── spark_processor.py      ← Simulated Spark Streaming processor
    └── cassandra_simulator.py  ← Simulated Cassandra store
```

---

## 🚀 Quick Start (3 steps)

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. (Optional) Generate sample data
```bash
cd data
python seed_data.py
```

### 3. Run the dashboard
```bash
streamlit run app.py
```
Open http://localhost:8501 in your browser. Done! ✅

---

## 🌐 Where to Host

### ✅ Best Option — Streamlit Community Cloud (FREE)
1. Push this project to a **GitHub repo**
2. Go to https://share.streamlit.io
3. Connect your GitHub → select `app.py` → Deploy
4. Get a public URL like `https://your-name-nexus.streamlit.app`
- **Free**, permanent, shareable link
- Perfect for college project demos

### Option 2 — Render.com (FREE tier)
1. Push to GitHub
2. New Web Service → connect repo
3. Build command: `pip install -r requirements.txt`
4. Start command: `streamlit run app.py --server.port $PORT --server.address 0.0.0.0`

### Option 3 — Railway.app
- Similar to Render, generous free tier
- Add env var: `PORT=8501`

### Option 4 — Run locally and share via ngrok
```bash
pip install pyngrok
ngrok http 8501
```
Gives you a temporary public URL — great for live demos.

---

## 🔧 Upgrading to Real Tools (future)

| Simulated Component        | Real Replacement                              |
|---------------------------|-----------------------------------------------|
| `kafka_simulator.py`      | `pip install kafka-python` + Apache Kafka     |
| `spark_processor.py`      | `pip install pyspark` + Apache Spark          |
| `cassandra_simulator.py`  | `pip install cassandra-driver` + Cassandra DB |
| Sample data               | Real JS tracker snippet on your website       |

---

## 🏗️ Architecture

```
[Browser / JS Tracker]
        ↓  HTTP POST
[Python Event Producer]
        ↓  produce()
[Kafka Bus — topic: user-events]
        ↓  consume() micro-batch
[Spark Processor — enrichment + aggregation]
        ↓  write_events() / write_summary()
[Cassandra Store — partitioned by user_id]
        ↓  read
[Streamlit Dashboard — live charts + tables]
```

---

## 📌 Notes
- No Kafka/Spark/Cassandra installation needed — all simulated in-memory
- The `modules/` folder uses the same API design as the real tools
- Comments in each file show the real PySpark / cassandra-driver equivalent code
- Switch to real tools by swapping the import at the top of `app.py`

---

*Built by Arun & GF · Big Data Mini Project · 2026*
