# pipeline/snapshot.py

import os, argparse
import json
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaConsumer, TopicPartition
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv


# # 1) Parse CLI args
# parser = argparse.ArgumentParser()
# parser.add_argument("--topic", "-t", help="Kafka topic to snapshot")
# args = parser.parse_args()

# ── CONFIG ───────────────────────────────────────────────────────────
load_dotenv("env/.env")
BROKERS     = os.getenv("KAFKA_BROKERS", "localhost:9092").split(",")
TOPIC       = os.getenv("SNAPSHOT_TOPIC", "risk-metrics") # risk-metrics, risk-deltas
OUT_DIR     = Path(os.getenv("SNAPSHOT_DIR", "snapshots"))
OUT_DIR.mkdir(exist_ok=True)
GROUP_ID    = f"snapshotter-{TOPIC}"
OFFSET_FILE = OUT_DIR / f"{TOPIC}-offset.txt"

# ── Load last offset ───────────────────────────────────────────────────
if OFFSET_FILE.exists():
    raw = OFFSET_FILE.read_text().strip()
    try:
        last_offset = int(raw)
    except (ValueError, TypeError):
        last_offset = None
else:
    last_offset = None

# ── Create consumer ────────────────────────────────────────────────────
consumer = KafkaConsumer(
    bootstrap_servers=BROKERS,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    consumer_timeout_ms=5000,
    value_deserializer=lambda v: json.loads(v.decode()),
)

# ── Manually assign all partitions of the topic ─────────────────────────
parts = consumer.partitions_for_topic(TOPIC)
if not parts:
    raise RuntimeError(f"No partitions found for topic {TOPIC!r}. Is Kafka up and the topic created?")
# build TopicPartition list
tps = [TopicPartition(TOPIC, p) for p in sorted(parts)]
consumer.assign(tps)

# ── Seek to last processed offset+1 (or the beginning) ───────────────────
for tp in tps:
    if last_offset is not None:
        consumer.seek(tp, last_offset + 1)
    else:
        consumer.seek_to_beginning(tp)

# ── Pull and collect records ───────────────────────────────────────────
records = []
for msg in consumer:
    records.append(msg.value)
    last_offset = msg.offset

# ── Commit & persist the offset ────────────────────────────────────────
consumer.commit()
OFFSET_FILE.write_text(str(last_offset))

if not records:
    print("No new records; exiting.")
    exit(0)

# ── Build DataFrame and write Parquet ─────────────────────────────────
df = pd.DataFrame(records)
hour_str = time.strftime("%Y%m%dT%H", time.gmtime(time.time()))
outfile = OUT_DIR / f"{TOPIC}-{hour_str}.parquet"

table = pa.Table.from_pandas(df)
if outfile.exists():
    old = pq.read_table(outfile)
    new = pa.concat_tables([old, table])
    pq.write_table(new, outfile)
else:
    pq.write_table(table, outfile)

print(f"Wrote {len(records)} records to {outfile}")
