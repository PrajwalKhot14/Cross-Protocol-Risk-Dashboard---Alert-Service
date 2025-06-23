import os, json, time, logging
from collections import deque, defaultdict
from kafka import KafkaConsumer, KafkaProducer
import numpy as np 
from dotenv import load_dotenv
from decimal import Decimal

from pipeline.models import Position
from pipeline.prices import price_cache, lts_cache
# from pipeline.consumer import state, price_cache, lts_cache

load_dotenv("env/.env")

BROKERS     = os.getenv("KAFKA_BROKERS","localhost:9092").split(",")
WINDOW_SIZE = 1    # minutes # Change this to 60
VAR_Q       = 0.01    # 1st percentile → 99% VaR


# ── Kafka clients ─────────────────────────────────────────────────────────────
consumer = KafkaConsumer(
    # "aave-raw", 
    "risk-deltas",
    bootstrap_servers=BROKERS,
    group_id="risk-aggregator",
    auto_offset_reset="latest",
    # enable_auto_commit=False,
    value_deserializer=lambda v: json.loads(v.decode()),
)

producer = KafkaProducer(
    bootstrap_servers=BROKERS,
    value_serializer=lambda v: json.dumps(v).encode(),
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

# In-memory state: per-user Position objects
# state = defaultdict(Position)

# ── sliding window ──────────────────────────────────────────────────────────────
window = deque(maxlen=WINDOW_SIZE)
last_emit = time.time()

# Helper to compute VaR once window is full
def compute_var(data):
    if len(data) < WINDOW_SIZE:
        return None
    return float(np.quantile(data, VAR_Q))

# Helper to compute Liquidations-at-Risk (LAR)
def compute_lar():
    prices = price_cache()
    lts    = lts_cache()
    total  = 0.0
    for pos in state.values():
        hf = pos.health_factor(prices, lts)
        if hf < 1:
            # sum all debt in USD or reference currency
            total += sum(prices[a] * q for a, q in pos.debt.items())
    return total

# ── main loop ──────────────────────────────────────────────────────────────────

# Main loop
try:
    for msg in consumer:
        topic = msg.topic
        record = msg.value

        # 1) Maintain position state from raw events
        if topic == "aave-raw":
            ev = record
            user = ev.get("user")
            asset = ev.get("asset")
            amt   = Decimal(ev.get("amount", "0")) / Decimal(10**18)
            pos   = state[user]
            match ev.get("event"):
                case "Supply":
                    pos.collateral[asset] = pos.collateral.get(asset, Decimal(0)) + amt
                case "Borrow":
                    pos.debt[asset]       = pos.debt.get(asset, Decimal(0))       + amt
                case "Repay":
                    old = pos.debt.get(asset, Decimal(0))
                    pos.debt[asset]       = max(old - amt, Decimal(0))
        # 2) Collect P&L deltas for VaR window
        elif topic == "risk-deltas":
            delta = record.get("delta")
            if isinstance(delta, (int, float)):
                window.append(delta)

        # Exactly-once: commit offsets after processing
        consumer.commit()

        # 3) Emit metrics every minute
        now = time.time()
        if now - last_emit >= 60 and len(window) == WINDOW_SIZE: # Change this to 60 
            # Calculate 99% VaR
            var = compute_var(list(window))
            lar = compute_lar()

            payload = {"ts": int(now), "var": var, "lar": lar}
            producer.send("risk-metrics", value=payload)
            producer.flush()
            logging.info(f"Published metrics → {payload}")
            last_emit = now

except KeyboardInterrupt:
    logging.info("Aggregator interrupted by user, shutting down...")
    consumer.close()
    producer.close()
