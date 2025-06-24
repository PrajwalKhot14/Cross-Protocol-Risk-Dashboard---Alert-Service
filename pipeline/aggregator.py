import os, json, time, logging
from collections import deque
from kafka import KafkaConsumer, KafkaProducer
import numpy as np
from dotenv import load_dotenv
from decimal import Decimal

from pipeline.models import Position
from pipeline.prices import price_cache, lts_cache
from pipeline.state import state 

load_dotenv("env/.env")



BROKERS     = os.getenv("KAFKA_BROKERS", "localhost:9092").split(",")
WINDOW_SIZE = 1
VAR_Q       = 0.01  # 99% VaR

print("✅ Using group_id = risk-aggregator")

consumer = KafkaConsumer(
    "aave-raw", "risk-deltas",
    bootstrap_servers=BROKERS,
    group_id="risk-aggregator",
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode()),
)

producer = KafkaProducer(
    bootstrap_servers=BROKERS,
    value_serializer=lambda v: json.dumps(v).encode(),
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

window = deque(maxlen=WINDOW_SIZE)
last_emit = time.time()

def compute_var(data):
    if len(data) < WINDOW_SIZE:
        return None
    return float(np.quantile(data, VAR_Q))

def compute_lar():
    prices = price_cache()
    lts    = lts_cache()
    total  = Decimal(0)

    for pos in state.values():
        hf = pos.health_factor(prices, lts)
        if hf < 1:
            for asset, qty in pos.debt.items():
                # qty is a Decimal, prices[asset] is a Decimal
                total += prices[asset] * qty

    # return a float for JSON serialization
    return float(total)


try:
    for msg in consumer:
        topic = msg.topic
        record = msg.value

        if topic == "aave-raw":
            ev = record
            user = ev.get("user")
            asset = ev.get("asset")
            amt = Decimal(ev.get("amount", "0")) / Decimal(10**18)
            pos = state[user]
            match ev.get("event"):
                case "Supply":
                    pos.collateral[asset] = pos.collateral.get(asset, Decimal(0)) + amt
                case "Borrow":
                    pos.debt[asset] = pos.debt.get(asset, Decimal(0)) + amt
                case "Repay":
                    old = pos.debt.get(asset, Decimal(0))
                    pos.debt[asset] = max(old - amt, Decimal(0))

        elif topic == "risk-deltas":
            delta = record.get("delta")
            if isinstance(delta, (int, float)):
                window.append(delta)

        consumer.commit()

        now = time.time()
        if now - last_emit >= 60 and len(window) == WINDOW_SIZE:
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
