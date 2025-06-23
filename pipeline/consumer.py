# pipeline/consumer.py

import os, json, time, logging, requests
from decimal import Decimal
from collections import defaultdict
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer


from pipeline.models import Position
from pipeline.prices import price_cache, lts_cache

# ────────────────────────────────────────────────────────────────────────────
# Load env vars
load_dotenv("env/.env")
BROKERS       = os.getenv("KAFKA_BROKERS", "localhost:9092").split(",")
SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK")
ALERT_HF      = float(os.getenv("ALERT_HF", "1.05"))
THROTTLE      = int(os.getenv("ALERT_THROTTLE", "3600"))  # seconds between alerts

# ── NEW delta‐producer & snapshot state ─────────────────────────────────────────
DELTA_PRODUCER = KafkaProducer(
    bootstrap_servers=BROKERS,
    value_serializer=lambda v: json.dumps(v).encode(),
)
_last_snapshot = 0.0
_last_value    = None

# ────────────────────────────────────────────────────────────────────────────
# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

# ────────────────────────────────────────────────────────────────────────────
# Kafka consumer (exactly-once commits)
consumer = KafkaConsumer(
    "aave-raw",
    bootstrap_servers=BROKERS,
    # group_id="risk-dash-consumer",
    # auto_offset_reset="earliest",

    group_id="risk-dash-test",
    auto_offset_reset="latest", 
    enable_auto_commit=False,
    value_deserializer=lambda v: json.loads(v.decode()),
)

# ────────────────────────────────────────────────────────────────────────────
# In-memory state per wallet
state: dict[str, Position] = defaultdict(Position)

def post_slack(text: str):
    if not SLACK_WEBHOOK:
        return
    try:
        requests.post(SLACK_WEBHOOK, json={"text": text}, timeout=5)
    except Exception as e:
        logging.error(f"Slack post failed: {e}")

# ────────────────────────────────────────────────────────────────────────────
# Main loop
for msg in consumer:
    ev    = msg.value
    user  = ev["user"]
    asset = ev["asset"]
    amt   = Decimal(ev["amount"]) / Decimal(10**18)

    pos = state[user]
    match ev.get("event"):
        case "Supply":
            pos.collateral[asset] = pos.collateral.get(asset, Decimal(0)) + amt
        case "Borrow":
            pos.debt[asset]       = pos.debt.get(asset, Decimal(0))       + amt
        case "Repay":
            old = pos.debt.get(asset, Decimal(0))
            pos.debt[asset] = max(old - amt, Decimal(0))
        case _:
            # ignore other events
            pass

    # Compute Health Factor
    hf = pos.health_factor(price_cache(), lts_cache())
    logging.info(f"user={user[:6]} hf={hf:.2f}")

    # Alert if below threshold and throttle per wallet
    now = time.time()
    if hf < ALERT_HF and now - pos.last_alert_ts > THROTTLE:
        text = f"⚠️ {user[:6]}… HF {hf:.2f} (block {ev['block']})"
        post_slack(text)
        pos.last_alert_ts = now

    # Exactly-once: commit this offset now that it’s been processed
    consumer.commit()

    # —— publish P&L delta every 60 s ——  
    now = time.time()
    if now - _last_snapshot >= 60:
        # 1) compute total net value
        prices = price_cache()
        total = Decimal(0)
        for pos in state.values():
            coll = sum(qty * prices[a] for a, qty in pos.collateral.items())
            debt = sum(qty * prices[a] for a, qty in pos.debt.items())
            total += (coll - debt)

        # 2) publish delta once we have an initial value
        if _last_value is not None:
            delta = float(total - _last_value)
            payload = {"ts": int(now), "value": float(total), "delta": delta}
            DELTA_PRODUCER.send("risk-deltas", value=payload)
            DELTA_PRODUCER.flush()
            logging.info(f"Published delta → {payload}")

        _last_value    = total
        _last_snapshot = now
