# dashboard.py

import os, time, json
import duckdb
import pandas as pd
import streamlit as st
from kafka import KafkaConsumer
from streamlit_autorefresh import st_autorefresh

# ── Config ───────────────────────────────────────────────────────────
BROKERS       = os.getenv("KAFKA_BROKERS", "localhost:9092").split(",")
DB_PATH       = os.getenv("DBT_DUCKDB_PATH", "risk_dw/analytics.db")
METRIC_TOPIC  = "risk-metrics"
DELTA_TOPIC   = "risk-deltas"
POLL_INTERVAL = 30  # seconds between Kafka polls

# ── Page setup ───────────────────────────────────────────────────────
st.set_page_config(layout="wide", page_title="Risk Dashboard")
st.title("📊 Real-Time Risk Dashboard")

# ── Sidebar ───────────────────────────────────────────────────────────
hours = st.sidebar.slider("Historical window (hours)", 1, 24, 6)

# ── Historical loaders ────────────────────────────────────────────────

if "latest_metrics" not in st.session_state:
    st.session_state.latest_metrics = {"var": None, "lar": None, "ts": None}

if "latest_delta" not in st.session_state:
    st.session_state.latest_delta = {"delta": None, "value": None, "ts": None}



@st.cache_data(ttl=60)
def load_metrics_history(h):
    con = duckdb.connect()
    try:
        return (
            con.execute(f"""
                SELECT
                date_trunc('hour', to_timestamp(ts))    AS hour,
                avg(var)                                AS var,
                avg(lar)                                AS lar
                FROM parquet_scan('snapshots/risk-metrics-*.parquet')
                WHERE to_timestamp(ts) >= now() - INTERVAL '{h} hours'
                GROUP BY hour
                ORDER BY hour
            """)
            .df()
        )
    except Exception as e:
        st.error(f"Error loading metrics: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60)
def load_deltas_history(h):
    con = duckdb.connect(DB_PATH)
    try:
        return (
            con.execute(f"""
              SELECT
                date_trunc('minute', to_timestamp(ts)) AS ts_min,
                avg(delta) AS avg_delta
              FROM parquet_scan('snapshots/risk-deltas-*.parquet')
              WHERE to_timestamp(ts) >= now() - INTERVAL '{h} hours'
              GROUP BY ts_min
              ORDER BY ts_min
            """)
            .df()
        )
    except Exception as e:
        st.error(f"Error loading deltas: {e}")
        return pd.DataFrame()

metrics_hist = load_metrics_history(hours)
deltas_hist  = load_deltas_history(hours)

# ── Tabs for Metrics vs. Deltas ───────────────────────────────────────
tab1, tab2 = st.tabs(["VaR & LaR", "P&L Deltas"])

with tab1:
    if not metrics_hist.empty:
        latest_row = metrics_hist.iloc[-1]

        col1, col2 = st.columns(2)
        with col1:
            st.metric("Current VaR (hourly)", f"{latest_row['var']:.4f}")
        with col2:
            st.metric("Current LaR (hourly)", f"{latest_row['lar']:.2f}")

        col1, col2 = st.columns(2)
        with col1:
            st.subheader(f"VaR (last {hours}h)")
            st.line_chart(metrics_hist.set_index("hour")["var"])
        with col2:
            st.subheader(f"LaR (last {hours}h)")
            st.line_chart(metrics_hist.set_index("hour")["lar"])
    else:
        st.warning("No VaR/LaR historical data available.")

    st.markdown("---")
    st.subheader("Live VaR/LaR (most recent)")
    live_metrics_placeholder = st.empty()

with tab2:
    if not deltas_hist.empty:
        latest_delta_row = deltas_hist.iloc[-1]
        delta_val = st.session_state.latest_delta.get("delta")
        value_val = st.session_state.latest_delta.get("value")
        
        col1, col2 = st.columns(2)
        with col1:
            if delta_val is not None:
                st.metric("Current Δ (avg/min)", f"{delta_val:.2e}")
            else:
                st.metric("Current Δ (avg/min)", "N/A")

        with col2:
            if value_val is not None:
                st.metric("Last Total Value", f"{value_val:,.2f}")
            else:
                st.metric("Last Total Value", "N/A")

        st.subheader(f"P&L Δ (avg per minute, last {hours}h)")
        st.line_chart(deltas_hist.set_index("ts_min")["avg_delta"])
    else:
        st.warning("No Δ historical data available.")

    st.markdown("---")
    st.subheader("Live Δ (most recent)")
    live_deltas_placeholder = st.empty()

# ── Live Kafka consumers ───────────────────────────────────────────────
if "kafka_consumer" not in st.session_state:
    st.session_state.kafka_consumer = KafkaConsumer(
        METRIC_TOPIC,
        DELTA_TOPIC,
        bootstrap_servers=BROKERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode()),
        group_id="risk-dashboard-ui",  # ensure consumer group
    )
consumer = st.session_state.kafka_consumer





# def update_live():
#     msgs = consumer.poll(timeout_ms=1000, max_records=10)
#     for tp, records in msgs.items():
#         for rec in records:
#             val = rec.value
#             if rec.topic == METRIC_TOPIC:
#                 latest_metrics.update(val)
#             elif rec.topic == DELTA_TOPIC:
#                 latest_delta.update(val)

#     if latest_metrics["ts"]:
#         dfm = pd.DataFrame({
#             "Metric": ["VaR", "LaR"],
#             "Value": [latest_metrics["var"], latest_metrics["lar"]]
#         }).set_index("Metric")
#         live_metrics_placeholder.table(dfm)

#     if latest_delta.get("ts"):
#         dfd = pd.DataFrame({
#             "Field": ["Δ", "Total Value"],
#             "Value": [latest_delta["delta"], latest_delta["value"]]
#         }).set_index("Field")
#         live_deltas_placeholder.table(dfd)


def update_live():
    msgs = consumer.poll(timeout_ms=1000, max_records=10)

    for tp, records in msgs.items():
        for rec in records:
            val = rec.value
            if rec.topic == METRIC_TOPIC:
                st.session_state.latest_metrics.update(val)
            elif rec.topic == DELTA_TOPIC:
                st.session_state.latest_delta.update(val)
                print("Latest delta:", st.session_state.latest_delta)

    # Render to Streamlit
    if st.session_state.latest_metrics.get("ts"):
        dfm = pd.DataFrame({
            "Metric": ["VaR", "LaR"],
            "Value": [st.session_state.latest_metrics["var"], st.session_state.latest_metrics["lar"]]
        }).set_index("Metric")
        live_metrics_placeholder.table(dfm)
    else:
        st.info("No live metrics available")

    if st.session_state.latest_delta.get("ts"):
        dfd = pd.DataFrame({
            "Field": ["Δ", "Total Value"],
            "Value": [
                f"{st.session_state.latest_delta['delta']:.2e}",
                f"{st.session_state.latest_delta['value']:,.2f}"
            ]
        }).set_index("Field")
        live_deltas_placeholder.table(dfd)
    else:
        st.info("No live delta data available")


# ── Main loop to refresh live data ────────────────────────────────────
# Refresh every 5 seconds
st_autorefresh(interval=POLL_INTERVAL * 1000, key="refresh")
update_live()