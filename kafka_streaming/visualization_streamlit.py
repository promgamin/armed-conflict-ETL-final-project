import sys
import os
import time
import threading
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from kafka_streaming.consumer import get_kafka_consumer

REFRESH_INTERVAL = 2
DISPLACEMENT_KEY = "desplazamiento"

st.set_page_config(
    page_title="Armed Conflict - Real Time Dashboard",
    layout="wide",
)

#Kafka listener running in a daemon thread, stores records in a dict to avoid duplicates
def kafka_listener(buffer: dict):
    consumer = get_kafka_consumer()
    while True:
        message_batch = consumer.poll(timeout_ms=1000)
        if not message_batch:
            continue
        for _, messages in message_batch.items():
            for message in messages:
                record = message.value
                key = hash(frozenset((k, str(v)) for k, v in record.items()))
                buffer[key] = record


#Init session state once per session
if "buffer" not in st.session_state:
    st.session_state.buffer = {}

if "thread_started" not in st.session_state:
    st.session_state.thread_started = False

if not st.session_state.thread_started:
    t = threading.Thread(
        target=kafka_listener,
        args=(st.session_state.buffer,),
        daemon=True,
    )
    t.start()
    st.session_state.thread_started = True


#header
st.title("Armed Conflict in Colombia — Real Time Dashboard")
st.caption(f"Auto-refresh every {REFRESH_INTERVAL}s · Kafka topic: armed_conflict_metrics")
st.divider()

buffer = st.session_state.buffer

#Empty state
if not buffer:
    st.info("Waiting for data from Kafka... Make sure the producer is running.")
    time.sleep(REFRESH_INTERVAL)
    st.rerun()

df = pd.DataFrame(list(buffer.values()))

#Top-level metrics
total_records = len(df)

col1, col2 = st.columns(2)
with col1:
    st.metric(
        label="Total Records Received",
        value=f"{total_records:,}",
    )
with col2:
    st.metric(
        label="Unique Departments",
        value=df["state_dept"].nunique() if "state_dept" in df.columns else "N/A",
    )

st.divider()

col_left, col_right = st.columns(2)

#Top 5 departments horizontal bar chart
with col_left:
    st.subheader("Top 5 Departments by Victims")
    if "state_dept" in df.columns:
        top5 = (
            df.groupby("state_dept", observed=True)
            .size()
            .nlargest(5)
            .reset_index(name="total_records")
            .sort_values("total_records", ascending=True)
        )
        fig_bar = px.bar(
            top5,
            x="total_records",
            y="state_dept",
            orientation="h",
            labels={"total_records": "Total Records", "state_dept": "Department"},
            color="total_records",
            color_continuous_scale="Reds",
        )
        fig_bar.update_layout(
            showlegend=False,
            coloraxis_showscale=False,
            margin=dict(l=0, r=0, t=10, b=0),
            height=300,
        )
        st.plotly_chart(fig_bar, use_container_width=True)
    else:
        st.warning("Column 'state_dept' not found in records.")

#Displacement vs others proportion over time
with col_right:
    st.subheader("Displacement vs Other Facts (%)")
    if "victimization_fact" in df.columns:
        df_line = df.copy()
        df_line["is_displacement"] = (
            df_line["victimization_fact"]
            .astype(str)
            .str.contains(DISPLACEMENT_KEY, case=False, na=False)
        )
        df_line["record_num"] = range(1, len(df_line) + 1)
        df_line["cum_displacement"] = df_line["is_displacement"].cumsum()
        df_line["displacement_pct"] = df_line["cum_displacement"] / df_line["record_num"] * 100
        df_line["others_pct"] = 100 - df_line["displacement_pct"]

#downsample to max 300 points so the chart stays responsive
        step = max(1, len(df_line) // 300)
        df_sampled = df_line.iloc[::step][["record_num", "displacement_pct", "others_pct"]]

        fig_line = go.Figure()
        fig_line.add_trace(go.Scatter(
            x=df_sampled["record_num"],
            y=df_sampled["displacement_pct"],
            mode="lines",
            name="Forced Displacement",
            line=dict(color="#e74c3c", width=2),
        ))
        fig_line.add_trace(go.Scatter(
            x=df_sampled["record_num"],
            y=df_sampled["others_pct"],
            mode="lines",
            name="Other Facts",
            line=dict(color="#3498db", width=2),
        ))
        fig_line.update_layout(
            xaxis_title="Records received",
            yaxis_title="Percentage (%)",
            yaxis=dict(range=[0, 100]),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            margin=dict(l=0, r=0, t=30, b=0),
            height=300,
        )
        st.plotly_chart(fig_line, use_container_width=True)
    else:
        st.warning("Column 'victimization_fact' not found in records.")

st.divider()

#last 5 records table
st.subheader("Last 5 Records Received")
display_cols = [
    "produced_at", "date_processing", "state_dept", "victimization_fact",
    "sex", "ethnic_group", "age_range", "total_victim", "source",
]
available_cols = [c for c in display_cols if c in df.columns]
last5 = df[available_cols].tail(5).iloc[::-1].reset_index(drop=True)
last5.index = last5.index + 1
st.dataframe(last5, use_container_width=True)

# Auto-refresh
time.sleep(REFRESH_INTERVAL)
st.rerun()