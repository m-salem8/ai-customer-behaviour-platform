import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
from streamlit_autorefresh import st_autorefresh

from live_events import render_live_events
from aggregated_metrics import render_aggregated_metrics

st.set_page_config(
    page_title="Customer Behaviour Dashboard",
    layout="wide"
)

st_autorefresh(interval=5000, key="dashboard_refresh")

st.title("Customer Behaviour Analytics Dashboard")

engine = create_engine(
    "postgresql://user:password@postgres:5432/customer_db"
)

time_window = st.selectbox(
    "Time window",
    ["Last 5 minutes", "Last 15 minutes", "Last 1 hour", "All time"]
)

window_sql = {
    "Last 5 minutes": "WHERE event_time >= EXTRACT(EPOCH FROM NOW() - INTERVAL '5 minutes')",
    "Last 15 minutes": "WHERE event_time >= EXTRACT(EPOCH FROM NOW() - INTERVAL '15 minutes')",
    "Last 1 hour": "WHERE event_time >= EXTRACT(EPOCH FROM NOW() - INTERVAL '1 hour')",
    "All time": ""
}[time_window]

extra_condition = window_sql.replace("WHERE", "AND") if window_sql else ""

total_events_df = pd.read_sql(f"""
SELECT COUNT(*) AS total_events
FROM customer_events
{window_sql};
""", engine)

conversion_df = pd.read_sql(f"""
SELECT
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases,
    SUM(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) AS product_views,
    CASE
        WHEN SUM(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) = 0
        THEN 0
        ELSE ROUND(
            SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END)::numeric
            / SUM(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END)::numeric
            * 100,
            2
        )
    END AS conversion_rate
FROM customer_events
{window_sql};
""", engine)

cart_abandonment_df = pd.read_sql(f"""
SELECT
    COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN user_id END) AS users_added_to_cart,
    COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) AS users_purchased,
    CASE
        WHEN COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN user_id END) = 0
        THEN 0
        ELSE ROUND(
            (
                COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN user_id END)
                - COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END)
            )::numeric
            / COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN user_id END)::numeric
            * 100,
            2
        )
    END AS cart_abandonment_rate
FROM customer_events
{window_sql};
""", engine)

total_events = int(total_events_df["total_events"].iloc[0])
conversion_rate = float(conversion_df["conversion_rate"].fillna(0).iloc[0])
cart_abandonment_rate = float(cart_abandonment_df["cart_abandonment_rate"].fillna(0).iloc[0])

col1, col2, col3 = st.columns(3)

col1.metric("Total Events", total_events)
col2.metric("Conversion Rate", f"{conversion_rate}%")
col3.metric("Cart Abandonment Rate", f"{cart_abandonment_rate}%")

st.divider()

render_aggregated_metrics(engine, window_sql, extra_condition)

st.divider()

render_live_events(engine, window_sql)