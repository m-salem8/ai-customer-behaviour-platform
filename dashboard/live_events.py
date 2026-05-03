import pandas as pd
import streamlit as st


def render_live_events(engine, window_sql: str):
    st.header("Live Events")

    latest_events_df = pd.read_sql(f"""
    SELECT user_id, event_type, product_id, event_time
    FROM customer_events
    {window_sql}
    ORDER BY event_time DESC
    LIMIT 20;
    """, engine)

    st.dataframe(latest_events_df, width="stretch")