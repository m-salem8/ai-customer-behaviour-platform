import pandas as pd
import streamlit as st


def render_aggregated_metrics(engine, window_sql: str, extra_condition: str):
    st.header("Aggregated Metrics")

    event_counts_df = pd.read_sql(f"""
    SELECT event_type, COUNT(*) AS event_count
    FROM customer_events
    {window_sql}
    GROUP BY event_type;
    """, engine)

    top_users_df = pd.read_sql(f"""
    SELECT user_id, COUNT(*) AS activity_count
    FROM customer_events
    {window_sql}
    GROUP BY user_id
    ORDER BY activity_count DESC
    LIMIT 10;
    """, engine)

    top_products_df = pd.read_sql(f"""
    SELECT product_id, COUNT(*) AS interaction_count
    FROM customer_events
    {window_sql}
    GROUP BY product_id
    ORDER BY interaction_count DESC
    LIMIT 10;
    """, engine)

    top_viewed_products_df = pd.read_sql(f"""
    SELECT product_id, COUNT(*) AS views
    FROM customer_events
    WHERE event_type = 'product_view'
    {extra_condition}
    GROUP BY product_id
    ORDER BY views DESC
    LIMIT 10;
    """, engine)

    left, right = st.columns(2)

    with left:
        st.subheader("Events by Type")
        st.bar_chart(event_counts_df.set_index("event_type"))

    with right:
        st.subheader("Top Active Users")
        st.bar_chart(top_users_df.set_index("user_id"))

    left, right = st.columns(2)

    with left:
        st.subheader("Top Products by All Interactions")
        st.bar_chart(top_products_df.set_index("product_id"))

    with right:
        st.subheader("Top Viewed Products")
        st.bar_chart(top_viewed_products_df.set_index("product_id"))