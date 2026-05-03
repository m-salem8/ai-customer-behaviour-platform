import streamlit as st
import psycopg2
import pandas as pd
from streamlit_autorefresh import st_autorefresh

st_autorefresh(interval=5000, key="datarefresh")  # refresh every 5 seconds

conn = psycopg2.connect(
    host="postgres",
    database="customer_db",
    user="user",
    password="password"
)

query = """
SELECT event_type, COUNT(*) AS event_count
FROM customer_events
GROUP BY event_type;
"""

df = pd.read_sql(query, conn)

st.title("Customer Events Dashboard")
st.bar_chart(df.set_index("event_type"))