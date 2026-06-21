import pandas as pd

df = pd.read_parquet("data/bronze/customer_events")

print(df.columns)
print(df.head())