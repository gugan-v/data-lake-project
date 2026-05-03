import pandas as pd
import os

path = "/home/ubuntu/dashboard-data"

files = [
    os.path.join(path, f)
    for f in os.listdir(path)
    if f.endswith(".parquet")
]

df = pd.concat([pd.read_parquet(f) for f in files])

print("Total Revenue:", df["amount"].sum())

print("\nRevenue by Country:")
print(df.groupby("country")["amount"].sum())

print("\nOrder Count by Status:")
print(df["status"].value_counts())
