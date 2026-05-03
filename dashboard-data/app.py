import streamlit as st
import pandas as pd
import plotly.express as px
import glob

# Title
st.title("📊 Data Lake Dashboard")

# Load all parquet files
files = glob.glob("*.parquet")

if not files:
    st.error("❌ No parquet files found. Check your folder path.")
else:
    df_list = [pd.read_parquet(f) for f in files]
    df = pd.concat(df_list, ignore_index=True)
    st.success("✅ Data loaded successfully")
    st.write(df.head())

# Show raw data
st.subheader("Raw Data")
st.dataframe(df)

# Show columns
st.write("Columns:", df.columns)

# Example visualizations (change column names based on your data)

# 1. Bar Chart
if len(df.columns) >= 2:
    col1 = df.columns[0]
    col2 = df.columns[1]

    st.subheader("Bar Chart")
    fig = px.bar(df, x=col1, y=col2)
    st.plotly_chart(fig)

# 2. Line Chart
st.subheader("Line Chart")
st.line_chart(df.select_dtypes(include='number'))

# 3. Pie Chart (if category exists)
for col in df.columns:
    if df[col].dtype == 'object':
        st.subheader(f"Pie Chart - {col}")
        pie_data = df[col].value_counts().reset_index()
        pie_data.columns = [col, "count"]

        fig = px.pie(pie_data, names=col, values="count")
        st.plotly_chart(fig)
        break
