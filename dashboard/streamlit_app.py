import os

import pandas as pd
import snowflake.connector
import streamlit as st


st.set_page_config(
    page_title="Synthetic Sales Analytics Dashboard",
    page_icon="📊",
    layout="wide",
)


def get_snowflake_connection():
    """
    Create Snowflake connection using Streamlit secrets first,
    then fallback to environment variables.
    """

    return snowflake.connector.connect(
        account=st.secrets.get("SNOWFLAKE_ACCOUNT", os.getenv("SNOWFLAKE_ACCOUNT")),
        user=st.secrets.get("SNOWFLAKE_USER", os.getenv("SNOWFLAKE_USER")),
        password=st.secrets.get("SNOWFLAKE_PASSWORD", os.getenv("SNOWFLAKE_PASSWORD")),
        role=st.secrets.get("SNOWFLAKE_ROLE", os.getenv("SNOWFLAKE_ROLE", "SYSADMIN")),
        warehouse=st.secrets.get(
            "SNOWFLAKE_WAREHOUSE",
            os.getenv("SNOWFLAKE_WAREHOUSE", "WH_DBT_DEV"),
        ),
        database=st.secrets.get(
            "SNOWFLAKE_DATABASE",
            os.getenv("SNOWFLAKE_DATABASE", "SYNTHETIC_DATA"),
        ),
        schema="MARTS",
    )


@st.cache_data(ttl=300)
def load_daily_kpi() -> pd.DataFrame:
    query = """
        SELECT
            ORDER_DATE,
            TOTAL_ORDERS,
            COMPLETED_ORDERS,
            PENDING_ORDERS,
            CANCELLED_ORDERS,
            REFUNDED_ORDERS,
            UNIQUE_CUSTOMERS,
            GROSS_SALES_AMOUNT,
            TOTAL_DISCOUNT_AMOUNT,
            NET_SALES_AMOUNT,
            AVG_ORDER_VALUE,
            DISCOUNT_RATE,
            LATEST_LOAD_TS
        FROM SYNTHETIC_DATA.MARTS.MART_SALES_DAILY_KPI
        ORDER BY ORDER_DATE
    """

    with get_snowflake_connection() as conn:
        return pd.read_sql(query, conn)


@st.cache_data(ttl=300)
def load_customer_kpi() -> pd.DataFrame:
    query = """
        SELECT
            CUSTOMER_ID,
            TOTAL_ORDERS,
            COMPLETED_ORDERS,
            FAILED_OR_REVERSED_ORDERS,
            FIRST_ORDER_DATE,
            LATEST_ORDER_DATE,
            COMPLETED_NET_SALES_AMOUNT,
            AVG_COMPLETED_ORDER_AMOUNT,
            DBT_UPDATED_AT
        FROM SYNTHETIC_DATA.MARTS.MART_CUSTOMER_SALES_KPI
        ORDER BY COMPLETED_NET_SALES_AMOUNT DESC
        LIMIT 100
    """

    with get_snowflake_connection() as conn:
        return pd.read_sql(query, conn)


@st.cache_data(ttl=300)
def load_audit() -> pd.DataFrame:
    query = """
        SELECT
            SOURCE_FILE_NAME,
            RAW_ROW_COUNT,
            FACT_ROW_COUNT,
            ROW_COUNT_DIFFERENCE,
            FIRST_LOADED_AT,
            LAST_LOADED_AT,
            FIRST_FACT_LOADED_AT,
            LAST_FACT_LOADED_AT,
            RECONCILIATION_STATUS
        FROM SYNTHETIC_DATA.MARTS.MART_LOAD_AUDIT
        ORDER BY LAST_LOADED_AT DESC
    """

    with get_snowflake_connection() as conn:
        return pd.read_sql(query, conn)


st.title("Synthetic Sales Analytics Dashboard")
st.caption(
    "End-to-end ELT dashboard powered by Python, S3, Snowpipe, Snowflake, dbt, and Streamlit."
)

daily_df = load_daily_kpi()
customer_df = load_customer_kpi()
audit_df = load_audit()

if daily_df.empty:
    st.warning("No sales KPI data available yet.")
    st.stop()

daily_df["ORDER_DATE"] = pd.to_datetime(daily_df["ORDER_DATE"])

min_date = daily_df["ORDER_DATE"].min().date()
max_date = daily_df["ORDER_DATE"].max().date()

selected_date_range = st.sidebar.date_input(
    "Select order date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

if isinstance(selected_date_range, tuple) and len(selected_date_range) == 2:
    start_date, end_date = selected_date_range
    filtered_daily_df = daily_df[
        (daily_df["ORDER_DATE"].dt.date >= start_date)
        & (daily_df["ORDER_DATE"].dt.date <= end_date)
    ]
else:
    filtered_daily_df = daily_df.copy()

total_orders = int(filtered_daily_df["TOTAL_ORDERS"].sum())
completed_orders = int(filtered_daily_df["COMPLETED_ORDERS"].sum())
unique_customers = int(filtered_daily_df["UNIQUE_CUSTOMERS"].sum())
net_sales = float(filtered_daily_df["NET_SALES_AMOUNT"].sum())
avg_order_value = float(filtered_daily_df["AVG_ORDER_VALUE"].mean())

col1, col2, col3, col4, col5 = st.columns(5)

col1.metric("Total orders", f"{total_orders:,}")
col2.metric("Completed orders", f"{completed_orders:,}")
col3.metric("Unique customers", f"{unique_customers:,}")
col4.metric("Net sales", f"${net_sales:,.2f}")
col5.metric("Avg order value", f"${avg_order_value:,.2f}")

st.divider()

left_col, right_col = st.columns(2)

with left_col:
    st.subheader("Daily net sales")
    st.line_chart(
        filtered_daily_df.set_index("ORDER_DATE")["NET_SALES_AMOUNT"],
        use_container_width=True,
    )

with right_col:
    st.subheader("Daily order volume")
    st.bar_chart(
        filtered_daily_df.set_index("ORDER_DATE")[
            ["COMPLETED_ORDERS", "PENDING_ORDERS", "CANCELLED_ORDERS", "REFUNDED_ORDERS"]
        ],
        use_container_width=True,
    )

st.divider()

st.subheader("Top customers by completed net sales")

if not customer_df.empty:
    top_customers = customer_df.head(20).copy()
    top_customers["CUSTOMER_ID"] = top_customers["CUSTOMER_ID"].astype(str)

    st.bar_chart(
        top_customers.set_index("CUSTOMER_ID")["COMPLETED_NET_SALES_AMOUNT"],
        use_container_width=True,
    )

    st.dataframe(
        top_customers,
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("No customer KPI data available yet.")

st.divider()

st.subheader("Load audit and reconciliation")

if not audit_df.empty:
    matched_count = int((audit_df["RECONCILIATION_STATUS"] == "MATCHED").sum())
    mismatched_count = int((audit_df["RECONCILIATION_STATUS"] != "MATCHED").sum())

    audit_col1, audit_col2, audit_col3 = st.columns(3)
    audit_col1.metric("Files loaded", f"{len(audit_df):,}")
    audit_col2.metric("Matched files", f"{matched_count:,}")
    audit_col3.metric("Mismatched files", f"{mismatched_count:,}")

    st.dataframe(
        audit_df,
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("No load audit data available yet.")