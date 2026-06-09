import os

import pandas as pd
import snowflake.connector
import streamlit as st


st.set_page_config(
    page_title="Synthetic Sales Analytics Dashboard",
    page_icon="📊",
    layout="wide",
)


def get_config_value(key: str, default: str | None = None) -> str | None:
    """
    Read config from Streamlit secrets first, then environment variables.

    This supports both:
    - Local Streamlit secrets: dashboard/.streamlit/secrets.toml
    - Environment variables: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, etc.
    """

    try:
        if key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass

    return os.getenv(key, default)


def get_snowflake_connection():
    """
    Create a Snowflake connection using Streamlit secrets or environment variables.
    """

    return snowflake.connector.connect(
        account=get_config_value("SNOWFLAKE_ACCOUNT"),
        user=get_config_value("SNOWFLAKE_USER"),
        password=get_config_value("SNOWFLAKE_PASSWORD"),
        role=get_config_value("SNOWFLAKE_ROLE", "SYSADMIN"),
        warehouse=get_config_value("SNOWFLAKE_WAREHOUSE", "WH_DBT_DEV"),
        database=get_config_value("SNOWFLAKE_DATABASE", "SYNTHETIC_DATA"),
        schema="MARTS",
    )


def run_query(query: str) -> pd.DataFrame:
    """
    Run a Snowflake query and return a pandas DataFrame.
    """

    conn = get_snowflake_connection()

    try:
        return pd.read_sql(query, conn)
    finally:
        conn.close()


@st.cache_data(ttl=300)
def load_daily_kpi() -> pd.DataFrame:
    query = """
        SELECT *
        FROM SYNTHETIC_DATA.MARTS.MART_SALES_DAILY_KPI
        ORDER BY ORDER_DATE
    """

    try:
        return run_query(query)
    except Exception as error:
        st.error("Unable to load daily KPI data.")
        st.code(str(error))
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_customer_kpi() -> pd.DataFrame:
    query = """
        SELECT *
        FROM SYNTHETIC_DATA.MARTS.MART_CUSTOMER_SALES_KPI
        ORDER BY COMPLETED_NET_SALES_AMOUNT DESC
        LIMIT 100
    """

    try:
        return run_query(query)
    except Exception as error:
        st.warning("Customer KPI data is not available yet.")
        st.code(str(error))
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_product_category_kpi() -> pd.DataFrame:
    query = """
        SELECT *
        FROM SYNTHETIC_DATA.MARTS.MART_PRODUCT_CATEGORY_SALES_KPI
        ORDER BY COMPLETED_NET_SALES_AMOUNT DESC
    """

    try:
        df = run_query(query)

        if "DISTINCT_PRODUCT_COUNT" in df.columns and "DISTINCT_PRODUCTS" not in df.columns:
            df = df.rename(columns={"DISTINCT_PRODUCT_COUNT": "DISTINCT_PRODUCTS"})

        return df

    except Exception as error:
        st.warning(
            "Product category KPI data is not available yet. "
            "Run dbt build for mart_product_category_sales_kpi first."
        )
        st.code(str(error))
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_audit() -> pd.DataFrame:
    query = """
        SELECT *
        FROM SYNTHETIC_DATA.MARTS.MART_LOAD_AUDIT
        ORDER BY LAST_LOADED_AT DESC
    """

    try:
        return run_query(query)
    except Exception as error:
        st.warning("Load audit data is not available yet.")
        st.code(str(error))
        return pd.DataFrame()


st.title("Synthetic Sales Analytics Dashboard")
st.caption(
    "End-to-end ELT dashboard powered by Python, AWS S3, Snowpipe, Snowflake, dbt, and Streamlit."
)

with st.sidebar:
    st.header("Dashboard filters")
    st.caption("Data is loaded from Snowflake mart tables built by dbt.")

daily_df = load_daily_kpi()
customer_df = load_customer_kpi()
product_category_df = load_product_category_kpi()
audit_df = load_audit()

if daily_df.empty:
    st.warning(
        "No daily sales KPI data available yet. "
        "Run dbt build for MART_SALES_DAILY_KPI first."
    )
    st.stop()

required_daily_columns = [
    "ORDER_DATE",
    "TOTAL_ORDERS",
    "COMPLETED_ORDERS",
    "NET_SALES_AMOUNT",
    "AVG_ORDER_VALUE",
]

missing_daily_columns = [
    column for column in required_daily_columns if column not in daily_df.columns
]

if missing_daily_columns:
    st.error("Daily KPI table is missing required columns.")
    st.write(missing_daily_columns)
    st.stop()

daily_df["ORDER_DATE"] = pd.to_datetime(daily_df["ORDER_DATE"])

min_date = daily_df["ORDER_DATE"].min().date()
max_date = daily_df["ORDER_DATE"].max().date()

with st.sidebar:
    selected_date_range = st.date_input(
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
    ].copy()
else:
    filtered_daily_df = daily_df.copy()

if "MONTH_NAME" in filtered_daily_df.columns:
    month_sort_df = (
        filtered_daily_df[["MONTH_NAME"]]
        .dropna()
        .drop_duplicates()
        .sort_values("MONTH_NAME")
    )

    available_months = month_sort_df["MONTH_NAME"].tolist()

    with st.sidebar:
        selected_months = st.multiselect(
            "Filter by month",
            options=available_months,
            default=available_months,
        )

    if selected_months:
        filtered_daily_df = filtered_daily_df[
            filtered_daily_df["MONTH_NAME"].isin(selected_months)
        ]

if filtered_daily_df.empty:
    st.warning("No data available for the selected filters.")
    st.stop()

total_orders = int(filtered_daily_df["TOTAL_ORDERS"].sum())
completed_orders = int(filtered_daily_df["COMPLETED_ORDERS"].sum())
net_sales = float(filtered_daily_df["NET_SALES_AMOUNT"].sum())
avg_order_value = float(filtered_daily_df["AVG_ORDER_VALUE"].mean())

if "UNIQUE_CUSTOMERS" in filtered_daily_df.columns:
    unique_customer_days = int(filtered_daily_df["UNIQUE_CUSTOMERS"].sum())
else:
    unique_customer_days = 0

col1, col2, col3, col4, col5 = st.columns(5)

col1.metric("Total orders", f"{total_orders:,}")
col2.metric("Completed orders", f"{completed_orders:,}")
col3.metric("Unique customer-days", f"{unique_customer_days:,}")
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
    st.subheader("Daily order status breakdown")

    order_status_columns = [
        column
        for column in [
            "COMPLETED_ORDERS",
            "PENDING_ORDERS",
            "CANCELLED_ORDERS",
            "REFUNDED_ORDERS",
        ]
        if column in filtered_daily_df.columns
    ]

    if order_status_columns:
        st.bar_chart(
            filtered_daily_df.set_index("ORDER_DATE")[order_status_columns],
            use_container_width=True,
        )
    else:
        st.info("Order status columns are not available in the daily KPI mart.")

st.divider()

st.subheader("Daily KPI table")

display_daily_columns = [
    column
    for column in [
        "ORDER_DATE",
        "YEAR_NUMBER",
        "QUARTER_NUMBER",
        "MONTH_NAME",
        "WEEK_NUMBER",
        "DAY_OF_WEEK_NAME",
        "IS_WEEKEND",
        "TOTAL_ORDERS",
        "COMPLETED_ORDERS",
        "PENDING_ORDERS",
        "CANCELLED_ORDERS",
        "REFUNDED_ORDERS",
        "UNIQUE_CUSTOMERS",
        "GROSS_SALES_AMOUNT",
        "TOTAL_DISCOUNT_AMOUNT",
        "NET_SALES_AMOUNT",
        "AVG_ORDER_VALUE",
        "DISCOUNT_RATE",
        "LATEST_LOAD_TS",
    ]
    if column in filtered_daily_df.columns
]

st.dataframe(
    filtered_daily_df[display_daily_columns],
    use_container_width=True,
    hide_index=True,
)

st.divider()

st.subheader("Top customers by completed net sales")

if not customer_df.empty and "CUSTOMER_ID" in customer_df.columns:
    top_customers = customer_df.head(20).copy()
    top_customers["CUSTOMER_ID"] = top_customers["CUSTOMER_ID"].astype(str)

    if "COMPLETED_NET_SALES_AMOUNT" in top_customers.columns:
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

st.subheader("Product category performance")

if not product_category_df.empty and "PRODUCT_CATEGORY" in product_category_df.columns:
    category_col1, category_col2, category_col3, category_col4 = st.columns(4)

    if "COMPLETED_NET_SALES_AMOUNT" in product_category_df.columns:
        top_category = product_category_df.iloc[0]["PRODUCT_CATEGORY"]
        top_category_sales = float(
            product_category_df.iloc[0]["COMPLETED_NET_SALES_AMOUNT"]
        )
        total_category_sales = float(
            product_category_df["COMPLETED_NET_SALES_AMOUNT"].sum()
        )

        category_col1.metric("Top category", str(top_category))
        category_col2.metric("Top category sales", f"${top_category_sales:,.2f}")
        category_col3.metric("Category net sales", f"${total_category_sales:,.2f}")
    else:
        category_col1.metric("Top category", "N/A")
        category_col2.metric("Top category sales", "N/A")
        category_col3.metric("Category net sales", "N/A")

    if "DISTINCT_PRODUCTS" in product_category_df.columns:
        total_distinct_products = int(product_category_df["DISTINCT_PRODUCTS"].sum())
        category_col4.metric("Distinct products", f"{total_distinct_products:,}")
    else:
        category_col4.metric("Distinct products", "N/A")

    if "COMPLETED_NET_SALES_AMOUNT" in product_category_df.columns:
        st.bar_chart(
            product_category_df.set_index("PRODUCT_CATEGORY")[
                "COMPLETED_NET_SALES_AMOUNT"
            ],
            use_container_width=True,
        )

    st.dataframe(
        product_category_df,
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info(
        "No product category KPI data available yet. "
        "Run dbt build for MART_PRODUCT_CATEGORY_SALES_KPI."
    )

st.divider()

st.subheader("Load audit and reconciliation")

if not audit_df.empty and "RECONCILIATION_STATUS" in audit_df.columns:
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

st.divider()

st.caption(
    "Dashboard source: Snowflake MARTS schema. "
    "Transformations and tests are managed by dbt."
)