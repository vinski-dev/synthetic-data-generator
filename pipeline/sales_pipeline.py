import os
import uuid
from datetime import datetime, timezone

import boto3
import numpy as np
import pandas as pd
from botocore.exceptions import ClientError

from config import BUCKET_NAME, NUM_RECORDS, OUTPUT_PREFIX, REGION
from main import upload_to_s3

def generate_sales_data(num_records: int = NUM_RECORDS) -> str:
    """
    Generate synthetic sales data and save it locally.
    Returns the local CSV path.
    """
    

    os.makedirs("output", exist_ok=True)

    np.random.seed(42)

    product_categories = [
        "Electronics",
        "Clothing",
        "Home",
        "Sports",
        "Beauty",
        "Books",
        "Toys",
    ]

    payment_methods = [
        "Credit Card",
        "Debit Card",
        "GCash",
        "Maya",
        "Bank Transfer",
        "Cash",
    ]

    order_statuses = [
        "Completed",
        "Pending",
        "Cancelled",
        "Refunded",
    ]

    start_date = pd.Timestamp("2024-01-01")
    end_date = pd.Timestamp("2026-06-01")

    random_dates = pd.to_datetime(
        np.random.randint(
            start_date.value // 10**9,
            end_date.value // 10**9,
            size=num_records,
        ),
        unit="s",
    )

    quantity = np.random.randint(1, 10, size=num_records)
    unit_price = np.round(np.random.uniform(50, 5000, size=num_records), 2)
    discount_amount = np.round(np.random.uniform(0, 500, size=num_records), 2)

    gross_amount = quantity * unit_price
    net_amount = np.maximum(gross_amount - discount_amount, 0)

    batch_id = str(uuid.uuid4())
    generated_at = datetime.now(timezone.utc).isoformat()

    df = pd.DataFrame(
        {
            "order_id": np.arange(1, num_records + 1),
            "customer_id": np.random.randint(1000, 9999, size=num_records),
            "product_id": np.random.randint(100, 999, size=num_records),
            "product_category": np.random.choice(product_categories, size=num_records),
            "order_timestamp": random_dates,
            "quantity": quantity,
            "unit_price": unit_price,
            "gross_amount": np.round(gross_amount, 2),
            "discount_amount": discount_amount,
            "net_amount": np.round(net_amount, 2),
            "payment_method": np.random.choice(payment_methods, size=num_records),
            "order_status": np.random.choice(
                order_statuses,
                size=num_records,
                p=[0.75, 0.12, 0.08, 0.05],
            ),
            "batch_id": batch_id,
            "generated_at_utc": generated_at,
        }
    )

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    local_path = f"output/{OUTPUT_PREFIX}_{timestamp}.csv"

    df.to_csv(local_path, index=False)

    print(f"Generated {len(df):,} records.")
    print(f"Local file created: {local_path}")

    return local_path

def validate_sales_file(local_path: str) -> str:
    """
    Validate generated sales CSV file before upload.
    Returns the same local path if validation passes.
    """

    if not os.path.exists(local_path):
        raise FileNotFoundError(f"File does not exist: {local_path}")

    df = pd.read_csv(local_path)

    required_columns = [
        "order_id",
        "customer_id",
        "product_id",
        "product_category",
        "order_timestamp",
        "quantity",
        "unit_price",
        "gross_amount",
        "discount_amount",
        "net_amount",
        "payment_method",
        "order_status",
        "batch_id",
        "generated_at_utc",
    ]

    missing_columns = set(required_columns) - set(df.columns)

    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    if df.empty:
        raise ValueError("CSV file is empty.")

    if df["order_id"].duplicated().any():
        raise ValueError("Duplicate order_id found.")

    if df["quantity"].le(0).any():
        raise ValueError("Invalid quantity found.")

    if df["net_amount"].lt(0).any():
        raise ValueError("Negative net_amount found.")

    print("Data validation passed.")

    return local_path

def save_to_csv(df: pd.DataFrame) -> str:
    """
    Save generated sales data to the local output folder.
    Returns the local CSV file path.
    """

    os.makedirs("output", exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_name = f"{OUTPUT_PREFIX}_{timestamp}.csv"
    local_path = os.path.join("output", file_name)

    df.to_csv(local_path, index=False)

    print(f"CSV file created: {local_path}")

    return local_path

def upload_file_to_s3(local_path: str) -> str:


    """
    Upload generated sales CSV to S3.
    Returns the final S3 URI.
    """
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"File does not exist: {local_path}")

    s3_client = boto3.client("s3", region_name=REGION)

    file_name = os.path.basename(local_path)

    today = datetime.now(timezone.utc)

    s3_key = (
        f"{OUTPUT_PREFIX}/raw/"
        f"year={today.year}/"
        f"month={today.month:02d}/"
        f"day={today.day:02d}/"
        f"{file_name}"
    )

    try:
        s3_client.upload_file(
            Filename=local_path,
            Bucket=BUCKET_NAME,
            Key=s3_key,
            ExtraArgs={"ContentType": "text/csv"},
        )

    except ClientError as error:
        raise RuntimeError(
            f"Failed to upload file to S3 bucket={BUCKET_NAME}, key={s3_key}: {error}"
        ) from error

    s3_uri = f"s3://{BUCKET_NAME}/{s3_key}"

    print(f"Uploaded file to: {s3_uri}")

    return s3_uri

def generate_late_arriving_sales_data(
        
    num_records: int = 5000,
    min_late_days: int = 7,
    max_late_days: int = 30,
) -> pd.DataFrame:
    """
    Generate late-arriving sales data.

    The file is generated and loaded today, but the order_timestamp values
    are intentionally backdated to simulate late-arriving business events.
    """

    np.random.seed()

    product_categories = [
        "Electronics",
        "Clothing",
        "Home",
        "Sports",
        "Beauty",
        "Books",
        "Toys",
    ]

    payment_methods = [
        "Credit Card",
        "Debit Card",
        "GCash",
        "Maya",
        "Bank Transfer",
        "Cash",
    ]

    order_statuses = [
        "Completed",
        "Pending",
        "Cancelled",
        "Refunded",
    ]

    now = datetime.now(timezone.utc)

    start_date = pd.Timestamp(now.date()) - pd.Timedelta(days=max_late_days)
    end_date = pd.Timestamp(now.date()) - pd.Timedelta(days=min_late_days)

    random_dates = pd.to_datetime(
        np.random.randint(
            start_date.value // 10**9,
            end_date.value // 10**9,
            size=num_records,
        ),
        unit="s",
    )

    quantity = np.random.randint(1, 10, size=num_records)
    unit_price = np.round(np.random.uniform(50, 5000, size=num_records), 2)
    discount_amount = np.round(np.random.uniform(0, 500, size=num_records), 2)

    gross_amount = quantity * unit_price
    net_amount = np.maximum(gross_amount - discount_amount, 0)

    batch_id = f"late_{uuid.uuid4()}"
    generated_at = datetime.now(timezone.utc).isoformat()

    df = pd.DataFrame(
        {
            "order_id": np.arange(900000001, 900000001 + num_records),
            "customer_id": np.random.randint(1000, 9999, size=num_records),
            "product_id": np.random.randint(100, 999, size=num_records),
            "product_category": np.random.choice(product_categories, size=num_records),
            "order_timestamp": random_dates,
            "quantity": quantity,
            "unit_price": unit_price,
            "gross_amount": np.round(gross_amount, 2),
            "discount_amount": discount_amount,
            "net_amount": np.round(net_amount, 2),
            "payment_method": np.random.choice(payment_methods, size=num_records),
            "order_status": np.random.choice(
                order_statuses,
                size=num_records,
                p=[0.75, 0.12, 0.08, 0.05],
            ),
            "batch_id": batch_id,
            "generated_at_utc": generated_at,
        }
    )

    return df

def save_late_arriving_to_csv(df: pd.DataFrame) -> str:

    """
    Save late-arriving sales data to local output folder.
    """

    os.makedirs("output", exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_name = f"{OUTPUT_PREFIX}_late_arriving_{timestamp}.csv"
    local_path = os.path.join("output", file_name)

    df.to_csv(local_path, index=False)

    print(f"Late-arriving CSV file created: {local_path}")

    return local_path

def upload_file_to_s3(local_path: str) -> str:
    """
    Airflow-compatible wrapper.
    """

    return upload_to_s3(local_path)