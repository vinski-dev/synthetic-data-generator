import os
import uuid
from datetime import datetime, timezone

import boto3
import numpy as np
import pandas as pd
from botocore.exceptions import ClientError

from config import BUCKET_NAME, NUM_RECORDS, OUTPUT_PREFIX, REGION


def generate_sales_data(num_records: int) -> pd.DataFrame:
    """
    Generate synthetic sales data using vectorized pandas/numpy logic.
    This is faster and cleaner than generating row-by-row.
    """

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

    return df


def validate_sales_data(df: pd.DataFrame) -> None:
    """
    Basic data quality checks before writing/uploading.
    """

    if df.empty:
        raise ValueError("Generated dataframe is empty.")

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

    if df["order_id"].duplicated().any():
        raise ValueError("Duplicate order_id found.")

    if df["net_amount"].lt(0).any():
        raise ValueError("Negative net_amount found.")

    if df["quantity"].le(0).any():
        raise ValueError("Invalid quantity found.")

    print("Data validation passed.")

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

def upload_to_s3(local_path: str) -> str:
    """
    Upload local CSV file to S3.
    """

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
        s3_client.upload_file(local_path, BUCKET_NAME, s3_key)

    except ClientError as error:
        raise RuntimeError(f"Failed to upload file to S3: {error}") from error

    s3_uri = f"s3://{BUCKET_NAME}/{s3_key}"

    print(f"Uploaded to S3: {s3_uri}")

    return s3_uri


def main():
    print("Starting synthetic sales data generation...")

    df = generate_sales_data(NUM_RECORDS)

    print(f"Generated records: {len(df):,}")

    validate_sales_data(df)

    local_path = save_to_csv(df)

    s3_uri = upload_to_s3(local_path)

    print("Process completed successfully.")
    print(f"Final S3 location: {s3_uri}")


if __name__ == "__main__":
    main()