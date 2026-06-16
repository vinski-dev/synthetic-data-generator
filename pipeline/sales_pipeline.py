import os
import hashlib
import json
import uuid
from datetime import datetime, timezone, date

import boto3
import numpy as np
import pandas as pd
from botocore.exceptions import ClientError

from config import BUCKET_NAME, NUM_RECORDS, OUTPUT_PREFIX, REGION


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



def generate_sales_data_for_business_date(
    business_date: date,
    num_records: int = 5000,
) -> pd.DataFrame:
    """
    Generate synthetic sales data for a specific historical business date.

    This is used for backfill scenarios where the file is loaded today,
    but the order_timestamp belongs to an older reporting date.
    """

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

    start_datetime = pd.Timestamp(business_date)
    end_datetime = start_datetime + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

    random_dates = pd.to_datetime(
        np.random.randint(
            start_datetime.value // 10**9,
            end_datetime.value // 10**9,
            size=num_records,
        ),
        unit="s",
    )

    quantity = np.random.randint(1, 10, size=num_records)
    unit_price = np.round(np.random.uniform(50, 5000, size=num_records), 2)
    discount_amount = np.round(np.random.uniform(0, 500, size=num_records), 2)

    gross_amount = quantity * unit_price
    net_amount = np.maximum(gross_amount - discount_amount, 0)

    business_date_key = int(business_date.strftime("%Y%m%d"))
    order_id_start = business_date_key * 1_000_000

    batch_id = f"backfill_{business_date.strftime('%Y%m%d')}_{uuid.uuid4()}"
    generated_at = datetime.now(timezone.utc).isoformat()

    df = pd.DataFrame(
        {
            "order_id": np.arange(order_id_start + 1, order_id_start + num_records + 1),
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
    Validate sales data in memory before saving or uploading.
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



def save_backfill_to_csv(df: pd.DataFrame, business_date: date) -> str:
    """
    Save backfill sales data to the local output folder.
    """

    os.makedirs("output", exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    business_date_key = business_date.strftime("%Y%m%d")

    file_name = f"{OUTPUT_PREFIX}_backfill_{business_date_key}_{timestamp}.csv"
    local_path = os.path.join("output", file_name)

    df.to_csv(local_path, index=False)

    print(f"Backfill CSV file created: {local_path}")

    return local_path



def calculate_file_sha256(local_path: str) -> str:
    """
    Calculate SHA-256 checksum for a local file.
    Used to verify that uploaded files were not corrupted or altered.
    """

    if not os.path.exists(local_path):
        raise FileNotFoundError(f"File does not exist: {local_path}")

    sha256_hash = hashlib.sha256()

    with open(local_path, "rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            sha256_hash.update(chunk)

    return sha256_hash.hexdigest()



def get_csv_row_count(local_path: str) -> int:
    """
    Count data rows in a CSV file, excluding the header.
    """

    if not os.path.exists(local_path):
        raise FileNotFoundError(f"File does not exist: {local_path}")

    with open(local_path, "r", encoding="utf-8") as file:
        line_count = sum(1 for _ in file)

    return max(line_count - 1, 0)



def calculate_s3_object_sha256(
    s3_client,
    bucket_name: str,
    s3_key: str,
) -> str:
    """
    Stream an S3 object and calculate its SHA-256 checksum.
    This verifies the uploaded object content.
    """

    sha256_hash = hashlib.sha256()

    response = s3_client.get_object(
        Bucket=bucket_name,
        Key=s3_key,
    )

    body = response["Body"]

    for chunk in iter(lambda: body.read(1024 * 1024), b""):
        sha256_hash.update(chunk)

    return sha256_hash.hexdigest()



def save_checksum_manifest(
    local_path: str,
    s3_uri: str,
    s3_key: str,
    checksum_sha256: str,
) -> str:
    """
    Save a local checksum manifest JSON file beside the generated CSV.
    """

    file_size_bytes = os.path.getsize(local_path)
    row_count = get_csv_row_count(local_path)

    manifest = {
        "file_name": os.path.basename(local_path),
        "s3_uri": s3_uri,
        "s3_key": s3_key,
        "checksum_algorithm": "SHA-256",
        "checksum_sha256": checksum_sha256,
        "file_size_bytes": file_size_bytes,
        "row_count": row_count,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    manifest_path = f"{local_path}.manifest.json"

    with open(manifest_path, "w", encoding="utf-8") as file:
        json.dump(manifest, file, indent=2)

    print(f"Checksum manifest created: {manifest_path}")

    return manifest_path


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



def upload_backfill_to_s3(local_path: str, business_date: date) -> str:
    """
    Upload a backfill CSV file to S3 under sales/raw/backfill/.
    Returns the final S3 URI.
    """

    if not os.path.exists(local_path):
        raise FileNotFoundError(f"File does not exist: {local_path}")

    s3_client = boto3.client("s3", region_name=REGION)

    file_name = os.path.basename(local_path)
    business_date_key = business_date.strftime("%Y-%m-%d")

    s3_key = (
        f"{OUTPUT_PREFIX}/raw/backfill/business_date={business_date_key}/{file_name}"
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
            f"Failed to upload backfill file to S3 bucket={BUCKET_NAME}, key={s3_key}: {error}"
        ) from error

    s3_uri = f"s3://{BUCKET_NAME}/{s3_key}"

    print(f"Uploaded backfill file to: {s3_uri}")

    return s3_uri
