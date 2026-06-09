from main import validate_sales_data
from pipeline.sales_pipeline import (
    generate_late_arriving_sales_data,
    validate_sales_file,
    save_late_arriving_to_csv,
    upload_to_s3,
)


def main():
    print("Starting late-arriving sales data simulation...")

    df = generate_late_arriving_sales_data(
        num_records=5000,
        min_late_days=7,
        max_late_days=30,
    )

    print(f"Generated late-arriving records: {len(df):,}")
    print(f"Min order timestamp: {df['order_timestamp'].min()}")
    print(f"Max order timestamp: {df['order_timestamp'].max()}")

    validate_sales_data(df)

    local_path = save_late_arriving_to_csv(df)

    s3_uri = upload_to_s3(local_path)

    print("Late-arriving data simulation completed successfully.")
    print(f"Final S3 location: {s3_uri}")


if __name__ == "__main__":
    main()