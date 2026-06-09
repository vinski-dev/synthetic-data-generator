import argparse
from datetime import datetime, timedelta

from pipeline.sales_pipeline import (
    generate_sales_data_for_business_date,
    save_backfill_to_csv,
    upload_backfill_to_s3,
    validate_sales_data,
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate and upload synthetic sales backfill data."
    )

    parser.add_argument(
        "--start-date",
        required=True,
        help="Backfill start date in YYYY-MM-DD format.",
    )

    parser.add_argument(
        "--end-date",
        required=True,
        help="Backfill end date in YYYY-MM-DD format.",
    )

    parser.add_argument(
        "--records-per-day",
        type=int,
        default=5000,
        help="Number of synthetic sales records to generate per business date.",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()

    if end_date < start_date:
        raise ValueError("end-date must be greater than or equal to start-date.")

    current_date = start_date
    uploaded_files = []

    print("Starting sales backfill workflow...")
    print(f"Backfill date range: {start_date} to {end_date}")
    print(f"Records per day: {args.records_per_day:,}")

    while current_date <= end_date:
        print(f"Processing business date: {current_date}")

        df = generate_sales_data_for_business_date(
            business_date=current_date,
            num_records=args.records_per_day,
        )

        print(f"Generated records for {current_date}: {len(df):,}")
        print(f"Min order timestamp: {df['order_timestamp'].min()}")
        print(f"Max order timestamp: {df['order_timestamp'].max()}")

        validate_sales_data(df)

        local_path = save_backfill_to_csv(df, current_date)

        s3_uri = upload_backfill_to_s3(local_path, current_date)

        uploaded_files.append(s3_uri)

        current_date += timedelta(days=1)

    print("Backfill workflow completed successfully.")
    print("Uploaded files:")

    for s3_uri in uploaded_files:
        print(f"- {s3_uri}")


if __name__ == "__main__":
    main()