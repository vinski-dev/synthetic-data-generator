from config import NUM_RECORDS
from pipeline.sales_pipeline import (
    generate_sales_data,
    upload_file_to_s3,
    validate_sales_file,
)


def main() -> None:
    print("Starting synthetic sales data generation...")

    local_path = generate_sales_data(NUM_RECORDS)

    validate_sales_file(local_path)

    s3_uri = upload_file_to_s3(local_path)

    print("Process completed successfully.")
    print(f"Final S3 location: {s3_uri}")


if __name__ == "__main__":
    main()