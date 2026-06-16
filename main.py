from config import NUM_RECORDS
from pipeline.sales_pipeline import (
    generate_sales_data,
    save_schema_manifest,
    save_to_csv,
    upload_file_to_s3,
    validate_sales_data,
    validate_schema_contract,
)


def main() -> None:
    print("Starting synthetic sales data generation...")

    # 1. Generate DataFrame
    df = generate_sales_data(NUM_RECORDS)

    print(f"Generated records: {len(df):,}")

    # 2. Validate DataFrame quality
    validate_sales_data(df)

    # 3. Validate schema contract while still in DataFrame form
    schema_validation_result = validate_schema_contract(df)

    # 4. Save DataFrame to local CSV
    local_path = save_to_csv(df)

    # 5. Save schema manifest beside CSV
    save_schema_manifest(
        local_path=local_path,
        schema_validation_result=schema_validation_result,
    )

    # 6. Upload CSV to S3 with checksum validation
    s3_uri = upload_file_to_s3(local_path)

    print("Process completed successfully.")
    print(f"Final S3 location: {s3_uri}")


if __name__ == "__main__":
    main()