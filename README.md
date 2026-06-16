# Synthetic Sales ELT Pipeline

![CI](https://github.com/vinski-dev/synthetic-data-generator/actions/workflows/ci.yml/badge.svg)
![dbt Deploy](https://github.com/vinski-dev/synthetic-data-generator/actions/workflows/dbt_deploy.yml/badge.svg)
![Python](https://img.shields.io/badge/Python-3.11-blue)
![dbt](https://img.shields.io/badge/dbt-Snowflake-orange)
![Snowflake](https://img.shields.io/badge/Warehouse-Snowflake-29B5E8)
![Airflow](https://img.shields.io/badge/Orchestration-Airflow-017CEE)
![AWS](https://img.shields.io/badge/Storage-AWS%20S3-orange)

## Overview

This project is a production-style **synthetic sales ELT pipeline** built to demonstrate practical Data Engineering and Analytics Engineering capabilities.

The pipeline generates synthetic sales transaction data using Python, lands the data in AWS S3, auto-ingests new files into Snowflake using Snowpipe, and transforms the raw data into analytics-ready models using dbt.

It also includes Airflow orchestration, dbt tests, source freshness checks, load reconciliation, Snowflake workload isolation, and GitHub Actions CI/CD.

## Why This Project Exists

This project was built as a portfolio-grade data engineering implementation to demonstrate:

* Cloud-based raw data landing with AWS S3
* Event-driven ingestion into Snowflake using Snowpipe
* Modular transformation using dbt
* Layered data modeling with staging, intermediate, and marts
* Data quality checks and business reconciliation tests
* Airflow orchestration for end-to-end ELT execution
* GitHub Actions CI/CD for automated validation
* Secure credential handling using environment variables and GitHub Secrets

## Architecture

```mermaid
flowchart LR
    A[Python Synthetic Data Generator] --> B[Local CSV Output]
    B --> C[AWS S3 Raw Landing Zone]
    C --> D[S3 Event Notification]
    D --> E[Snowpipe Auto-Ingestion]
    E --> F[Snowflake RAW.SALES_RAW]
    F --> G[dbt Staging Layer]
    G --> H[dbt Intermediate Layer]
    H --> I[dbt Mart Layer]
    I --> J[Analytics-Ready KPI Tables]

    K[Airflow DAG] --> A
    K --> L[Wait for Snowpipe Load]
    L --> M[Run dbt Build]

    N[GitHub Actions CI/CD] --> O[Python Validation]
    N --> P[dbt Parse Compile Build]
```

## End-to-End Data Flow

```text
Python Generator
    ↓
CSV File Created Locally
    ↓
Upload to AWS S3
    ↓
S3 Event Notification
    ↓
Snowpipe Auto-Ingest
    ↓
Snowflake RAW Layer
    ↓
dbt Staging Layer
    ↓
dbt Intermediate Layer
    ↓
dbt Mart Layer
    ↓
Sales KPI Tables
```

## Technology Stack

| Category         | Technology           |
| ---------------- | -------------------- |
| Programming      | Python 3.11          |
| Data Generation  | pandas, numpy, faker |
| Cloud Storage    | AWS S3               |
| Ingestion        | Snowpipe             |
| Data Warehouse   | Snowflake            |
| Transformation   | dbt                  |
| Orchestration    | Apache Airflow       |
| Containerization | Docker Compose       |
| CI/CD            | GitHub Actions       |
| Version Control  | Git / GitHub         |
| Dashboard / BI   | Streamlit            |
## Business Use Case

The project simulates a sales analytics pipeline for an e-commerce or retail-style business.

The generated data includes transaction-level sales records with:

* Order ID
* Customer ID
* Product ID
* Product category
* Order timestamp
* Quantity
* Unit price
* Gross amount
* Discount amount
* Net amount
* Payment method
* Order status
* Batch ID
* Source file metadata
* Load timestamp

The transformed mart models support business reporting such as:

* Daily sales performance
* Completed vs pending vs cancelled vs refunded orders
* Net sales amount
* Average order value
* Discount rate
* Customer-level sales behavior
* File-level load reconciliation

## Default Project Configuration

The current default configuration generates synthetic sales data and uploads it to AWS S3.

```python
BUCKET_NAME = "vinski-synthetic-data-bucket"
NUM_RECORDS = 100000
OUTPUT_PREFIX = "sales"
REGION = "ap-southeast-1"
```

Generated files are written to S3 using a partition-style path:

```text
s3://vinski-synthetic-data-bucket/sales/raw/year=YYYY/month=MM/day=DD/file.csv
```

## Project Structure

```text
synthetic-data-generator/
│
├── .github/
│   └── workflows/
│       ├── ci.yml
│       └── dbt_deploy.yml
│
├── config/
│
├── dags/
│   ├── synthetic_sales_s3_dag.py
│   └── sales_backfill_dag.py
│
├── dashboard/
│   ├── streamlit_app.py
│   └── .streamlit/
│       └── secrets.toml.example
│
├── dbt_project/
│   ├── dbt_project.yml
│   ├── selectors.yml
│   ├── macros/
│   │   └── generate_schema_name.sql
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   └── tests/
│
├── pipeline/
│   ├── __init__.py
│   └── sales_pipeline.py
│
├── plugins/
│
├── backfill_sales.py
├── config.py
├── generate.py
├── late_arriving.py
├── main.py
│
├── Dockerfile
├── docker-compose.yaml
├── requirements.txt
├── requirements-dev.txt
├── .gitignore
└── README.md
```

## Data Model

The project follows a layered dbt model design.

```text
RAW.SALES_RAW
    ↓
STAGING.STG_SALES
    ↓
INTERMEDIATE.INT_SALES_ENRICHED
    ↓
MARTS.FCT_SALES
    ↓
MARTS.MART_SALES_DAILY_KPI
    ↓
MARTS.MART_CUSTOMER_SALES_KPI
    ↓
MARTS.MART_LOAD_AUDIT
```

## dbt Layering Strategy

### 1. Raw Layer

The raw layer is populated by Snowpipe.

Table:

```text
SYNTHETIC_DATA.RAW.SALES_RAW
```

Purpose:

* Store Snowpipe-loaded files from S3
* Preserve source-level records
* Capture file metadata
* Capture load timestamp
* Keep raw ingestion separate from transformation logic

### 2. Staging Layer

Model:

```text
STAGING.STG_SALES
```

Purpose:

* Standardize column names
* Cast data types
* Normalize text values
* Deduplicate records
* Create a stable transaction event key
* Preserve source metadata

Example business key strategy:

```text
sales_event_key = md5(order_id + batch_id + source_file_name)
```

This avoids treating `order_id` alone as globally unique because each generated file may restart order IDs from 1.

### 3. Intermediate Layer

Model:

```text
INTERMEDIATE.INT_SALES_ENRICHED
```

Purpose:

* Apply reusable business logic
* Calculate discount rate
* Derive order month
* Add completed order flag
* Add failed or reversed order flag
* Prepare enriched transaction data for mart models

### 4. Mart Layer

Models:

```text
MARTS.FCT_SALES
MARTS.MART_SALES_DAILY_KPI
MARTS.MART_CUSTOMER_SALES_KPI
MARTS.MART_LOAD_AUDIT
```

Purpose:

* Provide transaction-level fact data
* Provide daily sales KPI reporting
* Provide customer-level sales metrics
* Reconcile raw file row counts against fact table row counts

## Snowflake Workload Isolation

The project uses separate Snowflake warehouses to isolate development, transformation, and mart workloads.

```text
WH_DBT_DEV         → local development, dbt debug, CI validation
WH_DBT_TRANSFORM   → staging and intermediate models
WH_DBT_MARTS       → facts, dimensions, KPI marts
```

This design provides cost visibility and workload separation without over-engineering the project.

## Dimensional Models

The mart layer includes dimensional models to make the analytics layer more reusable, reporting-friendly, and closer to a traditional dimensional warehouse design.

The dimensional models are:

```text
SYNTHETIC_DATA.MARTS.DIM_DATE
SYNTHETIC_DATA.MARTS.DIM_PRODUCT_CATEGORIES
```

These models support the main sales fact and KPI marts:

```text
SYNTHETIC_DATA.MARTS.FCT_SALES
SYNTHETIC_DATA.MARTS.MART_SALES_DAILY_KPI
SYNTHETIC_DATA.MARTS.MART_CUSTOMER_SALES_KPI
SYNTHETIC_DATA.MARTS.MART_PRODUCT_CATEGORY_SALES_KPI
```

### Dimensional Model Flow

```text
RAW.SALES_RAW
    ↓
STAGING.STG_SALES
    ↓
INTERMEDIATE.INT_SALES_ENRICHED
    ↓
MARTS.FCT_SALES
    ├── MARTS.DIM_DATE
    ├── MARTS.DIM_PRODUCT_CATEGORIES
    ├── MARTS.MART_SALES_DAILY_KPI
    ├── MARTS.MART_CUSTOMER_SALES_KPI
    └── MARTS.MART_PRODUCT_CATEGORY_SALES_KPI
```

### Date Dimension

Model:

```text
dbt_project/models/marts/dim_date.sql
```

Target table:

```text
SYNTHETIC_DATA.MARTS.DIM_DATE
```

The date dimension centralizes calendar logic so reporting tools and downstream users do not need to repeatedly calculate date attributes.

Key columns include:

```text
date_day
year_number
quarter_number
month_number
month_name
week_number
day_of_month
day_of_week_number
day_of_week_name
week_start_date
month_start_date
month_end_date
quarter_start_date
quarter_end_date
year_start_date
year_end_date
is_weekend
```

This supports reporting by year, quarter, month, week, weekday, and weekend indicators.

### Product Category Dimension

Model:

```text
dbt_project/models/marts/dim_product_categories.sql
```

Target table:

```text
SYNTHETIC_DATA.MARTS.DIM_PRODUCT_CATEGORIES
```

The product category dimension provides a reusable product-category view derived from the sales fact table.

Key columns include:

```text
product_category
total_orders
distinct_product_count
first_order_date
latest_order_date
completed_net_sales_amount
avg_completed_order_amount
```

This supports category-level analytics across product groups such as Electronics, Clothing, Home, Sports, Beauty, Books, and Toys.

### Product Category KPI Mart

Model:

```text
dbt_project/models/marts/mart_product_category_sales_kpi.sql
```

Target table:

```text
SYNTHETIC_DATA.MARTS.MART_PRODUCT_CATEGORY_SALES_KPI
```

This mart provides product category-level business KPIs.

Key metrics include:

```text
total_orders
completed_orders
failed_or_reversed_orders
distinct_products
unique_customers
gross_sales_amount
total_discount_amount
net_sales_amount
completed_net_sales_amount
avg_completed_order_amount
```

### Why These Models Matter

The dimensional models improve the project by separating reusable descriptive entities from transaction-level facts.

`DIM_DATE` centralizes calendar logic and makes reporting easier across daily, weekly, monthly, quarterly, and yearly views.

`DIM_PRODUCT_CATEGORIES` provides a reusable category-level model that can be used for filtering, grouping, and analyzing sales performance by product category.

Together, these models make the mart layer more analytics-ready and closer to a production-style dimensional model.

### Validation Queries

Check the date dimension:

```sql
SELECT COUNT(*) AS date_count
FROM SYNTHETIC_DATA.MARTS.DIM_DATE;

SELECT *
FROM SYNTHETIC_DATA.MARTS.DIM_DATE
ORDER BY DATE_DAY
LIMIT 10;
```

Check the product category dimension:

```sql
SELECT *
FROM SYNTHETIC_DATA.MARTS.DIM_PRODUCT_CATEGORIES
ORDER BY COMPLETED_NET_SALES_AMOUNT DESC;
```

Check the product category KPI mart:

```sql
SELECT *
FROM SYNTHETIC_DATA.MARTS.MART_PRODUCT_CATEGORY_SALES_KPI
ORDER BY COMPLETED_NET_SALES_AMOUNT DESC;
```

### Interview Explanation

I added dimensional date and product category models to make the mart layer more analytics-ready. The date dimension centralizes reusable calendar logic such as year, quarter, month, week, day of week, and weekend flags. The product category dimension summarizes category-level behavior and supports reporting by product category.

This improves reusability because dashboards and reporting tools can join to dimensions instead of recalculating the same logic repeatedly. It also makes the project closer to a traditional dimensional model with facts, dimensions, and KPI marts.


## Data Quality Checks

The project includes dbt tests for both technical and business quality.

## File Checksum Validation for Ingestion

This project includes file-level checksum validation in the Python ingestion layer to verify that generated files are uploaded to S3 without corruption or unintended changes.

The ingestion process uses a SHA-256 checksum to compare the local CSV file against the uploaded S3 object.

### Checksum Validation Flow

```text
Generate sales DataFrame
    ↓
Validate DataFrame
    ↓
Save CSV locally in output/
    ↓
Calculate local SHA-256 checksum
    ↓
Upload CSV to S3 raw landing zone
    ↓
Create local manifest JSON
    ↓
Upload manifest JSON to S3
    ↓
Read uploaded S3 object
    ↓
Recalculate S3 object SHA-256 checksum
    ↓
Compare local checksum vs S3 checksum
    ↓
Fail ingestion if checksums do not match
```

### Local Output Files

When `main.py` runs, the pipeline creates a local copy of the generated data and its checksum manifest.

Example:

```text
output/sales_20260616_082043.csv
output/sales_20260616_082043.csv.manifest.json
```

The local CSV is useful for debugging, replay, and audit review before or after ingestion.

### S3 Landing Files

The same CSV file and its manifest are uploaded to S3.

Example:

```text
s3://vinski-synthetic-data-bucket/sales/raw/year=2026/month=06/day=16/sales_20260616_082043.csv

s3://vinski-synthetic-data-bucket/sales/raw/year=2026/month=06/day=16/sales_20260616_082043.csv.manifest.json
```

The CSV file is loaded by Snowpipe into Snowflake.

The manifest JSON file is used for file-level audit and checksum validation.

### Manifest File Contents

Each manifest JSON contains file-level metadata:

```json
{
  "file_name": "sales_20260616_082043.csv",
  "s3_uri": "s3://vinski-synthetic-data-bucket/sales/raw/year=2026/month=06/day=16/sales_20260616_082043.csv",
  "s3_key": "sales/raw/year=2026/month=06/day=16/sales_20260616_082043.csv",
  "checksum_algorithm": "SHA-256",
  "checksum_sha256": "example_checksum_value",
  "file_size_bytes": 12345678,
  "row_count": 100000,
  "created_at_utc": "2026-06-16T08:20:43+00:00"
}
```

### Python Implementation

The checksum logic is implemented in:

```text
pipeline/sales_pipeline.py
```

Key functions:

```text
calculate_file_sha256()
get_csv_row_count()
calculate_s3_object_sha256()
save_checksum_manifest()
upload_file_to_s3()
```

The `upload_file_to_s3()` function is responsible for:

```text
Calculating the local checksum
Uploading the CSV to S3
Creating the manifest JSON
Uploading the manifest JSON to S3
Reading the S3 object back
Comparing local checksum against S3 checksum
```

### Manual Run

Run the ingestion manually:

```powershell
cd C:\dev\synthetic-data-generator
.\venv311\Scripts\Activate.ps1
.\set_env.ps1

python main.py
```

Expected output includes:

```text
Local SHA-256 checksum: ...
Local file size bytes: ...
Local CSV row count: 100,000
Checksum manifest created: ...
Checksum manifest uploaded to: s3://...
S3 SHA-256 checksum: ...
Checksum validation passed. Local file matches S3 object.
```

### Snowflake Loading Behavior

Snowpipe should load only CSV files into:

```text
SYNTHETIC_DATA.RAW.SALES_RAW
```

Manifest files should not be loaded into the raw sales table because they are audit metadata, not transaction records.

The Snowpipe `COPY INTO` statement should use a CSV-only pattern:

```sql
PATTERN = '.*[.]csv$'
```

This prevents files such as the following from being loaded into `SALES_RAW`:

```text
*.manifest.json
```

To confirm manifest files were not loaded into the sales raw table:

```sql
SELECT *
FROM SYNTHETIC_DATA.RAW.SALES_RAW
WHERE SOURCE_FILE_NAME ILIKE '%manifest%';
```

Expected result:

```text
0 rows
```

### Checking Manifest Files from Snowflake

Manifest files can be checked from the external stage instead of the sales raw table.

Create a JSON file format:

```sql
CREATE OR REPLACE FILE FORMAT SYNTHETIC_DATA.RAW.JSON_FILE_FORMAT
TYPE = JSON;
```

List manifest files from the stage:

```sql
LIST @SYNTHETIC_DATA.RAW.SALES_S3_STAGE
PATTERN = '.*manifest[.]json';
```

Read manifest contents:

```sql
SELECT
    METADATA$FILENAME AS manifest_file_path,
    $1:file_name::STRING AS data_file_name,
    $1:s3_uri::STRING AS data_s3_uri,
    $1:s3_key::STRING AS data_s3_key,
    $1:checksum_algorithm::STRING AS checksum_algorithm,
    $1:checksum_sha256::STRING AS checksum_sha256,
    $1:file_size_bytes::NUMBER AS file_size_bytes,
    $1:row_count::NUMBER AS expected_row_count,
    $1:created_at_utc::TIMESTAMP_NTZ AS manifest_created_at_utc
FROM @SYNTHETIC_DATA.RAW.SALES_S3_STAGE
(
    FILE_FORMAT => SYNTHETIC_DATA.RAW.JSON_FILE_FORMAT,
    PATTERN => '.*manifest[.]json'
)
ORDER BY manifest_created_at_utc DESC;
```

### Optional File Ingestion Audit Table

For a more production-style implementation, manifest records can be loaded into a separate audit table:

```text
SYNTHETIC_DATA.RAW.FILE_INGESTION_AUDIT
```

This keeps transaction data and file-level metadata separate.

Recommended design:

```text
SALES_RAW
    → transaction-level sales records

FILE_INGESTION_AUDIT
    → file name, checksum, file size, expected row count, manifest timestamp
```

This separation keeps the raw sales table clean while preserving auditability.

### Why SHA-256 Was Used

SHA-256 is used because it provides a strong file fingerprint for validating file integrity.

The pipeline does not rely on the S3 `ETag` as the main checksum because `ETag` is not always a simple MD5 hash, especially when multipart upload or encryption is involved.

### Why This Matters

Checksum validation improves ingestion reliability by proving that:

```text
The local generated file was fingerprinted before upload
The uploaded S3 object matches the local file
A manifest exists for audit and reconciliation
The pipeline can detect file corruption or tampering
```

This makes the ingestion layer more production-ready and auditable.

### Interview Explanation

I implemented checksum validation in the ingestion layer using SHA-256. Before uploading a generated CSV to S3, the pipeline calculates the local file checksum, file size, and row count. After the upload, it reads the S3 object back and recalculates the checksum. If the local checksum and S3 checksum do not match, the ingestion fails.

The pipeline also creates a manifest JSON file beside the CSV. The manifest stores the file name, S3 path, checksum, file size, row count, and creation timestamp. Snowpipe loads only the CSV files into the raw sales table, while the manifest files are used separately for audit and reconciliation.

## Schema Evolution Strategy

This project includes a controlled schema evolution strategy for the ELT pipeline.

The goal is to allow safe source schema changes while preventing breaking changes from silently corrupting downstream Snowflake tables, dbt models, and dashboard outputs.

### Schema Evolution Flow

```text
Python ingestion generates sales DataFrame
    ↓
Data quality validation runs
    ↓
Schema contract validation runs
    ↓
CSV file is saved locally
    ↓
Schema manifest is created
    ↓
Checksum manifest is created
    ↓
CSV and manifest files are uploaded to S3
    ↓
Snowpipe loads approved CSV files into Snowflake RAW
    ↓
Snowflake validates RAW table structure
    ↓
dbt staging safely handles optional columns
    ↓
Marts expose new columns only when intentionally approved
```

### Schema Contract

The source schema contract is stored in:

```text
schema/sales_schema_contract.json
```

The schema contract defines:

```text
Dataset name
Schema version
Required columns
Optional columns
Expected simplified data types
Compatibility mode
Whether additive columns are allowed
```

Example contract structure:

```json
{
  "dataset": "sales",
  "version": "1.0.0",
  "compatibility": "backward",
  "allow_additive_columns": true,
  "required_columns": {
    "order_id": "integer",
    "customer_id": "integer",
    "product_id": "integer",
    "product_category": "string",
    "order_timestamp": "datetime",
    "quantity": "integer",
    "unit_price": "number",
    "gross_amount": "number",
    "discount_amount": "number",
    "net_amount": "number",
    "payment_method": "string",
    "order_status": "string",
    "batch_id": "string",
    "generated_at_utc": "string"
  },
  "optional_columns": {
    "sales_channel": "string",
    "promotion_code": "string",
    "device_type": "string"
  }
}
```

### Schema Evolution Rules

Allowed changes:

```text
New nullable columns
New optional business attributes
Backward-compatible additive changes
Columns added intentionally to staging or marts
```

Breaking changes:

```text
Missing required columns
Renamed required columns
Unexpected type changes
Column reordering in CSV files
Required fields becoming nullable
Removing fields used by downstream models
```

### Python Schema Validation

Schema validation is implemented in:

```text
pipeline/sales_pipeline.py
```

Key functions:

```text
load_schema_contract()
infer_dataframe_schema()
validate_schema_contract()
save_schema_manifest()
```

The recommended ingestion order is:

```text
generate_sales_data()
    ↓
validate_sales_data()
    ↓
validate_schema_contract()
    ↓
save_to_csv()
    ↓
save_schema_manifest()
    ↓
upload_file_to_s3()
```

Example `main.py` flow:

```python
df = generate_sales_data(NUM_RECORDS)

validate_sales_data(df)

schema_validation_result = validate_schema_contract(df)

local_path = save_to_csv(df)

save_schema_manifest(
    local_path=local_path,
    schema_validation_result=schema_validation_result,
)

s3_uri = upload_file_to_s3(local_path)
```

### Schema Manifest

Each pipeline run can create a schema manifest beside the CSV file.

Example local files:

```text
output/sales_20260616_082043.csv
output/sales_20260616_082043.csv.schema.json
output/sales_20260616_082043.csv.manifest.json
```

The schema manifest records:

```text
Dataset name
Contract version
Actual detected schema
Missing required columns
Additive columns
Type mismatches
Schema validation status
Creation timestamp
```

This gives the pipeline an audit trail of the schema that was validated before ingestion.

### Example Additive Column

An example safe schema evolution is adding:

```text
sales_channel
```

This column can be added as an optional source attribute.

Example values:

```text
Online
Store
Mobile App
Marketplace
```

The schema contract can allow this as an optional field:

```json
"optional_columns": {
  "sales_channel": "string"
}
```

Once approved, the raw Snowflake table can be updated:

```sql
ALTER TABLE SYNTHETIC_DATA.RAW.SALES_RAW
ADD COLUMN IF NOT EXISTS SALES_CHANNEL STRING;
```

### Snowflake Schema Validation

Snowflake can validate the actual `RAW.SALES_RAW` table structure using `INFORMATION_SCHEMA.COLUMNS`.

This helps confirm that the raw table still contains the required columns expected by the pipeline.

Example validation query:

```sql
WITH expected_schema AS (

    SELECT *
    FROM VALUES
        ('ORDER_ID', 'NUMBER', TRUE),
        ('CUSTOMER_ID', 'NUMBER', TRUE),
        ('PRODUCT_ID', 'NUMBER', TRUE),
        ('PRODUCT_CATEGORY', 'STRING', TRUE),
        ('ORDER_TIMESTAMP', 'TIMESTAMP', TRUE),
        ('QUANTITY', 'NUMBER', TRUE),
        ('UNIT_PRICE', 'NUMBER', TRUE),
        ('GROSS_AMOUNT', 'NUMBER', TRUE),
        ('DISCOUNT_AMOUNT', 'NUMBER', TRUE),
        ('NET_AMOUNT', 'NUMBER', TRUE),
        ('PAYMENT_METHOD', 'STRING', TRUE),
        ('ORDER_STATUS', 'STRING', TRUE),
        ('BATCH_ID', 'STRING', TRUE),
        ('GENERATED_AT_UTC', 'STRING', TRUE),
        ('SOURCE_FILE_NAME', 'STRING', TRUE),
        ('LOAD_TS', 'TIMESTAMP', TRUE)
    AS expected(column_name, expected_type, is_required)

),

actual_schema AS (

    SELECT
        column_name,
        data_type,
        CASE
            WHEN data_type IN ('NUMBER', 'DECIMAL', 'NUMERIC', 'INT', 'INTEGER', 'BIGINT') THEN 'NUMBER'
            WHEN data_type IN ('TEXT', 'VARCHAR', 'STRING') THEN 'STRING'
            WHEN data_type LIKE 'TIMESTAMP%' THEN 'TIMESTAMP'
            WHEN data_type = 'DATE' THEN 'DATE'
            ELSE data_type
        END AS normalized_data_type
    FROM SYNTHETIC_DATA.INFORMATION_SCHEMA.COLUMNS
    WHERE table_schema = 'RAW'
      AND table_name = 'SALES_RAW'

)

SELECT
    expected.column_name,
    expected.expected_type,
    actual.data_type AS actual_snowflake_type,
    actual.normalized_data_type AS actual_normalized_type,
    expected.is_required,
    CASE
        WHEN actual.column_name IS NULL THEN 'MISSING_COLUMN'
        WHEN expected.expected_type <> actual.normalized_data_type THEN 'TYPE_MISMATCH'
        ELSE 'PASSED'
    END AS schema_validation_status

FROM expected_schema expected
LEFT JOIN actual_schema actual
    ON expected.column_name = actual.column_name
ORDER BY
    schema_validation_status DESC,
    expected.column_name;
```

Expected result:

```text
schema_validation_status = PASSED
```

Any result with `MISSING_COLUMN` or `TYPE_MISMATCH` should be reviewed before downstream transformations continue.

### Detecting Additive Columns in Snowflake

This query detects columns that exist in `RAW.SALES_RAW` but are not part of the expected schema contract:

```sql
WITH expected_columns AS (

    SELECT column_name
    FROM VALUES
        ('ORDER_ID'),
        ('CUSTOMER_ID'),
        ('PRODUCT_ID'),
        ('PRODUCT_CATEGORY'),
        ('ORDER_TIMESTAMP'),
        ('QUANTITY'),
        ('UNIT_PRICE'),
        ('GROSS_AMOUNT'),
        ('DISCOUNT_AMOUNT'),
        ('NET_AMOUNT'),
        ('PAYMENT_METHOD'),
        ('ORDER_STATUS'),
        ('BATCH_ID'),
        ('GENERATED_AT_UTC'),
        ('SOURCE_FILE_NAME'),
        ('LOAD_TS')
    AS expected(column_name)

),

actual_columns AS (

    SELECT column_name
    FROM SYNTHETIC_DATA.INFORMATION_SCHEMA.COLUMNS
    WHERE table_schema = 'RAW'
      AND table_name = 'SALES_RAW'

)

SELECT
    actual.column_name AS additive_column,
    'ADDITIVE_COLUMN_DETECTED' AS schema_validation_status
FROM actual_columns actual
LEFT JOIN expected_columns expected
    ON actual.column_name = expected.column_name
WHERE expected.column_name IS NULL
ORDER BY actual.column_name;
```

Additive columns are not automatically bad. They should be reviewed, documented, and promoted intentionally through dbt.

### dbt Safe Column Handling

dbt staging can safely handle optional columns using a macro.

Macro file:

```text
dbt_project/macros/safe_column.sql
```

Example macro:

```sql
{% macro safe_column(source_relation, column_name, data_type='varchar') %}
    {%- set columns = adapter.get_columns_in_relation(source_relation) -%}
    {%- set column_names = columns | map(attribute='name') | map('upper') | list -%}

    {%- if column_name.upper() in column_names -%}
        {{ column_name }}
    {%- else -%}
        cast(null as {{ data_type }})
    {%- endif -%}
{% endmacro %}
```

Example usage in `stg_sales.sql`:

```sql
{% set sales_raw_relation = source('raw', 'sales_raw') %}

select
    order_id,
    customer_id,
    product_id,
    product_category,

    {{ safe_column(sales_raw_relation, 'sales_channel', 'varchar') }} as sales_channel,

    order_timestamp,
    quantity,
    unit_price,
    gross_amount,
    discount_amount,
    net_amount,
    payment_method,
    order_status,
    batch_id,
    generated_at_utc,
    source_file_name,
    load_ts

from {{ sales_raw_relation }}
```

If `SALES_CHANNEL` exists, dbt selects it.

If `SALES_CHANNEL` does not exist yet, dbt returns:

```sql
cast(null as varchar)
```

This prevents optional source columns from breaking dbt compilation.

### Promotion Pattern

New source columns should move through the pipeline intentionally:

```text
RAW
    → accept approved additive column

STAGING
    → standardize and type-cast column

INTERMEDIATE
    → apply business logic only if needed

MARTS
    → expose column only when required for reporting

DASHBOARD
    → use column only after mart adoption
```

This prevents every upstream source change from automatically becoming a reporting change.

### Why This Matters

Schema evolution is important because upstream systems change over time.

A production-style ELT pipeline should:

```text
Detect schema drift
Allow safe additive changes
Fail fast on breaking changes
Protect downstream dbt models
Avoid silent dashboard corruption
Create an audit trail of schema changes
```

This project handles schema evolution using a contract-based approach in Python, metadata validation in Snowflake, and safe optional column handling in dbt.

### Interview Explanation

I implemented schema evolution as a controlled contract-based process. The Python ingestion layer validates the generated DataFrame against a JSON schema contract before saving and uploading the file. Required columns must exist, type-breaking changes fail ingestion, and approved optional columns can be added safely.

In Snowflake, I validate the raw table structure using `INFORMATION_SCHEMA.COLUMNS` and separately detect additive columns. In dbt, staging models use a safe column macro so optional fields do not break compilation if they are not yet present in the raw table.

This gives the pipeline a controlled schema evolution workflow: breaking changes fail early, while additive changes can be reviewed, documented, and promoted intentionally through staging, intermediate models, marts, and dashboards.


## Late-Arriving Data Simulation

This project includes a late-arriving data simulation to demonstrate how the pipeline handles records that arrive today but belong to older business dates.

In real-world data pipelines, late-arriving data can happen when source systems delay file delivery, retry failed batches, or send corrections for prior business periods. This project simulates that scenario by generating a file that lands in S3 today while the `order_timestamp` values are intentionally backdated.

### Late-Arriving Data Flow

```text
late_arriving.py
    ↓
Generate historical order timestamps
    ↓
Validate synthetic sales data
    ↓
Save late-arriving CSV locally
    ↓
Upload file to S3 sales/raw/
    ↓
Snowpipe auto-ingests the file
    ↓
dbt incremental fact model processes by load_ts
    ↓
Historical KPI dates are updated
```

### Why This Matters

The file arrival date and business event date are different.

Example:

```text
File arrival date: 2026-06-09
S3 path: sales/raw/year=2026/month=06/day=09/
Order timestamps: 2026-05-10 to 2026-06-01
```

This means the file lands in today’s S3 partition, but the actual sales transactions belong to prior reporting dates.

### Run Late-Arriving Data Simulation

From the project root:

```powershell
cd C:\dev\synthetic-data-generator
python late_arriving.py
```

Expected output:

```text
Starting late-arriving sales data simulation...
Generated late-arriving records: 5,000
Min order timestamp: 2026-05-10 00:08:38
Max order timestamp: 2026-06-01 23:57:17
Data validation passed.
Late-arriving CSV file created: output\sales_late_arriving_YYYYMMDD_HHMMSS.csv
Uploaded to S3: s3://vinski-synthetic-data-bucket/sales/raw/year=YYYY/month=MM/day=DD/sales_late_arriving_YYYYMMDD_HHMMSS.csv
Late-arriving data simulation completed successfully.
```

### Validate Snowpipe Loaded the Late File

Run in Snowflake:

```sql
SELECT
    SOURCE_FILE_NAME,
    COUNT(*) AS row_count,
    MIN(ORDER_TIMESTAMP) AS min_order_timestamp,
    MAX(ORDER_TIMESTAMP) AS max_order_timestamp,
    MAX(LOAD_TS) AS latest_load_ts
FROM SYNTHETIC_DATA.RAW.SALES_RAW
WHERE SOURCE_FILE_NAME ILIKE '%late_arriving%'
GROUP BY SOURCE_FILE_NAME
ORDER BY latest_load_ts DESC;
```

Expected result:

```text
ROW_COUNT = 5000
ORDER_TIMESTAMP values are historical
LOAD_TS is recent
```

### Run dbt After Late Data Arrives

After Snowpipe loads the file, run:

```powershell
cd C:\dev\synthetic-data-generator\dbt_project
dbt build --selector transform_pipeline --no-partial-parse
```

### Validate Late Data Reached the Fact Table

```sql
SELECT
    SOURCE_FILE_NAME,
    COUNT(*) AS fact_row_count,
    MIN(ORDER_DATE) AS min_order_date,
    MAX(ORDER_DATE) AS max_order_date,
    MAX(LOAD_TS) AS latest_load_ts
FROM SYNTHETIC_DATA.MARTS.FCT_SALES
WHERE SOURCE_FILE_NAME ILIKE '%late_arriving%'
GROUP BY SOURCE_FILE_NAME
ORDER BY latest_load_ts DESC;
```

Expected result:

```text
FACT_ROW_COUNT = 5000
ORDER_DATE values are historical
LOAD_TS is recent
```

### Validate Historical KPI Dates Were Updated

```sql
SELECT
    ORDER_DATE,
    TOTAL_ORDERS,
    COMPLETED_ORDERS,
    NET_SALES_AMOUNT,
    LATEST_LOAD_TS
FROM SYNTHETIC_DATA.MARTS.MART_SALES_DAILY_KPI
WHERE ORDER_DATE BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE - 7
ORDER BY LATEST_LOAD_TS DESC
LIMIT 20;
```

This confirms that historical business dates are updated when late-arriving records are processed.

### Late-Arriving Data Design Pattern

The dbt fact model uses `load_ts` for incremental processing instead of filtering only on `order_date`.

This is important because filtering only by business date can miss records that arrive late but belong to older reporting periods.

Recommended pattern:

```text
Use load timestamp for incremental ingestion logic.
Use business date for reporting and KPI aggregation.
```

### Interview Explanation

I simulated late-arriving data by generating sales files that land in S3 today but contain historical order timestamps. Snowpipe ingests the file based on arrival time, while dbt processes the data using the load timestamp. This allows the pipeline to capture late-arriving records and update historical KPI dates correctly.

This pattern is important because real production pipelines often receive delayed files, retries, or corrections from source systems. By using `load_ts` for incremental processing and `order_date` for business reporting, the pipeline avoids missing late-arriving events.

## Backfill Window

This project includes a manual backfill workflow for generating and processing historical sales data across a selected business date range.

A backfill window is a controlled date range used to reload, regenerate, or correct historical data. In this project, the backfill workflow creates synthetic sales records for specific historical `order_timestamp` dates, uploads the files to S3, lets Snowpipe ingest them into Snowflake, and then runs dbt to update the fact and KPI marts.

### Backfill Use Case

Backfills are common in production data pipelines when:

* A source system sends delayed historical files
* A prior ingestion failed and needs to be replayed
* Business logic changes require historical KPI recalculation
* A data correction needs to be applied to previous reporting dates
* Late-arriving records need to be included in historical dashboards

### Backfill Flow

```text
backfill_sales.py
    ↓
Generate sales records for each business date
    ↓
Validate generated data
    ↓
Save backfill CSV files locally
    ↓
Upload files to S3 under sales/raw/backfill/
    ↓
Snowpipe auto-ingests files into RAW.SALES_RAW
    ↓
dbt processes records using LOAD_TS
    ↓
Historical fact and KPI tables are updated by ORDER_DATE
```

### Backfill File Path Pattern

Backfill files are uploaded to S3 using this pattern:

```text
sales/raw/backfill/business_date=YYYY-MM-DD/
```

Example:

```text
s3://vinski-synthetic-data-bucket/sales/raw/backfill/business_date=2026-05-01/sales_backfill_20260501_20260609_120000.csv
```

This separates backfill files from normal daily files while keeping them under the same Snowpipe-monitored raw prefix.

### Run Backfill Locally

From the project root:

```powershell
cd C:\dev\synthetic-data-generator
python backfill_sales.py --start-date 2026-05-01 --end-date 2026-05-03 --records-per-day 1000
```

Example output:

```text
Starting sales backfill workflow...
Backfill date range: 2026-05-01 to 2026-05-03
Records per day: 1,000

Processing business date: 2026-05-01
Generated records for 2026-05-01: 1,000
Data validation passed.
Backfill CSV file created.
Uploaded backfill file to S3.

Processing business date: 2026-05-02
Generated records for 2026-05-02: 1,000
Data validation passed.
Backfill CSV file created.
Uploaded backfill file to S3.

Processing business date: 2026-05-03
Generated records for 2026-05-03: 1,000
Data validation passed.
Backfill CSV file created.
Uploaded backfill file to S3.

Backfill workflow completed successfully.
```

### Run Backfill Through Airflow

The project can also support a manual Airflow DAG for backfill execution.

DAG:

```text
sales_backfill_workflow
```

Recommended Airflow parameters:

```json
{
  "start_date": "2026-05-01",
  "end_date": "2026-05-03",
  "records_per_day": 1000
}
```

Airflow task flow:

```text
generate_and_upload_backfill_files
    ↓
wait_for_snowpipe_backfill
    ↓
run_dbt_build
```

The Airflow backfill workflow is manual by design because backfills should usually be intentional, controlled, and auditable.

### Validate Snowpipe Loaded Backfill Files

Run this in Snowflake:

```sql
SELECT
    SOURCE_FILE_NAME,
    COUNT(*) AS row_count,
    MIN(ORDER_TIMESTAMP) AS min_order_timestamp,
    MAX(ORDER_TIMESTAMP) AS max_order_timestamp,
    MAX(LOAD_TS) AS latest_load_ts
FROM SYNTHETIC_DATA.RAW.SALES_RAW
WHERE SOURCE_FILE_NAME ILIKE '%backfill%'
GROUP BY SOURCE_FILE_NAME
ORDER BY latest_load_ts DESC;
```

Expected result:

```text
Each backfill file should have the expected row count.
ORDER_TIMESTAMP should match the historical business date.
LOAD_TS should reflect the recent ingestion time.
```

### Run dbt After Backfill

After Snowpipe loads the backfill files, run dbt:

```powershell
cd C:\dev\synthetic-data-generator\dbt_project
dbt build --selector transform_pipeline --no-partial-parse
```

The dbt fact model should process the backfill records because incremental logic is based on `LOAD_TS`.

### Validate Backfill Reached the Fact Table

```sql
SELECT
    SOURCE_FILE_NAME,
    COUNT(*) AS fact_row_count,
    MIN(ORDER_DATE) AS min_order_date,
    MAX(ORDER_DATE) AS max_order_date,
    MAX(LOAD_TS) AS latest_load_ts
FROM SYNTHETIC_DATA.MARTS.FCT_SALES
WHERE SOURCE_FILE_NAME ILIKE '%backfill%'
GROUP BY SOURCE_FILE_NAME
ORDER BY latest_load_ts DESC;
```

Expected result:

```text
FACT_ROW_COUNT should match the generated backfill record count.
ORDER_DATE should match the historical business date.
LOAD_TS should reflect the recent processing timestamp.
```

### Validate Historical KPI Dates Were Updated

```sql
SELECT
    ORDER_DATE,
    TOTAL_ORDERS,
    COMPLETED_ORDERS,
    NET_SALES_AMOUNT,
    LATEST_LOAD_TS
FROM SYNTHETIC_DATA.MARTS.MART_SALES_DAILY_KPI
WHERE ORDER_DATE BETWEEN '2026-05-01' AND '2026-05-03'
ORDER BY ORDER_DATE;
```

This confirms that the historical reporting dates affected by the backfill window were updated in the KPI mart.

### Backfill Design Pattern

This project separates ingestion time from business event time:

```text
LOAD_TS      = when Snowflake ingested the record
ORDER_DATE   = when the business event happened
```

Recommended pattern:

```text
Use LOAD_TS for incremental processing.
Use ORDER_DATE for reporting and KPI aggregation.
```

This prevents late-arriving or backfilled records from being missed by dbt incremental models.

### Backfill Safety Considerations

For production-style backfills, the recommended controls are:

* Use explicit start and end dates
* Keep backfill runs manual or approval-based
* Log uploaded files and row counts
* Validate Snowpipe ingestion before running dbt
* Reconcile raw row counts against fact table row counts
* Rebuild affected downstream marts after backfill
* Avoid filtering incremental models only by business date

### Interview Explanation

I added a backfill workflow to simulate how a production data pipeline handles historical reloads or corrections. The workflow accepts a start date, end date, and records-per-day parameter, generates historical sales records for each business date, uploads them to S3, waits for Snowpipe ingestion, and runs dbt to update the downstream fact and KPI marts.

The important design choice is that dbt incremental processing uses `LOAD_TS`, while reporting uses `ORDER_DATE`. This allows the pipeline to capture records that arrive today but belong to prior reporting periods, which is a common real-world data engineering scenario.

## Airflow SMTP Email Alerting

This project supports SMTP-based email alerting for Airflow task failures.

Email alerting provides an additional observability layer alongside Slack alerting. When an Airflow task fails, Airflow can send an email notification to a configured recipient with failure details.

### Email Alerting Flow

```text
Airflow task fails
    ↓
Airflow detects task failure
    ↓
email_on_failure is triggered
    ↓
Airflow uses SMTP configuration
    ↓
Failure email is sent to AIRFLOW_ALERT_EMAIL
    ↓
Engineer reviews the Airflow logs and resolves the issue
```

### SMTP Configuration in `.env`

SMTP values are stored in the local `.env` file.

Example using Gmail SMTP:

```env
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_STARTTLS=True
SMTP_SSL=False
SMTP_USER=your_sender_email@gmail.com
SMTP_PASSWORD=your_gmail_app_password
SMTP_MAIL_FROM=your_sender_email@gmail.com
AIRFLOW_ALERT_EMAIL=your_receiver_email@gmail.com
```

For Gmail, use a Google App Password instead of your regular Gmail password.

The real `.env` file should never be committed to GitHub.

Make sure `.gitignore` includes:

```text
.env
set_env.ps1
```

### Docker Compose Configuration

The SMTP values are passed into the Airflow containers through the `environment:` section in `docker-compose.yaml`.

Add these values under the Airflow common environment section:

```yaml
AIRFLOW__EMAIL__EMAIL_BACKEND: airflow.utils.email.send_email_smtp

AIRFLOW__SMTP__SMTP_HOST: ${SMTP_HOST}
AIRFLOW__SMTP__SMTP_PORT: ${SMTP_PORT}
AIRFLOW__SMTP__SMTP_STARTTLS: ${SMTP_STARTTLS}
AIRFLOW__SMTP__SMTP_SSL: ${SMTP_SSL}
AIRFLOW__SMTP__SMTP_USER: ${SMTP_USER}
AIRFLOW__SMTP__SMTP_PASSWORD: ${SMTP_PASSWORD}
AIRFLOW__SMTP__SMTP_MAIL_FROM: ${SMTP_MAIL_FROM}

AIRFLOW_ALERT_EMAIL: ${AIRFLOW_ALERT_EMAIL}
```

Example placement:

```yaml
x-airflow-common:
  &airflow-common
  build: .
  environment:
    AIRFLOW__CORE__EXECUTOR: CeleryExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres/airflow
    AIRFLOW__CELERY__BROKER_URL: redis://:@redis:6379/0
    AIRFLOW__CORE__FERNET_KEY: ${FERNET_KEY}
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "true"
    AIRFLOW__CORE__LOAD_EXAMPLES: "false"

    AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
    AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
    AWS_DEFAULT_REGION: ${AWS_DEFAULT_REGION}

    SNOWFLAKE_ACCOUNT: ${SNOWFLAKE_ACCOUNT}
    SNOWFLAKE_USER: ${SNOWFLAKE_USER}
    SNOWFLAKE_PASSWORD: ${SNOWFLAKE_PASSWORD}
    SNOWFLAKE_ROLE: ${SNOWFLAKE_ROLE}
    SNOWFLAKE_WAREHOUSE: ${SNOWFLAKE_WAREHOUSE}
    SNOWFLAKE_DATABASE: ${SNOWFLAKE_DATABASE}

    SLACK_WEBHOOK_URL: ${SLACK_WEBHOOK_URL}

    AIRFLOW__EMAIL__EMAIL_BACKEND: airflow.utils.email.send_email_smtp
    AIRFLOW__SMTP__SMTP_HOST: ${SMTP_HOST}
    AIRFLOW__SMTP__SMTP_PORT: ${SMTP_PORT}
    AIRFLOW__SMTP__SMTP_STARTTLS: ${SMTP_STARTTLS}
    AIRFLOW__SMTP__SMTP_SSL: ${SMTP_SSL}
    AIRFLOW__SMTP__SMTP_USER: ${SMTP_USER}
    AIRFLOW__SMTP__SMTP_PASSWORD: ${SMTP_PASSWORD}
    AIRFLOW__SMTP__SMTP_MAIL_FROM: ${SMTP_MAIL_FROM}

    AIRFLOW_ALERT_EMAIL: ${AIRFLOW_ALERT_EMAIL}
```

### DAG Configuration

Each DAG can enable email alerting using `default_args`.

Example:

```python
import os

DEFAULT_ALERT_EMAIL = os.getenv("AIRFLOW_ALERT_EMAIL")


@dag(
    dag_id="synthetic_sales_full_elt",
    description="Generate sales data, upload to S3, wait for Snowpipe, then run dbt transformations.",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "email": [DEFAULT_ALERT_EMAIL] if DEFAULT_ALERT_EMAIL else [],
        "email_on_failure": True,
        "email_on_retry": False,
    },
    tags=["synthetic-data", "s3", "snowpipe", "snowflake", "dbt"],
)
def synthetic_sales_full_elt():
    ...
```

The same pattern can be applied to the manual backfill DAG:

```python
@dag(
    dag_id="sales_backfill_workflow",
    description="Manual sales backfill workflow for historical business dates.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "email": [DEFAULT_ALERT_EMAIL] if DEFAULT_ALERT_EMAIL else [],
        "email_on_failure": True,
        "email_on_retry": False,
    },
    tags=["sales", "backfill", "snowpipe", "dbt"],
)
def sales_backfill_workflow():
    ...
```

### Rebuild Airflow After Updating SMTP Settings

After updating `.env` or `docker-compose.yaml`, rebuild the Airflow containers:

```powershell
cd C:\dev\synthetic-data-generator

docker compose down
docker compose up -d --build
```

Verify the SMTP environment variables are available inside the Airflow container:

```powershell
docker compose exec airflow-scheduler printenv | findstr SMTP
docker compose exec airflow-scheduler printenv | findstr AIRFLOW_ALERT_EMAIL
```

Expected result:

```text
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
AIRFLOW_ALERT_EMAIL=your_receiver_email@gmail.com
```

### Testing Email Alerting

To test email alerting safely, add a temporary failing task to a DAG:

```python
@task
def test_email_failure_task() -> None:
    raise RuntimeError("Testing Airflow email failure alert")
```

Then link it after an existing task:

```python
test_email_failure = test_email_failure_task()
build >> test_email_failure
```

Trigger the DAG manually from the Airflow UI.

Expected result:

```text
Task fails
    ↓
Airflow sends an email alert
    ↓
Engineer receives failure notification
```

After confirming the email alert works, remove the temporary test task.

### Why SMTP Email Alerting Was Added

SMTP email alerting makes the pipeline more production-like by notifying engineers when an Airflow task fails.

This helps reduce time to detection for failures in:

```text
Data generation
S3 upload
Snowpipe ingestion waiting
dbt build
Soda data quality checks
Backfill workflows
```

### Interview Explanation

I added SMTP-based email alerting to the Airflow DAGs. The SMTP credentials are stored in the local `.env` file and passed into the Airflow containers through Docker Compose. The DAGs use `email_on_failure=True`, so failed tasks send notifications to the configured alert recipient.

This gives the pipeline an additional observability layer and makes failure handling more production-ready.


### Technical Data Quality

* Not-null checks
* Unique key checks
* Accepted values checks
* Source freshness checks

### Business Data Quality

* Gross amount reconciliation
* Net amount reconciliation
* Raw-to-fact row count reconciliation
* Source file load audit

Example validations:

```text
gross_amount = quantity * unit_price
net_amount = gross_amount - discount_amount
raw_row_count = fact_row_count
order_status IN ('COMPLETED', 'PENDING', 'CANCELLED', 'REFUNDED')
```

## Load Reconciliation

The `mart_load_audit` model validates whether each Snowpipe-loaded file is represented correctly in the transformed fact table.

It compares:

```text
RAW.SALES_RAW row count by source_file_name
        vs
MARTS.FCT_SALES row count by source_file_name
```

Expected result:

```text
RECONCILIATION_STATUS = MATCHED
```

Example query:

```sql
SELECT *
FROM SYNTHETIC_DATA.MARTS.MART_LOAD_AUDIT
ORDER BY LAST_LOADED_AT DESC;
```

Summary validation:

```sql
SELECT
    COUNT(*) AS total_files,
    SUM(raw_row_count) AS total_raw_rows,
    SUM(fact_row_count) AS total_fact_rows,
    SUM(row_count_difference) AS total_difference
FROM SYNTHETIC_DATA.MARTS.MART_LOAD_AUDIT;
```

Expected:

```text
TOTAL_DIFFERENCE = 0
```

## Airflow Orchestration

Airflow orchestrates the ELT workflow.

DAG:

```text
synthetic_sales_full_elt
```

Task flow:

```text
generate_task
    ↓
validate_task
    ↓
upload_task
    ↓
wait_for_snowpipe_task
    ↓
dbt_build_task
```

Optional observability task:

```text
dbt_source_freshness_task
```

During development, source freshness can be treated as a monitoring task instead of a blocking step. Once ingestion timing is stable, it can be promoted into a blocking data quality gate.

## GitHub Actions CI/CD

The project includes GitHub Actions workflows for automated validation.

### CI Workflow

File:

```text
.github/workflows/ci.yml
```

Purpose:

* Install Python 3.11
* Install project dependencies
* Validate Python modules
* Generate dbt `profiles.yml` from GitHub Secrets
* Run `dbt debug`
* Run `dbt parse`
* Run `dbt compile`
* Run `dbt build --empty`

### CD Workflow

File:

```text
.github/workflows/dbt_deploy.yml
```

Purpose:

* Run dbt build against Snowflake
* Validate dbt models after merge to main
* Support manual deployment through `workflow_dispatch`

## Security and Credential Handling

Credentials are not hardcoded in the project.

The project uses:

* `.env` for local Docker/Airflow development
* PowerShell environment variables for local dbt testing
* GitHub Secrets for CI/CD
* Snowflake storage integrations for secure S3 access
* AWS IAM roles and policies for least-privilege access

Do not commit:

```text
.env
set_env.ps1
venv/
venv311/
output/
logs/
dbt_project/target/
dbt_project/logs/
dbt_project/dbt_packages/
```

## GitHub Secrets

Configure these in:

```text
GitHub Repository → Settings → Secrets and variables → Actions → Secrets
```

Required secrets:

```text
SNOWFLAKE_ACCOUNT
SNOWFLAKE_USER
SNOWFLAKE_PASSWORD
SNOWFLAKE_ROLE
SNOWFLAKE_WAREHOUSE
SNOWFLAKE_DATABASE
```

Recommended values:

```text
SNOWFLAKE_ACCOUNT=CYOVZFT-UF47725
SNOWFLAKE_ROLE=SYSADMIN
SNOWFLAKE_WAREHOUSE=WH_DBT_DEV
SNOWFLAKE_DATABASE=SYNTHETIC_DATA
```

The `SNOWFLAKE_ACCOUNT` value should contain only the account identifier.

Use:

```text
CYOVZFT-UF47725
```

Do not use:

```text
https://CYOVZFT-UF47725.snowflakecomputing.com
CYOVZFT-UF47725.ap-southeast-1.aws
```
## Streamlit Dashboard

This project includes a Streamlit dashboard that connects directly to the Snowflake mart layer created by dbt.

The dashboard provides a business-facing view of the transformed sales data and demonstrates the final consumption layer of the ELT pipeline.

### Dashboard Data Sources

The dashboard reads from the following Snowflake mart tables:

```text
SYNTHETIC_DATA.MARTS.MART_SALES_DAILY_KPI
SYNTHETIC_DATA.MARTS.MART_CUSTOMER_SALES_KPI
SYNTHETIC_DATA.MARTS.MART_LOAD_AUDIT
```

### Dashboard Features

The Streamlit dashboard includes:

* Total orders
* Completed orders
* Unique customers
* Net sales amount
* Average order value
* Daily net sales trend
* Daily order status breakdown
* Top customers by completed net sales
* Load audit and reconciliation status

### Dashboard Architecture

```text
Snowflake MARTS Layer
    ↓
Streamlit Snowflake Connector
    ↓
Pandas DataFrames
    ↓
Streamlit KPI Cards and Charts
    ↓
Business-facing Sales Dashboard
```

### Run the Dashboard Locally

From the project root:

```powershell
cd C:\dev\synthetic-data-generator
.\venv311\Scripts\Activate.ps1
python -m streamlit run dashboard\streamlit_app.py
```

The dashboard will run locally at:

```text
http://localhost:8501
```

### Streamlit Credentials

For local development, Streamlit reads Snowflake credentials from:

```text
dashboard/.streamlit/secrets.toml
```

Example:

```toml
SNOWFLAKE_ACCOUNT = "your_snowflake_account"
SNOWFLAKE_USER = "your_snowflake_username"
SNOWFLAKE_PASSWORD = "your_snowflake_password"
SNOWFLAKE_ROLE = "SYSADMIN"
SNOWFLAKE_WAREHOUSE = "WH_DBT_DEV"
SNOWFLAKE_DATABASE = "SYNTHETIC_DATA"
```

This file should not be committed to GitHub.

Make sure `.gitignore` includes:

```text
dashboard/.streamlit/secrets.toml
```

### Required Dashboard Dependencies

The dashboard requires:

```text
streamlit
snowflake-connector-python
pandas
```

These should be included in `requirements.txt`.

### Dashboard Use Case

The dashboard completes the end-to-end analytics flow:

```text
Python synthetic data generation
    ↓
AWS S3 raw landing
    ↓
Snowpipe ingestion
    ↓
Snowflake raw table
    ↓
dbt staging, intermediate, and marts
    ↓
Streamlit dashboard
```

This demonstrates how raw data becomes business-facing analytics through a modern ELT pipeline.

### Portfolio Explanation

I added a Streamlit dashboard as the consumption layer of the pipeline. The dashboard connects to the Snowflake mart tables produced by dbt and visualizes daily sales KPIs, customer-level sales performance, and load reconciliation results.

This completes the end-to-end data flow from ingestion to analytics: Python generates the data, S3 stores the raw files, Snowpipe loads them into Snowflake, dbt transforms them into marts, and Streamlit presents the final business metrics.


## Local Setup

### 1. Clone the Repository

```powershell
git clone https://github.com/vinski-dev/synthetic-data-generator.git
cd synthetic-data-generator
```

### 2. Create Python Virtual Environment

```powershell
py -3.11 -m venv venv311
.\venv311\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

### 3. Configure Local Environment Variables

Create a `.env` file in the project root.

```env
AIRFLOW_UID=50000

AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_DEFAULT_REGION=ap-southeast-1

SNOWFLAKE_ACCOUNT=your_snowflake_account_identifier
SNOWFLAKE_USER=your_snowflake_username
SNOWFLAKE_PASSWORD=your_snowflake_password
SNOWFLAKE_ROLE=SYSADMIN
SNOWFLAKE_WAREHOUSE=WH_DBT_DEV
SNOWFLAKE_DATABASE=SYNTHETIC_DATA
```

## Running the Pipeline Manually

Generate synthetic sales data and upload to S3:

```powershell
python main.py
```

Expected behavior:

```text
Generate 100,000 synthetic sales records
Validate generated data
Write CSV to output folder
Upload CSV to S3
```

## Running dbt Locally

Go to the dbt project folder:

```powershell
cd dbt_project
```

Run dbt debug:

```powershell
dbt debug --no-partial-parse
```

Parse project:

```powershell
dbt parse --no-partial-parse
```

Build dbt models:

```powershell
dbt build --selector transform_pipeline --no-partial-parse
```

Run source freshness:

```powershell
dbt source freshness --no-partial-parse
```

Generate dbt docs:

```powershell
dbt docs generate
dbt docs serve
```

## Running Airflow Locally

Start Airflow using Docker Compose:

```powershell
docker compose up -d --build
```

Check containers:

```powershell
docker compose ps
```

View scheduler logs:

```powershell
docker compose logs -f airflow-scheduler
```

Open Airflow UI:

```text
http://localhost:8080
```

Default login:

```text
Username: airflow
Password: airflow
```

Stop Airflow:

```powershell
docker compose down
```

## Snowflake Validation Queries

Check raw load:

```sql
SELECT
    SOURCE_FILE_NAME,
    COUNT(*) AS row_count,
    MIN(LOAD_TS) AS first_loaded_at,
    MAX(LOAD_TS) AS last_loaded_at
FROM SYNTHETIC_DATA.RAW.SALES_RAW
GROUP BY SOURCE_FILE_NAME
ORDER BY last_loaded_at DESC;
```

Check dbt model row counts:

```sql
SELECT COUNT(*) AS staging_count
FROM SYNTHETIC_DATA.STAGING.STG_SALES;

SELECT COUNT(*) AS intermediate_count
FROM SYNTHETIC_DATA.INTERMEDIATE.INT_SALES_ENRICHED;

SELECT COUNT(*) AS fact_count
FROM SYNTHETIC_DATA.MARTS.FCT_SALES;
```

Check daily KPI mart:

```sql
SELECT *
FROM SYNTHETIC_DATA.MARTS.MART_SALES_DAILY_KPI
ORDER BY ORDER_DATE DESC
LIMIT 20;
```

Check customer KPI mart:

```sql
SELECT *
FROM SYNTHETIC_DATA.MARTS.MART_CUSTOMER_SALES_KPI
ORDER BY COMPLETED_NET_SALES_AMOUNT DESC
LIMIT 20;
```

Check load audit:

```sql
SELECT *
FROM SYNTHETIC_DATA.MARTS.MART_LOAD_AUDIT
ORDER BY LAST_LOADED_AT DESC;
```

## Soda Data Quality Checks

This project includes Soda data quality checks as an additional operational validation layer on top of dbt tests.

dbt tests validate model-level assumptions such as uniqueness, not-null rules, accepted values, and reconciliation logic. Soda adds a separate data quality layer that can run locally, in GitHub Actions, or inside Airflow after dbt transformations are complete.

### Soda Folder Structure

```text
soda/
├── configuration.yml
├── checks_raw.yml
├── checks_marts.yml
└── checks_reconciliation.yml
```

### What Soda Validates

The Soda checks validate:

* Raw table row count
* Required fields
* Missing values
* Duplicate keys
* Invalid order statuses
* Negative quantity values
* Negative amount values
* Fact table completeness
* Mart table row counts
* Raw-to-fact reconciliation
* Load audit status

### Soda Configuration

Soda uses the same Snowflake environment variables used by dbt:

```text
SNOWFLAKE_ACCOUNT
SNOWFLAKE_USER
SNOWFLAKE_PASSWORD
SNOWFLAKE_ROLE
SNOWFLAKE_WAREHOUSE
SNOWFLAKE_DATABASE
```

The Soda Snowflake connection is configured in:

```text
soda/configuration.yml
```

### Run Soda Locally

From the project root:

```powershell
cd C:\dev\synthetic-data-generator
.\venv311\Scripts\Activate.ps1
.\set_env.ps1
```

Run all Soda checks:

```powershell
soda scan -d synthetic_sales_snowflake -c soda\configuration.yml soda\checks_raw.yml soda\checks_marts.yml soda\checks_reconciliation.yml
```

If the `soda` command is not recognized, run:

```powershell
python -m soda.scan -d synthetic_sales_snowflake -c soda\configuration.yml soda\checks_raw.yml soda\checks_marts.yml soda\checks_reconciliation.yml
```

### Run Soda After dbt

Recommended local validation flow:

```powershell
cd C:\dev\synthetic-data-generator\dbt_project
dbt build --selector transform_pipeline --no-partial-parse
```

Then from the project root:

```powershell
cd C:\dev\synthetic-data-generator
soda scan -d synthetic_sales_snowflake -c soda\configuration.yml soda\checks_raw.yml soda\checks_marts.yml soda\checks_reconciliation.yml
```

This validates that dbt successfully produced clean and reconciled mart outputs.

### Run Soda in Airflow

Airflow can run Soda after the dbt build task.

The Docker Compose file must mount the local `soda/` folder into the Airflow container:

```yaml
- ${AIRFLOW_PROJ_DIR:-.}/soda:/opt/airflow/soda
```

Example Airflow task:

```python
@task
def soda_quality_check_task() -> None:
    run_command(
        [
            "soda",
            "scan",
            "-d",
            "synthetic_sales_snowflake",
            "-c",
            "/opt/airflow/soda/configuration.yml",
            "/opt/airflow/soda/checks_raw.yml",
            "/opt/airflow/soda/checks_marts.yml",
            "/opt/airflow/soda/checks_reconciliation.yml",
        ],
        cwd="/opt/airflow",
    )
```

Recommended Airflow dependency:

```text
dbt_build_task
    ↓
soda_quality_check_task
```

This ensures the pipeline validates data quality after transformations are complete.

### Required Docker Compose Mount

Make sure `docker-compose.yaml` includes:

```yaml
volumes:
  - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
  - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
  - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config
  - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins
  - ${AIRFLOW_PROJ_DIR:-.}/pipeline:/opt/airflow/pipeline
  - ${AIRFLOW_PROJ_DIR:-.}/dbt_project:/opt/airflow/dbt_project
  - ${AIRFLOW_PROJ_DIR:-.}/soda:/opt/airflow/soda
  - ${AIRFLOW_PROJ_DIR:-.}/config.py:/opt/airflow/config.py
  - ${AIRFLOW_PROJ_DIR:-.}/output:/opt/airflow/output
```

After updating Docker Compose, rebuild Airflow:

```powershell
cd C:\dev\synthetic-data-generator
docker compose down
docker compose up -d --build
```

Verify Soda files are mounted:

```powershell
docker compose exec airflow-scheduler ls -la /opt/airflow/soda
```

Expected files:

```text
configuration.yml
checks_raw.yml
checks_marts.yml
checks_reconciliation.yml
```

### Soda in GitHub Actions

The project can also run Soda checks in CI/CD through:

```text
.github/workflows/soda_data_quality.yml
```

The workflow connects to Snowflake using GitHub Actions secrets and runs the Soda checks against the raw and mart tables.

Required GitHub secrets:

```text
SNOWFLAKE_ACCOUNT
SNOWFLAKE_USER
SNOWFLAKE_PASSWORD
SNOWFLAKE_ROLE
SNOWFLAKE_WAREHOUSE
SNOWFLAKE_DATABASE
```

### Why Soda Was Added

Soda adds a practical operational data quality layer to the project.

dbt tests are excellent for model-level validation, while Soda is useful for pipeline-level checks such as row counts, missing values, invalid business values, reconciliation failures, and data quality monitoring across raw and mart tables.

This makes the pipeline more production-ready because data quality can be validated after ingestion, after transformation, and before dashboards consume the final marts.

### Interview Explanation

I added Soda data quality checks as an additional validation layer on top of dbt tests. dbt tests validate model assumptions such as uniqueness, not-null rules, and accepted values. Soda validates the pipeline outputs from an operational data quality perspective, including row counts, missing values, invalid statuses, negative values, and raw-to-fact reconciliation.

The checks can run locally, in GitHub Actions, or inside Airflow after dbt build. This gives the project a more production-style quality gate before the final Snowflake marts are used by the Streamlit dashboard.

## Troubleshooting

### Docker command not recognized

```text
docker: The term 'docker' is not recognized
```

Fix:

* Install Docker Desktop
* Start Docker Desktop
* Restart PowerShell
* Validate:

```powershell
docker --version
docker compose version
```

### Airflow cannot import pipeline module

```text
ModuleNotFoundError: No module named 'pipeline'
```

Fix:

* Confirm `pipeline/` is mounted in `docker-compose.yaml`
* Confirm `PYTHONPATH=/opt/airflow`
* Restart Airflow:

```powershell
docker compose down
docker compose up -d --build
```

### dbt profiles.yml not found in GitHub Actions

Fix:

* Generate `profiles.yml` inside the GitHub Actions workflow
* Use GitHub Secrets for Snowflake credentials
* Run dbt commands with:

```text
--profiles-dir .
```

### Snowflake account identifier error

Use only the account identifier:

```text
CYOVZFT-UF47725
```

Do not include dots, slashes, region, URL, or `snowflakecomputing.com`.

### dbt source freshness failed

Check latest raw load:

```sql
SELECT
    COUNT(*) AS row_count,
    MAX(LOAD_TS) AS latest_load_ts,
    DATEDIFF('hour', MAX(LOAD_TS), CURRENT_TIMESTAMP()) AS hours_since_latest_load
FROM SYNTHETIC_DATA.RAW.SALES_RAW;
```

For development, use a relaxed freshness SLA. Once the ingestion schedule is stable, tighten the freshness rule.

### Snowpipe did not load files

Check pipe status:

```sql
SELECT SYSTEM$PIPE_STATUS('SYNTHETIC_DATA.RAW.SALES_AUTO_PIPE');
```

Check files in stage:

```sql
LIST @SYNTHETIC_DATA.RAW.SALES_S3_STAGE;
```

Refresh recent staged files:

```sql
ALTER PIPE SYNTHETIC_DATA.RAW.SALES_AUTO_PIPE REFRESH;
```

## Current Implementation Status

Implemented:

* Python synthetic sales data generator
* Local CSV output
* AWS S3 upload
* Snowpipe auto-ingestion
* Snowflake raw table
* dbt staging model
* dbt intermediate model
* dbt mart models
* dbt tests
* Source freshness
* Load audit mart
* Airflow orchestration
* GitHub Actions CI/CD


## Future Enhancements

Potential next improvements:

* Add Terraform for AWS and Snowflake infrastructure
* Add data lineage screenshots from dbt docs


## Interview-Ready Summary

I built an end-to-end ELT pipeline that generates synthetic sales data using Python, lands the files in AWS S3, auto-ingests them into Snowflake using Snowpipe, and transforms the raw data into analytics-ready models using dbt.

Airflow orchestrates the workflow from data generation to transformation, while GitHub Actions provides CI/CD validation. The dbt project includes staging, intermediate, and mart layers, along with data quality tests, source freshness checks, and load reconciliation to ensure the final KPI tables are reliable and business-ready.
