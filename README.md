\# Synthetic Sales ELT Pipeline with Airflow, S3, Snowpipe, Snowflake, dbt, and GitHub Actions



\## Project Overview



This project is an end-to-end data engineering pipeline that generates synthetic sales transaction data, lands the data in AWS S3, auto-ingests it into Snowflake using Snowpipe, and transforms it into analytics-ready models using dbt.



The pipeline demonstrates a practical modern ELT architecture with orchestration, cloud storage, warehouse ingestion, data transformation, testing, reconciliation, documentation, and CI/CD.



\## Architecture



```text

Python Synthetic Data Generator

&#x20;       ↓

Local CSV Output

&#x20;       ↓

AWS S3 Raw Landing Zone

&#x20;       ↓

Snowpipe Auto-Ingestion

&#x20;       ↓

Snowflake RAW.SALES\_RAW

&#x20;       ↓

dbt Staging Models

&#x20;       ↓

dbt Intermediate Models

&#x20;       ↓

dbt Mart Models

&#x20;       ↓

Analytics-Ready KPI Tables

```



\## Technology Stack



| Area             | Tool                 |

| ---------------- | -------------------- |

| Programming      | Python 3.11          |

| Data Generation  | pandas, numpy, faker |

| Cloud Storage    | AWS S3               |

| Orchestration    | Apache Airflow       |

| Data Warehouse   | Snowflake            |

| Auto-Ingestion   | Snowpipe             |

| Transformation   | dbt                  |

| CI/CD            | GitHub Actions       |

| Containerization | Docker Compose       |

| Version Control  | Git and GitHub       |



\## Business Use Case



The pipeline simulates a sales analytics workload.



It generates synthetic order-level transaction data with fields such as:



\* Order ID

\* Customer ID

\* Product ID

\* Product category

\* Order timestamp

\* Quantity

\* Unit price

\* Gross amount

\* Discount amount

\* Net amount

\* Payment method

\* Order status

\* Batch ID

\* Generated timestamp



The transformed data supports reporting use cases such as:



\* Daily sales performance

\* Completed vs cancelled/refunded orders

\* Customer-level sales KPIs

\* Discount analysis

\* File-level load reconciliation

\* Data quality monitoring



\## Project Structure



```text

synthetic-data-generator/

│

├── dags/

│   └── synthetic\_sales\_s3\_dag.py

│

├── pipeline/

│   ├── \_\_init\_\_.py

│   └── sales\_pipeline.py

│

├── dbt\_project/

│   ├── dbt\_project.yml

│   ├── profiles.yml

│   ├── selectors.yml

│   │

│   ├── macros/

│   │   └── generate\_schema\_name.sql

│   │

│   ├── models/

│   │   ├── staging/

│   │   │   ├── src\_sales.yml

│   │   │   └── stg\_sales.sql

│   │   │

│   │   ├── intermediate/

│   │   │   ├── intermediate.yml

│   │   │   └── int\_sales\_enriched.sql

│   │   │

│   │   └── marts/

│   │       ├── fct\_sales.sql

│   │       ├── mart\_sales\_daily\_kpi.sql

│   │       ├── mart\_customer\_sales\_kpi.sql

│   │       ├── mart\_load\_audit.sql

│   │       ├── marts.yml

│   │       └── exposures.yml

│   │

│   └── tests/

│       ├── assert\_sales\_amounts\_reconcile.sql

│       └── assert\_load\_audit\_matched.sql

│

├── .github/

│   └── workflows/

│       ├── ci.yml

│       └── dbt\_deploy.yml

│

├── output/

├── config.py

├── main.py

├── requirements.txt

├── Dockerfile

├── docker-compose.yaml

├── .env.example

├── .gitignore

└── README.md

```



\## Configuration



The project uses the following core configuration:



```python

BUCKET\_NAME = "vinski-synthetic-data-bucket"

NUM\_RECORDS = 100000

OUTPUT\_PREFIX = "sales"

REGION = "ap-southeast-1"

```



Generated files are uploaded to S3 using this path pattern:



```text

s3://vinski-synthetic-data-bucket/sales/raw/year=YYYY/month=MM/day=DD/file.csv

```



\## Environment Variables



Create a local `.env` file in the project root.



Do not commit `.env` to GitHub.



Example:



```env

AIRFLOW\_UID=50000



AWS\_ACCESS\_KEY\_ID=your\_aws\_access\_key

AWS\_SECRET\_ACCESS\_KEY=your\_aws\_secret\_key

AWS\_DEFAULT\_REGION=ap-southeast-1



SNOWFLAKE\_ACCOUNT=your\_snowflake\_account\_identifier

SNOWFLAKE\_USER=your\_snowflake\_username

SNOWFLAKE\_PASSWORD=your\_snowflake\_password

SNOWFLAKE\_ROLE=SYSADMIN

SNOWFLAKE\_WAREHOUSE=WH\_DBT\_DEV

SNOWFLAKE\_DATABASE=SYNTHETIC\_DATA

```



\## Python Setup



Create a Python 3.11 virtual environment.



```powershell

py -3.11 -m venv venv311

.\\venv311\\Scripts\\Activate.ps1

python -m pip install --upgrade pip

python -m pip install -r requirements.txt

```



Verify Python version:



```powershell

python --version

```



Expected:



```text

Python 3.11.x

```



\## Running the Python Pipeline Manually



To generate synthetic sales data and upload it to S3:



```powershell

python main.py

```



Expected result:



```text

Generated records: 100,000

Data validation passed

CSV file created in output/

File uploaded to S3

```



\## AWS S3 Setup



The project expects an S3 bucket:



```text

vinski-synthetic-data-bucket

```



The pipeline writes files under:



```text

sales/raw/

```



Minimum IAM permissions for the upload user:



```json

{

&#x20; "Version": "2012-10-17",

&#x20; "Statement": \[

&#x20;   {

&#x20;     "Sid": "AllowListTargetBucket",

&#x20;     "Effect": "Allow",

&#x20;     "Action": \[

&#x20;       "s3:ListBucket",

&#x20;       "s3:GetBucketLocation"

&#x20;     ],

&#x20;     "Resource": "arn:aws:s3:::vinski-synthetic-data-bucket"

&#x20;   },

&#x20;   {

&#x20;     "Sid": "AllowWriteToSalesPrefix",

&#x20;     "Effect": "Allow",

&#x20;     "Action": \[

&#x20;       "s3:PutObject",

&#x20;       "s3:GetObject",

&#x20;       "s3:AbortMultipartUpload",

&#x20;       "s3:ListMultipartUploadParts"

&#x20;     ],

&#x20;     "Resource": "arn:aws:s3:::vinski-synthetic-data-bucket/sales/\*"

&#x20;   }

&#x20; ]

}

```



\## Snowflake Setup



Create the main database and schemas:



```sql

USE ROLE SYSADMIN;



CREATE DATABASE IF NOT EXISTS SYNTHETIC\_DATA;



CREATE SCHEMA IF NOT EXISTS SYNTHETIC\_DATA.RAW;

CREATE SCHEMA IF NOT EXISTS SYNTHETIC\_DATA.STAGING;

CREATE SCHEMA IF NOT EXISTS SYNTHETIC\_DATA.INTERMEDIATE;

CREATE SCHEMA IF NOT EXISTS SYNTHETIC\_DATA.MARTS;

```



\## Snowflake Warehouses



This project uses a practical workload isolation approach:



```text

WH\_DBT\_DEV         → local development and dbt debug

WH\_DBT\_TRANSFORM   → staging and intermediate models

WH\_DBT\_MARTS       → facts, dimensions, and KPI marts

```



Create warehouses:



```sql

USE ROLE SYSADMIN;



CREATE WAREHOUSE IF NOT EXISTS WH\_DBT\_DEV

&#x20;   WAREHOUSE\_SIZE = 'XSMALL'

&#x20;   AUTO\_SUSPEND = 60

&#x20;   AUTO\_RESUME = TRUE

&#x20;   INITIALLY\_SUSPENDED = TRUE;



CREATE WAREHOUSE IF NOT EXISTS WH\_DBT\_TRANSFORM

&#x20;   WAREHOUSE\_SIZE = 'XSMALL'

&#x20;   AUTO\_SUSPEND = 60

&#x20;   AUTO\_RESUME = TRUE

&#x20;   INITIALLY\_SUSPENDED = TRUE;



CREATE WAREHOUSE IF NOT EXISTS WH\_DBT\_MARTS

&#x20;   WAREHOUSE\_SIZE = 'XSMALL'

&#x20;   AUTO\_SUSPEND = 120

&#x20;   AUTO\_RESUME = TRUE

&#x20;   INITIALLY\_SUSPENDED = TRUE;

```



Grant warehouse usage:



```sql

USE ROLE SECURITYADMIN;



GRANT USAGE, OPERATE ON WAREHOUSE WH\_DBT\_DEV TO ROLE SYSADMIN;

GRANT USAGE, OPERATE ON WAREHOUSE WH\_DBT\_TRANSFORM TO ROLE SYSADMIN;

GRANT USAGE, OPERATE ON WAREHOUSE WH\_DBT\_MARTS TO ROLE SYSADMIN;

```



\## Raw Snowflake Table



Snowpipe loads data into this table:



```sql

USE ROLE SYSADMIN;

USE DATABASE SYNTHETIC\_DATA;

USE SCHEMA RAW;



CREATE OR REPLACE TABLE SALES\_RAW (

&#x20;   ORDER\_ID NUMBER,

&#x20;   CUSTOMER\_ID NUMBER,

&#x20;   PRODUCT\_ID NUMBER,

&#x20;   PRODUCT\_CATEGORY STRING,

&#x20;   ORDER\_TIMESTAMP TIMESTAMP\_NTZ,

&#x20;   QUANTITY NUMBER,

&#x20;   UNIT\_PRICE NUMBER(18,2),

&#x20;   GROSS\_AMOUNT NUMBER(18,2),

&#x20;   DISCOUNT\_AMOUNT NUMBER(18,2),

&#x20;   NET\_AMOUNT NUMBER(18,2),

&#x20;   PAYMENT\_METHOD STRING,

&#x20;   ORDER\_STATUS STRING,

&#x20;   BATCH\_ID STRING,

&#x20;   GENERATED\_AT\_UTC TIMESTAMP\_TZ,

&#x20;   SOURCE\_FILE\_NAME STRING,

&#x20;   LOAD\_TS TIMESTAMP\_LTZ DEFAULT CURRENT\_TIMESTAMP()

);

```



\## Snowpipe Auto-Ingestion



Snowpipe auto-ingestion loads new S3 files into Snowflake when files arrive under:



```text

s3://vinski-synthetic-data-bucket/sales/raw/

```



High-level Snowpipe flow:



```text

S3 object created

&#x20;       ↓

S3 event notification

&#x20;       ↓

Snowflake notification channel

&#x20;       ↓

Snowpipe COPY INTO

&#x20;       ↓

RAW.SALES\_RAW

```



Create file format:



```sql

USE DATABASE SYNTHETIC\_DATA;

USE SCHEMA RAW;



CREATE OR REPLACE FILE FORMAT SALES\_CSV\_FF

&#x20;   TYPE = CSV

&#x20;   FIELD\_DELIMITER = ','

&#x20;   SKIP\_HEADER = 1

&#x20;   FIELD\_OPTIONALLY\_ENCLOSED\_BY = '"'

&#x20;   NULL\_IF = ('', 'NULL', 'null')

&#x20;   EMPTY\_FIELD\_AS\_NULL = TRUE

&#x20;   TIMESTAMP\_FORMAT = AUTO;

```



Create external stage:



```sql

CREATE OR REPLACE STAGE SALES\_S3\_STAGE

&#x20;   URL = 's3://vinski-synthetic-data-bucket/sales/raw/'

&#x20;   STORAGE\_INTEGRATION = S3\_SALES\_INT

&#x20;   FILE\_FORMAT = SALES\_CSV\_FF;

```



Create Snowpipe:



```sql

CREATE OR REPLACE PIPE SALES\_AUTO\_PIPE

&#x20;   AUTO\_INGEST = TRUE

AS

COPY INTO SALES\_RAW (

&#x20;   ORDER\_ID,

&#x20;   CUSTOMER\_ID,

&#x20;   PRODUCT\_ID,

&#x20;   PRODUCT\_CATEGORY,

&#x20;   ORDER\_TIMESTAMP,

&#x20;   QUANTITY,

&#x20;   UNIT\_PRICE,

&#x20;   GROSS\_AMOUNT,

&#x20;   DISCOUNT\_AMOUNT,

&#x20;   NET\_AMOUNT,

&#x20;   PAYMENT\_METHOD,

&#x20;   ORDER\_STATUS,

&#x20;   BATCH\_ID,

&#x20;   GENERATED\_AT\_UTC,

&#x20;   SOURCE\_FILE\_NAME,

&#x20;   LOAD\_TS

)

FROM (

&#x20;   SELECT

&#x20;       $1::NUMBER AS ORDER\_ID,

&#x20;       $2::NUMBER AS CUSTOMER\_ID,

&#x20;       $3::NUMBER AS PRODUCT\_ID,

&#x20;       $4::STRING AS PRODUCT\_CATEGORY,

&#x20;       TRY\_TO\_TIMESTAMP\_NTZ($5) AS ORDER\_TIMESTAMP,

&#x20;       $6::NUMBER AS QUANTITY,

&#x20;       $7::NUMBER(18,2) AS UNIT\_PRICE,

&#x20;       $8::NUMBER(18,2) AS GROSS\_AMOUNT,

&#x20;       $9::NUMBER(18,2) AS DISCOUNT\_AMOUNT,

&#x20;       $10::NUMBER(18,2) AS NET\_AMOUNT,

&#x20;       $11::STRING AS PAYMENT\_METHOD,

&#x20;       $12::STRING AS ORDER\_STATUS,

&#x20;       $13::STRING AS BATCH\_ID,

&#x20;       TRY\_TO\_TIMESTAMP\_TZ($14) AS GENERATED\_AT\_UTC,

&#x20;       METADATA$FILENAME AS SOURCE\_FILE\_NAME,

&#x20;       CURRENT\_TIMESTAMP() AS LOAD\_TS

&#x20;   FROM @SALES\_S3\_STAGE

)

FILE\_FORMAT = (FORMAT\_NAME = SALES\_CSV\_FF)

ON\_ERROR = 'CONTINUE';

```



Check Snowpipe status:



```sql

SELECT SYSTEM$PIPE\_STATUS('SYNTHETIC\_DATA.RAW.SALES\_AUTO\_PIPE');

```



Check loaded files:



```sql

SELECT

&#x20;   SOURCE\_FILE\_NAME,

&#x20;   COUNT(\*) AS ROW\_COUNT,

&#x20;   MIN(LOAD\_TS) AS FIRST\_LOADED\_AT,

&#x20;   MAX(LOAD\_TS) AS LAST\_LOADED\_AT

FROM SYNTHETIC\_DATA.RAW.SALES\_RAW

GROUP BY SOURCE\_FILE\_NAME

ORDER BY LAST\_LOADED\_AT DESC;

```



\## dbt Model Layers



The dbt project follows a layered transformation approach.



\### Staging Layer



Schema:



```text

SYNTHETIC\_DATA.STAGING

```



Model:



```text

stg\_sales

```



Purpose:



\* Read from `RAW.SALES\_RAW`

\* Standardize column names

\* Cast data types

\* Normalize text fields

\* Create a stable `sales\_event\_key`

\* Deduplicate records

\* Preserve source metadata



\### Intermediate Layer



Schema:



```text

SYNTHETIC\_DATA.INTERMEDIATE

```



Model:



```text

int\_sales\_enriched

```



Purpose:



\* Add reusable business logic

\* Calculate discount rate

\* Add completed-order flag

\* Add failed/reversed-order flag

\* Add order month

\* Prepare enriched sales data for marts



\### Mart Layer



Schema:



```text

SYNTHETIC\_DATA.MARTS

```



Models:



```text

fct\_sales

mart\_sales\_daily\_kpi

mart\_customer\_sales\_kpi

mart\_load\_audit

```



Purpose:



\* Create analytics-ready fact table

\* Create daily KPI mart

\* Create customer KPI mart

\* Reconcile raw file row counts against fact table row counts



\## dbt Commands



Go to the dbt project:



```powershell

cd C:\\dev\\synthetic-data-generator\\dbt\_project

```



Debug dbt connection:



```powershell

dbt debug --no-partial-parse

```



Parse project:



```powershell

dbt parse --no-partial-parse

```



Run source freshness:



```powershell

dbt source freshness --no-partial-parse

```



Build all transformation models:



```powershell

dbt build --selector transform\_pipeline --no-partial-parse

```



Build only the audit model and upstream dependencies:



```powershell

dbt build --select +mart\_load\_audit --no-partial-parse

```



Generate dbt docs:



```powershell

dbt docs generate

dbt docs serve

```



\## dbt Data Quality Checks



The project includes dbt tests for:



\* Not-null checks

\* Unique keys

\* Accepted values

\* Source freshness

\* Amount reconciliation

\* Load reconciliation



Examples:



```text

sales\_event\_key must be unique

order\_id must not be null

customer\_id must not be null

order\_status must be one of COMPLETED, PENDING, CANCELLED, REFUNDED

gross\_amount must reconcile with quantity \* unit\_price

net\_amount must reconcile with gross\_amount - discount\_amount

raw file row count must match fact table row count

```



\## Load Audit Mart



The model `mart\_load\_audit` reconciles raw file loads against the transformed fact table.



It answers:



```text

Did every file loaded by Snowpipe reach the fact table correctly?

```



Example query:



```sql

SELECT \*

FROM SYNTHETIC\_DATA.MARTS.MART\_LOAD\_AUDIT

ORDER BY LAST\_LOADED\_AT DESC;

```



Expected result:



```text

RECONCILIATION\_STATUS = MATCHED

```



Summary validation:



```sql

SELECT

&#x20;   COUNT(\*) AS TOTAL\_FILES,

&#x20;   SUM(RAW\_ROW\_COUNT) AS TOTAL\_RAW\_ROWS,

&#x20;   SUM(FACT\_ROW\_COUNT) AS TOTAL\_FACT\_ROWS,

&#x20;   SUM(ROW\_COUNT\_DIFFERENCE) AS TOTAL\_DIFFERENCE

FROM SYNTHETIC\_DATA.MARTS.MART\_LOAD\_AUDIT;

```



Expected:



```text

TOTAL\_DIFFERENCE = 0

```



\## Airflow Orchestration



Airflow orchestrates the end-to-end ELT flow.



DAG:



```text

synthetic\_sales\_full\_elt

```



Task flow:



```text

generate\_task

&#x20;   ↓

validate\_task

&#x20;   ↓

upload\_task

&#x20;   ↓

wait\_for\_snowpipe\_task

&#x20;   ↓

dbt\_build\_task

```



Optional monitoring task:



```text

dbt\_source\_freshness\_task

```



During development, source freshness can be treated as monitoring instead of a blocking step. Once ingestion timing is stable, it can be promoted into a blocking quality gate.



\## Running Airflow with Docker Compose



Start Airflow:



```powershell

cd C:\\dev\\synthetic-data-generator

docker compose up -d --build

```



Check running containers:



```powershell

docker compose ps

```



View scheduler logs:



```powershell

docker compose logs -f airflow-scheduler

```



Open Airflow:



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



\## GitHub Actions CI/CD



This project includes GitHub Actions workflows for CI/CD.



\### CI Workflow



File:



```text

.github/workflows/ci.yml

```



Purpose:



\* Install Python 3.11

\* Install dependencies

\* Validate Python imports

\* Run dbt debug

\* Run dbt parse

\* Run dbt compile

\* Run dbt build dry-run or empty build



\### CD Workflow



File:



```text

.github/workflows/dbt\_deploy.yml

```



Purpose:



\* Run dbt against Snowflake

\* Check source freshness

\* Build staging, intermediate, and mart models

\* Support manual trigger through GitHub Actions



\## GitHub Secrets



Configure these secrets in GitHub:



```text

SNOWFLAKE\_ACCOUNT

SNOWFLAKE\_USER

SNOWFLAKE\_PASSWORD

SNOWFLAKE\_ROLE

SNOWFLAKE\_WAREHOUSE

SNOWFLAKE\_DATABASE

AWS\_ACCESS\_KEY\_ID

AWS\_SECRET\_ACCESS\_KEY

AWS\_DEFAULT\_REGION

```



Recommended values:



```text

SNOWFLAKE\_ROLE=SYSADMIN

SNOWFLAKE\_WAREHOUSE=WH\_DBT\_DEV

SNOWFLAKE\_DATABASE=SYNTHETIC\_DATA

AWS\_DEFAULT\_REGION=ap-southeast-1

```



\## Local Development Workflow



Recommended development flow:



```text

Create feature branch

&#x20;       ↓

Update Python/dbt/Airflow code

&#x20;       ↓

Run local dbt parse/build

&#x20;       ↓

Run Airflow DAG locally

&#x20;       ↓

Commit changes

&#x20;       ↓

Push to GitHub

&#x20;       ↓

GitHub Actions validates code

&#x20;       ↓

Merge to main

```



Example commands:



```powershell

git checkout -b feature/add-new-sales-mart



cd C:\\dev\\synthetic-data-generator\\dbt\_project

dbt parse --no-partial-parse

dbt build --selector transform\_pipeline --no-partial-parse



cd C:\\dev\\synthetic-data-generator

git status

git add .

git commit -m "Add new sales mart"

git push origin feature/add-new-sales-mart

```



\## Useful Validation Queries



Check raw row count:



```sql

SELECT COUNT(\*) AS RAW\_ROW\_COUNT

FROM SYNTHETIC\_DATA.RAW.SALES\_RAW;

```



Check staging row count:



```sql

SELECT COUNT(\*) AS STAGING\_ROW\_COUNT

FROM SYNTHETIC\_DATA.STAGING.STG\_SALES;

```



Check intermediate row count:



```sql

SELECT COUNT(\*) AS INTERMEDIATE\_ROW\_COUNT

FROM SYNTHETIC\_DATA.INTERMEDIATE.INT\_SALES\_ENRICHED;

```



Check fact row count:



```sql

SELECT COUNT(\*) AS FACT\_ROW\_COUNT

FROM SYNTHETIC\_DATA.MARTS.FCT\_SALES;

```



Check latest daily KPIs:



```sql

SELECT \*

FROM SYNTHETIC\_DATA.MARTS.MART\_SALES\_DAILY\_KPI

ORDER BY ORDER\_DATE DESC

LIMIT 20;

```



Check customer KPIs:



```sql

SELECT \*

FROM SYNTHETIC\_DATA.MARTS.MART\_CUSTOMER\_SALES\_KPI

ORDER BY COMPLETED\_NET\_SALES\_AMOUNT DESC

LIMIT 20;

```



Check load audit:



```sql

SELECT \*

FROM SYNTHETIC\_DATA.MARTS.MART\_LOAD\_AUDIT

ORDER BY LAST\_LOADED\_AT DESC;

```



\## Troubleshooting



\### Docker is not recognized



Error:



```text

docker: The term 'docker' is not recognized

```



Fix:



\* Install Docker Desktop

\* Start Docker Desktop

\* Restart PowerShell

\* Run:



```powershell

docker --version

docker compose version

```



\### Airflow cannot import pipeline module



Error:



```text

ModuleNotFoundError: No module named 'pipeline'

```



Fix:



Make sure `docker-compose.yaml` mounts the pipeline folder:



```yaml

\- ${AIRFLOW\_PROJ\_DIR:-.}/pipeline:/opt/airflow/pipeline

```



Also set:



```yaml

PYTHONPATH: /opt/airflow

```



Restart Airflow:



```powershell

docker compose down

docker compose up -d --build

```



\### dbt cannot connect to Snowflake



Check your Snowflake account identifier.



Correct examples:



```text

CYOVZFT-UF47725

```



or:



```text

account\_locator.ap-southeast-1.aws

```



Do not include:



```text

https://

.snowflakecomputing.com

```



\### dbt environment variable not found



Error:



```text

Env var required but not provided: SNOWFLAKE\_ACCOUNT

```



Fix local PowerShell:



```powershell

$env:SNOWFLAKE\_ACCOUNT="your\_account"

$env:SNOWFLAKE\_USER="your\_user"

$env:SNOWFLAKE\_PASSWORD="your\_password"

$env:SNOWFLAKE\_ROLE="SYSADMIN"

$env:SNOWFLAKE\_WAREHOUSE="WH\_DBT\_DEV"

$env:SNOWFLAKE\_DATABASE="SYNTHETIC\_DATA"

```



\### dbt duplicate exposure error



Error:



```text

dbt found two exposures with the name sales\_daily\_kpi\_dashboard

```



Fix:



Keep only one exposure file.



Recommended:



```text

models/marts/exposures.yml

```



Delete duplicate:



```powershell

Remove-Item models\\marts\\exposure.yml

```



\### dbt partial parsing error



Error example:



```text

KeyError: synthetic\_sales\_analytics://macros/generate\_schema\_name.sql

```



Fix:



```powershell

cd C:\\dev\\synthetic-data-generator\\dbt\_project



Remove-Item -Recurse -Force target -ErrorAction SilentlyContinue

Remove-Item -Recurse -Force logs -ErrorAction SilentlyContinue



dbt parse --no-partial-parse

```



Inside Docker:



```powershell

docker compose exec airflow-scheduler rm -rf /opt/airflow/dbt\_project/target

docker compose exec airflow-scheduler rm -rf /opt/airflow/dbt\_project/logs

```



\### Snowpipe did not load file



Check pipe status:



```sql

SELECT SYSTEM$PIPE\_STATUS('SYNTHETIC\_DATA.RAW.SALES\_AUTO\_PIPE');

```



Check files in stage:



```sql

LIST @SYNTHETIC\_DATA.RAW.SALES\_S3\_STAGE;

```



Refresh pipe for recent files:



```sql

ALTER PIPE SYNTHETIC\_DATA.RAW.SALES\_AUTO\_PIPE REFRESH;

```



\### Source freshness failed



Check latest raw load:



```sql

SELECT

&#x20;   COUNT(\*) AS ROW\_COUNT,

&#x20;   MAX(LOAD\_TS) AS LATEST\_LOAD\_TS,

&#x20;   DATEDIFF('hour', MAX(LOAD\_TS), CURRENT\_TIMESTAMP()) AS HOURS\_SINCE\_LATEST\_LOAD

FROM SYNTHETIC\_DATA.RAW.SALES\_RAW;

```



For development, use a relaxed freshness rule such as:



```yaml

warn\_after:

&#x20; count: 3

&#x20; period: day

error\_after:

&#x20; count: 7

&#x20; period: day

```



Once the pipeline runs daily and consistently, tighten freshness to:



```yaml

warn\_after:

&#x20; count: 26

&#x20; period: hour

error\_after:

&#x20; count: 30

&#x20; period: hour

```



\## Security Notes



This project avoids hardcoding credentials in source code.



Credentials should be managed through:



\* Local `.env`

\* PowerShell environment variables

\* Docker Compose environment variables

\* GitHub Actions secrets

\* Snowflake storage integrations

\* AWS IAM roles and policies



Do not commit:



```text

.env

set\_env.ps1

venv/

venv311/

output/

logs/

dbt\_project/target/

dbt\_project/logs/

dbt\_project/dbt\_packages/

```



\## .gitignore Recommendation



```text

.env

set\_env.ps1



venv/

venv311/

\_\_pycache\_\_/

\*.pyc



output/

logs/



dbt\_project/target/

dbt\_project/logs/

dbt\_project/dbt\_packages/



.DS\_Store

```



\## Interview Talking Points



\### Short Project Pitch



I built an end-to-end ELT pipeline that generates synthetic sales data using Python, lands it in AWS S3, auto-ingests it into Snowflake using Snowpipe, and transforms it into analytics-ready models using dbt. Airflow orchestrates the workflow, and GitHub Actions provides CI/CD validation.



\### Data Engineering Explanation



The pipeline separates ingestion, storage, transformation, and reporting layers. Python handles data generation and file upload, S3 acts as the raw landing zone, Snowpipe performs event-driven ingestion into Snowflake, and dbt handles modular transformations across staging, intermediate, and mart layers.



\### dbt Explanation



The staging layer standardizes and deduplicates raw sales records. The intermediate layer applies reusable business logic such as completed-order flags and discount rate. The mart layer produces the fact table, daily KPI table, customer KPI table, and load audit table.



\### Data Quality Explanation



I added not-null, unique, accepted-values, source freshness, amount reconciliation, and load reconciliation tests. This ensures the data is technically valid and business-consistent before it is used for reporting.



\### Workload Isolation Explanation



I used a balanced workload isolation approach. Development uses a separate warehouse, staging and intermediate transformations share a transformation warehouse, and mart models use a dedicated mart warehouse. This gives cost visibility and workload separation without over-engineering the project.



\### CI/CD Explanation



GitHub Actions validates Python and dbt changes before merge. The CI workflow checks Python imports, dbt parsing, compilation, and build validation. The deployment workflow can run dbt build against Snowflake using GitHub repository secrets.



\## Final Architecture Summary



```text

Airflow

&#x20; └── Orchestrates the pipeline



Python

&#x20; └── Generates synthetic sales data



AWS S3

&#x20; └── Stores raw CSV files



Snowpipe

&#x20; └── Auto-ingests S3 files into Snowflake RAW



Snowflake

&#x20; └── Stores raw, staging, intermediate, and mart tables



dbt

&#x20; └── Builds transformation models and tests



GitHub Actions

&#x20; └── Provides CI/CD validation

```



\## Current Status



Implemented:



```text

Python synthetic sales generator

AWS S3 upload

Snowpipe auto-ingestion

Snowflake raw table

dbt staging model

dbt intermediate model

dbt mart models

dbt tests

dbt source freshness

dbt exposures

dbt selectors

Airflow orchestration

GitHub Actions CI/CD

```



Possible future enhancements:



```text

Add Great Expectations or Soda checks

Add Slack or email alerting

Add dashboard in Power BI, Tableau, or Streamlit

Add Terraform for AWS and Snowflake infrastructure

Add dbt Cloud deployment

Add dimensional product and date models

Add incremental backfill strategy

Add data lineage screenshots from dbt docs

```



