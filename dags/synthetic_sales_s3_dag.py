from __future__ import annotations

import os
import subprocess
import time
from datetime import datetime
from urllib.parse import urlparse

import snowflake.connector
from airflow.decorators import dag, task

from pipeline.sales_pipeline import (
    generate_sales_data,
    validate_sales_file,
    upload_file_to_s3,
)


DBT_PROJECT_DIR = "/opt/airflow/dbt_project"


def run_command(command: list[str], cwd: str | None = None) -> None:
    """
    Run shell command safely and fail the Airflow task if the command fails.
    """

    result = subprocess.run(
        command,
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )

    print(result.stdout)

    if result.stderr:
        print(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed with exit code {result.returncode}: {' '.join(command)}"
        )


@dag(
    dag_id="synthetic_sales_full_elt",
    description="Generate sales data, upload to S3, wait for Snowpipe, then run dbt transformations.",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["synthetic-data", "s3", "snowpipe", "snowflake", "dbt"],
)
def synthetic_sales_full_elt():

    @task
    def generate_task() -> str:
        return generate_sales_data()

    @task
    def validate_task(local_path: str) -> str:
        return validate_sales_file(local_path)

    @task
    def upload_task(local_path: str) -> str:
        return upload_file_to_s3(local_path)

    @task
    def wait_for_snowpipe_task(s3_uri: str) -> str:
        """
        Wait until Snowpipe loads the uploaded S3 file into RAW.SALES_RAW.
        """

        parsed = urlparse(s3_uri)
        file_name = os.path.basename(parsed.path)

        print(f"Waiting for Snowpipe to load file: {file_name}")

        conn = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            role=os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "WH_DBT_DEV"),
            database=os.environ.get("SNOWFLAKE_DATABASE", "SYNTHETIC_DATA"),
            schema="RAW",
        )

        try:
            cursor = conn.cursor()

            max_attempts = 60
            sleep_seconds = 20
           

            for attempt in range(1, max_attempts + 1):
                print(f"Snowpipe check attempt {attempt}/{max_attempts}")

                cursor.execute(
                    """
                    SELECT COUNT(*) AS row_count
                    FROM SYNTHETIC_DATA.RAW.SALES_RAW
                    WHERE SOURCE_FILE_NAME ILIKE %s
                    """,
                    (f"%{file_name}%",),
                )

                row_count = cursor.fetchone()[0]

                print(f"Rows found for {file_name}: {row_count}")

                if row_count > 0:
                    print("Snowpipe load detected.")
                    return file_name

                time.sleep(sleep_seconds)

            raise TimeoutError(
                f"Snowpipe did not load file within expected time: {file_name}"
            )

        finally:
            conn.close()

    @task
    def dbt_source_freshness_task() -> None:
        run_command(
        [
            "dbt",
            "source",
            "freshness",
            "--project-dir",
            DBT_PROJECT_DIR,
            "--profiles-dir",
            DBT_PROJECT_DIR,
            "--no-partial-parse",
        ],
        cwd=DBT_PROJECT_DIR,
    )


    @task
    def dbt_build_task() -> None:
        run_command(
        [
            "dbt",
            "build",
            "--selector",
            "transform_pipeline",
            "--project-dir",
            DBT_PROJECT_DIR,
            "--profiles-dir",
            DBT_PROJECT_DIR,
            "--no-partial-parse",
        ],
        cwd=DBT_PROJECT_DIR,
    )

    generated_file = generate_task()
    validated_file = validate_task(generated_file)
    uploaded_s3_uri = upload_task(validated_file)

    loaded_file = wait_for_snowpipe_task(uploaded_s3_uri)

    freshness = dbt_source_freshness_task()
    build = dbt_build_task()

    loaded_file >> build
    loaded_file >> freshness

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

    build = dbt_build_task()
    quality = soda_quality_check_task()

    build >> quality
    
synthetic_sales_full_elt()