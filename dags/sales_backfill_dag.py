from __future__ import annotations
from airflow_alerts import send_slack_failure_alert

import os
import subprocess
import time
from datetime import datetime, timedelta
from urllib.parse import urlparse

import snowflake.connector
from airflow.decorators import dag, task
from airflow.sdk import get_current_context

from pipeline.sales_pipeline import (
    generate_sales_data_for_business_date,
    save_backfill_to_csv,
    upload_backfill_to_s3,
    validate_sales_data,
)


DBT_PROJECT_DIR = "/opt/airflow/dbt_project"


def run_command(command: list[str], cwd: str | None = None) -> None:
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
    dag_id="sales_backfill_workflow",
    description="Manual sales backfill workflow for historical business dates.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "on_failure_callback": send_slack_failure_alert,
    },
    tags=["sales", "backfill", "snowpipe", "dbt"],
    params={
        "start_date": "2026-05-01",
        "end_date": "2026-05-03",
        "records_per_day": 1000,
    },
)
def sales_backfill_workflow():

    @task
    def generate_and_upload_backfill_files() -> list[str]:
        context = get_current_context()
        params = context["params"]

        start_date = datetime.strptime(params["start_date"], "%Y-%m-%d").date()
        end_date = datetime.strptime(params["end_date"], "%Y-%m-%d").date()
        records_per_day = int(params["records_per_day"])

        if end_date < start_date:
            raise ValueError("end_date must be greater than or equal to start_date.")

        uploaded_files = []
        current_date = start_date

        while current_date <= end_date:
            print(f"Generating backfill for business date: {current_date}")

            df = generate_sales_data_for_business_date(
                business_date=current_date,
                num_records=records_per_day,
            )

            validate_sales_data(df)

            local_path = save_backfill_to_csv(df, current_date)

            s3_uri = upload_backfill_to_s3(local_path, current_date)

            uploaded_files.append(s3_uri)

            current_date += timedelta(days=1)

        return uploaded_files

    @task
    def wait_for_snowpipe_backfill(uploaded_files: list[str]) -> list[str]:
        expected_files = [
            os.path.basename(urlparse(uri).path) for uri in uploaded_files
        ]

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
                print(f"Snowpipe backfill check attempt {attempt}/{max_attempts}")

                loaded_files = []

                for file_name in expected_files:
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
                        loaded_files.append(file_name)

                if set(loaded_files) == set(expected_files):
                    print("All backfill files detected in Snowflake RAW.")
                    return loaded_files

                time.sleep(sleep_seconds)

            raise TimeoutError("Snowpipe did not load all backfill files in time.")

        finally:
            conn.close()

    @task
    def run_dbt_build() -> None:
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

    uploaded_files = generate_and_upload_backfill_files()
    loaded_files = wait_for_snowpipe_backfill(uploaded_files)
    run_dbt_build_task = run_dbt_build()

    loaded_files >> run_dbt_build_task


sales_backfill_workflow()
