from __future__ import annotations

import os
from datetime import datetime

from airflow.decorators import dag, task


DEFAULT_ALERT_EMAIL = os.getenv("AIRFLOW_ALERT_EMAIL", "").strip()

DEFAULT_ARGS = {
    "email_on_failure": bool(DEFAULT_ALERT_EMAIL),
    "email_on_retry": False,
    "retries": 0,
}

if DEFAULT_ALERT_EMAIL:
    DEFAULT_ARGS["email"] = [DEFAULT_ALERT_EMAIL]


@dag(
    dag_id="test_email_alert_dag",
    description="Temporary DAG to test Airflow SMTP email failure alerts.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["test", "alerting", "email"],
)
def test_email_alert_dag():
    @task
    def intentionally_fail() -> None:
        raise RuntimeError("Testing Airflow SMTP email failure alert.")

    intentionally_fail()


test_email_alert_dag()
