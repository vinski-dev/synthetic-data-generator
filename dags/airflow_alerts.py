from __future__ import annotations

import os
from typing import Any

import requests


def send_slack_failure_alert(context: dict[str, Any]) -> None:
    """
    Send a Slack alert when an Airflow task fails.

    This uses SLACK_WEBHOOK_URL from environment variables.
    """

    webhook_url = os.getenv("SLACK_WEBHOOK_URL")

    if not webhook_url:
        print("SLACK_WEBHOOK_URL is not configured. Skipping Slack alert.")
        return

    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")
    exception = context.get("exception")

    dag_id = task_instance.dag_id if task_instance else "unknown_dag"
    task_id = task_instance.task_id if task_instance else "unknown_task"
    execution_date = context.get("logical_date")
    try_number = task_instance.try_number if task_instance else "unknown"
    log_url = task_instance.log_url if task_instance else "No log URL available"
    run_id = dag_run.run_id if dag_run else "unknown_run_id"

    message = {
        "text": (
            ":red_circle: *Airflow Task Failed*\n\n"
            f"*DAG:* `{dag_id}`\n"
            f"*Task:* `{task_id}`\n"
            f"*Run ID:* `{run_id}`\n"
            f"*Logical Date:* `{execution_date}`\n"
            f"*Try Number:* `{try_number}`\n"
            f"*Error:* `{exception}`\n"
            f"*Logs:* {log_url}"
        )
    }

    response = requests.post(
        webhook_url,
        json=message,
        timeout=10,
    )

    if response.status_code >= 400:
        raise RuntimeError(
            f"Slack alert failed with status={response.status_code}, body={response.text}"
        )