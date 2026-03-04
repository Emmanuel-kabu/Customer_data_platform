"""
Slack notification helpers for Airflow DAGs.
Sends pipeline status alerts to Slack channels.
"""

import os
import json
import logging
from datetime import datetime

import requests

logger = logging.getLogger(__name__)

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL", "#data-platform-alerts")


def send_slack_notification(
    message: str,
    status: str = "info",
    dag_id: str = "",
    task_id: str = "",
    execution_date: str = "",
    error: str = ""
):
    """
    Send a formatted notification to Slack.

    Args:
        message: Main notification message
        status: One of 'success', 'failure', 'warning', 'info'
        dag_id: DAG identifier
        task_id: Task identifier
        execution_date: Pipeline execution date
        error: Error details if applicable
    """
    if not SLACK_WEBHOOK_URL or SLACK_WEBHOOK_URL.startswith("https://hooks.slack.com/services/YOUR"):
        logger.warning("Slack webhook URL not configured. Skipping notification.")
        return

    status_emoji = {
        "success": ":white_check_mark:",
        "failure": ":x:",
        "warning": ":warning:",
        "info": ":information_source:"
    }

    status_color = {
        "success": "#36a64f",
        "failure": "#ff0000",
        "warning": "#ffcc00",
        "info": "#439FE0"
    }

    emoji = status_emoji.get(status, ":bell:")

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{emoji} CDP Pipeline Alert - {status.upper()}"
            }
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*DAG:*\n{dag_id or 'N/A'}"},
                {"type": "mrkdwn", "text": f"*Task:*\n{task_id or 'N/A'}"},
                {"type": "mrkdwn", "text": f"*Execution Date:*\n{execution_date or 'N/A'}"},
                {"type": "mrkdwn", "text": f"*Timestamp:*\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"}
            ]
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Message:*\n{message}"
            }
        }
    ]

    if error:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Error Details:*\n```{error[:500]}```"
            }
        })

    payload = {
        "channel": SLACK_CHANNEL,
        "username": "CDP Pipeline Bot",
        "icon_emoji": ":robot_face:",
        "blocks": blocks
    }

    try:
        response = requests.post(
            SLACK_WEBHOOK_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        if response.status_code != 200:
            logger.error(f"Slack notification failed: {response.status_code} - {response.text}")
        else:
            logger.info(f"Slack notification sent successfully: {status}")
    except Exception as e:
        logger.error(f"Failed to send Slack notification: {e}")


def on_success_callback(context):
    """Airflow callback for successful task completion."""
    dag_id = context.get("dag", {}).dag_id if context.get("dag") else ""
    task_id = context.get("task_instance", {}).task_id if context.get("task_instance") else ""
    execution_date = str(context.get("execution_date", ""))

    send_slack_notification(
        message=f"Task `{task_id}` in DAG `{dag_id}` completed successfully.",
        status="success",
        dag_id=dag_id,
        task_id=task_id,
        execution_date=execution_date
    )


def on_failure_callback(context):
    """Airflow callback for failed task."""
    dag_id = context.get("dag", {}).dag_id if context.get("dag") else ""
    task_id = context.get("task_instance", {}).task_id if context.get("task_instance") else ""
    execution_date = str(context.get("execution_date", ""))
    exception = str(context.get("exception", "Unknown error"))

    send_slack_notification(
        message=f"Task `{task_id}` in DAG `{dag_id}` FAILED!",
        status="failure",
        dag_id=dag_id,
        task_id=task_id,
        execution_date=execution_date,
        error=exception
    )


def on_retry_callback(context):
    """Airflow callback for task retry."""
    dag_id = context.get("dag", {}).dag_id if context.get("dag") else ""
    task_id = context.get("task_instance", {}).task_id if context.get("task_instance") else ""
    execution_date = str(context.get("execution_date", ""))
    try_number = context.get("task_instance", {}).try_number if context.get("task_instance") else 0

    send_slack_notification(
        message=f"Task `{task_id}` in DAG `{dag_id}` is retrying (attempt {try_number}).",
        status="warning",
        dag_id=dag_id,
        task_id=task_id,
        execution_date=execution_date
    )
