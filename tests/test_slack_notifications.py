"""
Tests for Slack notification module.
Tests: notification sending, callbacks, message formatting.
"""

import os
import sys
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "plugins"))


class TestSlackNotifications:
    """Tests for Slack notification functions."""

    @patch("slack_notifications.SLACK_WEBHOOK_URL", "")
    def test_skips_when_no_webhook(self):
        """Should skip notification when webhook URL is empty."""
        from slack_notifications import send_slack_notification
        # Should not raise any exception
        send_slack_notification("Test message", status="info")

    @patch("slack_notifications.SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK")
    def test_skips_placeholder_webhook(self):
        """Should skip notification when using placeholder webhook."""
        from slack_notifications import send_slack_notification
        send_slack_notification("Test message", status="info")

    @patch("slack_notifications.SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/REAL/WEBHOOK/URL")
    @patch("slack_notifications.requests.post")
    def test_sends_notification_successfully(self, mock_post):
        """Should send notification when webhook is configured."""
        from slack_notifications import send_slack_notification
        mock_post.return_value = MagicMock(status_code=200)

        send_slack_notification(
            message="Test notification",
            status="success",
            dag_id="test_dag",
            task_id="test_task"
        )

        mock_post.assert_called_once()

    @patch("slack_notifications.SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/REAL/WEBHOOK/URL")
    @patch("slack_notifications.requests.post")
    def test_includes_error_in_failure_notification(self, mock_post):
        """Should include error details for failure notifications."""
        from slack_notifications import send_slack_notification
        mock_post.return_value = MagicMock(status_code=200)

        send_slack_notification(
            message="Task failed",
            status="failure",
            error="ValueError: something broke"
        )

        call_args = mock_post.call_args
        payload = call_args.kwargs.get("json") or call_args[1].get("json")
        # Check blocks contain error section
        blocks_str = str(payload.get("blocks", []))
        assert "ValueError" in blocks_str

    @patch("slack_notifications.SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/REAL/WEBHOOK/URL")
    @patch("slack_notifications.requests.post")
    def test_handles_post_failure(self, mock_post):
        """Should handle HTTP errors gracefully."""
        from slack_notifications import send_slack_notification
        mock_post.return_value = MagicMock(status_code=500, text="Internal Server Error")

        # Should not raise exception
        send_slack_notification("Test", status="info")

    @patch("slack_notifications.SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/REAL/WEBHOOK/URL")
    @patch("slack_notifications.requests.post")
    def test_handles_network_error(self, mock_post):
        """Should handle network errors gracefully."""
        from slack_notifications import send_slack_notification
        mock_post.side_effect = Exception("Network error")

        # Should not raise exception
        send_slack_notification("Test", status="info")


class TestAirflowCallbacks:
    """Tests for Airflow callback functions."""

    @patch("slack_notifications.send_slack_notification")
    def test_on_success_callback(self, mock_notify):
        """Should send success notification."""
        from slack_notifications import on_success_callback

        mock_dag = MagicMock()
        mock_dag.dag_id = "test_dag"
        mock_ti = MagicMock()
        mock_ti.task_id = "test_task"

        context = {
            "dag": mock_dag,
            "task_instance": mock_ti,
            "execution_date": "2025-01-01"
        }

        on_success_callback(context)
        mock_notify.assert_called_once()
        call_kwargs = mock_notify.call_args.kwargs
        assert call_kwargs["status"] == "success"

    @patch("slack_notifications.send_slack_notification")
    def test_on_failure_callback(self, mock_notify):
        """Should send failure notification with error."""
        from slack_notifications import on_failure_callback

        mock_dag = MagicMock()
        mock_dag.dag_id = "test_dag"
        mock_ti = MagicMock()
        mock_ti.task_id = "test_task"

        context = {
            "dag": mock_dag,
            "task_instance": mock_ti,
            "execution_date": "2025-01-01",
            "exception": ValueError("Test error")
        }

        on_failure_callback(context)
        mock_notify.assert_called_once()
        call_kwargs = mock_notify.call_args.kwargs
        assert call_kwargs["status"] == "failure"

    @patch("slack_notifications.send_slack_notification")
    def test_on_retry_callback(self, mock_notify):
        """Should send retry notification."""
        from slack_notifications import on_retry_callback

        mock_dag = MagicMock()
        mock_dag.dag_id = "test_dag"
        mock_ti = MagicMock()
        mock_ti.task_id = "test_task"
        mock_ti.try_number = 2

        context = {
            "dag": mock_dag,
            "task_instance": mock_ti,
            "execution_date": "2025-01-01",
        }

        on_retry_callback(context)
        mock_notify.assert_called_once()
        call_kwargs = mock_notify.call_args.kwargs
        assert call_kwargs["status"] == "warning"
