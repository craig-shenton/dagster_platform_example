"""
Notification hooks for alerts and messaging.
"""

from typing import List, Optional

from dagster import HookContext, failure_hook, success_hook

from platform_core.resources.api_resources import SlackResource, EmailResource


@failure_hook
def slack_failure_hook(context: HookContext) -> None:
    """Send Slack notification on failure."""
    
    # Get Slack configuration from run config or environment
    slack_webhook_url = context.dagster_run.tags.get("slack_webhook_url")
    slack_channel = context.dagster_run.tags.get("slack_channel")
    
    if not slack_webhook_url:
        context.log.warning("Slack webhook URL not configured, skipping notification")
        return
    
    # Create Slack resource
    slack = SlackResource(
        webhook_url=slack_webhook_url,
        channel=slack_channel,
    )
    
    # Format failure message
    message = f"""
:x: *Dagster Job Failed*

*Job:* {context.dagster_run.job_name}
*Run ID:* {context.run_id}
*Asset/Op:* {context.step_key}
*Error:* {context.failure_data}
*Time:* {context.dagster_run.end_time}

<{context.dagster_run.external_job_origin.external_repository_origin.repository_location_origin.location_name}|View in Dagster>
    """.strip()
    
    try:
        slack.send_alert(
            title="Dagster Job Failed",
            message=message,
            color="danger",
        )
        context.log.info("Slack failure notification sent")
    except Exception as e:
        context.log.error(f"Failed to send Slack notification: {e}")


@success_hook
def slack_success_hook(context: HookContext) -> None:
    """Send Slack notification on success (for important jobs)."""
    
    # Only send success notifications for jobs tagged as important
    if "notify_on_success" not in context.dagster_run.tags:
        return
    
    # Get Slack configuration
    slack_webhook_url = context.dagster_run.tags.get("slack_webhook_url")
    slack_channel = context.dagster_run.tags.get("slack_channel")
    
    if not slack_webhook_url:
        context.log.warning("Slack webhook URL not configured, skipping notification")
        return
    
    # Create Slack resource
    slack = SlackResource(
        webhook_url=slack_webhook_url,
        channel=slack_channel,
    )
    
    # Calculate execution time
    start_time = context.dagster_run.start_time
    end_time = context.dagster_run.end_time
    execution_time = (end_time - start_time).total_seconds() if start_time and end_time else 0
    
    # Format success message
    message = f"""
:white_check_mark: *Dagster Job Completed Successfully*

*Job:* {context.dagster_run.job_name}
*Run ID:* {context.run_id}
*Asset/Op:* {context.step_key}
*Execution Time:* {execution_time:.2f}s
*Completed:* {context.dagster_run.end_time}

<{context.dagster_run.external_job_origin.external_repository_origin.repository_location_origin.location_name}|View in Dagster>
    """.strip()
    
    try:
        slack.send_alert(
            title="Dagster Job Completed",
            message=message,
            color="good",
        )
        context.log.info("Slack success notification sent")
    except Exception as e:
        context.log.error(f"Failed to send Slack notification: {e}")


@failure_hook
def email_failure_hook(context: HookContext) -> None:
    """Send email notification on failure."""
    
    # Get email configuration from run config or environment
    email_recipients = context.dagster_run.tags.get("email_recipients", "").split(",")
    email_recipients = [email.strip() for email in email_recipients if email.strip()]
    
    if not email_recipients:
        context.log.warning("Email recipients not configured, skipping notification")
        return
    
    # Get SMTP configuration (in a real implementation, this would come from config)
    smtp_config = {
        "smtp_server": context.dagster_run.tags.get("smtp_server", "localhost"),
        "smtp_port": int(context.dagster_run.tags.get("smtp_port", "587")),
        "username": context.dagster_run.tags.get("smtp_username", ""),
        "password": context.dagster_run.tags.get("smtp_password", ""),
        "from_email": context.dagster_run.tags.get("from_email", "dagster@example.com"),
    }
    
    # Create email resource
    email = EmailResource(**smtp_config)
    
    # Format email content
    subject = f"Dagster Job Failed: {context.dagster_run.job_name}"
    body = f"""
<h2>Dagster Job Failed</h2>

<p><strong>Job:</strong> {context.dagster_run.job_name}</p>
<p><strong>Run ID:</strong> {context.run_id}</p>
<p><strong>Asset/Op:</strong> {context.step_key}</p>
<p><strong>Error:</strong> {context.failure_data}</p>
<p><strong>Time:</strong> {context.dagster_run.end_time}</p>

<p>Please check the Dagster UI for more details.</p>
    """.strip()
    
    try:
        email.send_email(
            to_emails=email_recipients,
            subject=subject,
            body=body,
            is_html=True,
        )
        context.log.info(f"Email failure notification sent to {len(email_recipients)} recipients")
    except Exception as e:
        context.log.error(f"Failed to send email notification: {e}")


def create_custom_notification_hook(
    hook_name: str,
    notification_function: callable,
    on_success: bool = False,
    on_failure: bool = True,
):
    """
    Create a custom notification hook.
    
    Args:
        hook_name: Name of the hook
        notification_function: Function to send notifications
        on_success: Whether to trigger on success
        on_failure: Whether to trigger on failure
    
    Returns:
        Configured notification hook(s)
    """
    hooks = []
    
    if on_success:
        @success_hook(name=f"{hook_name}_success")
        def custom_success_notification_hook(context: HookContext) -> None:
            """Custom success notification hook."""
            try:
                notification_function(context, event_type="success")
            except Exception as e:
                context.log.error(f"Custom success notification hook {hook_name} failed: {e}")
        
        hooks.append(custom_success_notification_hook)
    
    if on_failure:
        @failure_hook(name=f"{hook_name}_failure")
        def custom_failure_notification_hook(context: HookContext) -> None:
            """Custom failure notification hook."""
            try:
                notification_function(context, event_type="failure")
            except Exception as e:
                context.log.error(f"Custom failure notification hook {hook_name} failed: {e}")
        
        hooks.append(custom_failure_notification_hook)
    
    return hooks if len(hooks) > 1 else hooks[0]


def create_pagerduty_hook(
    integration_key: str,
    hook_name: str = "pagerduty_alert",
    severity: str = "error",
):
    """
    Create a PagerDuty integration hook for critical alerts.
    
    Args:
        integration_key: PagerDuty integration key
        hook_name: Name of the hook
        severity: Alert severity level
    
    Returns:
        Configured PagerDuty hook
    """
    
    @failure_hook(name=hook_name)
    def pagerduty_alert_hook(context: HookContext) -> None:
        """Send PagerDuty alert on failure."""
        
        import requests
        
        # PagerDuty Events API payload
        payload = {
            "routing_key": integration_key,
            "event_action": "trigger",
            "payload": {
                "summary": f"Dagster Job Failed: {context.dagster_run.job_name}",
                "source": "dagster",
                "severity": severity,
                "custom_details": {
                    "job_name": context.dagster_run.job_name,
                    "run_id": context.run_id,
                    "step_key": context.step_key,
                    "failure_data": str(context.failure_data),
                    "failure_time": str(context.dagster_run.end_time),
                },
            },
        }
        
        try:
            response = requests.post(
                "https://events.pagerduty.com/v2/enqueue",
                json=payload,
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()
            context.log.info("PagerDuty alert sent successfully")
        except Exception as e:
            context.log.error(f"Failed to send PagerDuty alert: {e}")
    
    return pagerduty_alert_hook