"""
API resources for the Dagster platform.
"""

from typing import Any, Dict, Optional

import requests
from dagster import ConfigurableResource, resource
from pydantic import Field


class HTTPResource(ConfigurableResource):
    """Resource for making HTTP requests to APIs."""
    
    base_url: str = Field(description="Base URL for API requests")
    timeout: int = Field(default=30, description="Request timeout in seconds")
    headers: Optional[Dict[str, str]] = Field(default=None, description="Default headers")
    auth_token: Optional[str] = Field(default=None, description="Authentication token")
    
    def get_headers(self) -> Dict[str, str]:
        """Get request headers with authentication."""
        headers = self.headers or {}
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"
        return headers
    
    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
        """Make a GET request to the API."""
        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        response = requests.get(
            url,
            params=params,
            headers=self.get_headers(),
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response
    
    def post(self, endpoint: str, data: Optional[Dict[str, Any]] = None) -> requests.Response:
        """Make a POST request to the API."""
        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        response = requests.post(
            url,
            json=data,
            headers=self.get_headers(),
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response
    
    def put(self, endpoint: str, data: Optional[Dict[str, Any]] = None) -> requests.Response:
        """Make a PUT request to the API."""
        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        response = requests.put(
            url,
            json=data,
            headers=self.get_headers(),
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response
    
    def delete(self, endpoint: str) -> requests.Response:
        """Make a DELETE request to the API."""
        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        response = requests.delete(
            url,
            headers=self.get_headers(),
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response


class SlackResource(ConfigurableResource):
    """Resource for sending messages to Slack."""
    
    webhook_url: str = Field(description="Slack webhook URL")
    channel: Optional[str] = Field(default=None, description="Default channel")
    username: Optional[str] = Field(default="Dagster", description="Bot username")
    
    def send_message(self, message: str, channel: Optional[str] = None) -> None:
        """Send a message to Slack."""
        payload = {
            "text": message,
            "username": self.username,
        }
        
        if channel or self.channel:
            payload["channel"] = channel or self.channel
        
        response = requests.post(self.webhook_url, json=payload)
        response.raise_for_status()
    
    def send_alert(self, title: str, message: str, color: str = "danger") -> None:
        """Send an alert message with formatting."""
        payload = {
            "attachments": [
                {
                    "color": color,
                    "title": title,
                    "text": message,
                    "footer": "Dagster Platform",
                    "ts": int(pd.Timestamp.now().timestamp()),
                }
            ],
            "username": self.username,
        }
        
        if self.channel:
            payload["channel"] = self.channel
        
        response = requests.post(self.webhook_url, json=payload)
        response.raise_for_status()


class EmailResource(ConfigurableResource):
    """Resource for sending emails via SMTP."""
    
    smtp_server: str = Field(description="SMTP server hostname")
    smtp_port: int = Field(default=587, description="SMTP server port")
    username: str = Field(description="SMTP username")
    password: str = Field(description="SMTP password")
    from_email: str = Field(description="From email address")
    use_tls: bool = Field(default=True, description="Use TLS encryption")
    
    def send_email(self, to_emails: list[str], subject: str, body: str, is_html: bool = False) -> None:
        """Send an email to the specified recipients."""
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        
        msg = MIMEMultipart()
        msg["From"] = self.from_email
        msg["To"] = ", ".join(to_emails)
        msg["Subject"] = subject
        
        msg.attach(MIMEText(body, "html" if is_html else "plain"))
        
        with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
            if self.use_tls:
                server.starttls()
            server.login(self.username, self.password)
            server.send_message(msg)


# Resource factory functions for backward compatibility
@resource(description="HTTP API resource for making requests")
def http_resource(context) -> HTTPResource:
    """Legacy resource factory for HTTPResource."""
    return HTTPResource(
        base_url=context.resource_config["base_url"],
        timeout=context.resource_config.get("timeout", 30),
        headers=context.resource_config.get("headers"),
        auth_token=context.resource_config.get("auth_token"),
    )


@resource(description="Slack resource for sending notifications")
def slack_resource(context) -> SlackResource:
    """Legacy resource factory for SlackResource."""
    return SlackResource(
        webhook_url=context.resource_config["webhook_url"],
        channel=context.resource_config.get("channel"),
        username=context.resource_config.get("username", "Dagster"),
    )