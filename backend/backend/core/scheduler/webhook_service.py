"""Outbound webhook delivery for run completion notifications."""

import hashlib
import hmac
import json
import logging
import time

import requests

logger = logging.getLogger(__name__)


def send_webhook(webhook_url, payload_dict, webhook_secret=None, max_retries=3):
    """POST JSON payload to webhook_url with optional HMAC-SHA256 signature.

    Args:
        webhook_url: Target URL to POST to.
        payload_dict: Dict to JSON-serialize and send.
        webhook_secret: If provided, compute HMAC-SHA256 and attach as
            ``X-Visitran-Signature`` header.
        max_retries: Number of delivery attempts (with exponential backoff).

    Returns:
        True if the webhook was delivered successfully, False otherwise.
    """
    payload_json = json.dumps(payload_dict, default=str)

    headers = {"Content-Type": "application/json"}
    if webhook_secret:
        sig = hmac.new(
            webhook_secret.encode(), payload_json.encode(), hashlib.sha256
        ).hexdigest()
        headers["X-Visitran-Signature"] = f"sha256={sig}"

    for attempt in range(max_retries):
        try:
            resp = requests.post(
                webhook_url, data=payload_json, headers=headers, timeout=10
            )
            if resp.status_code < 400:
                logger.info(
                    "Webhook delivered to %s (status=%d)", webhook_url, resp.status_code
                )
                return True
            logger.warning(
                "Webhook %s returned %d (attempt %d/%d)",
                webhook_url,
                resp.status_code,
                attempt + 1,
                max_retries,
            )
        except requests.RequestException as e:
            logger.warning("Webhook attempt %d/%d failed: %s", attempt + 1, max_retries, e)

        if attempt < max_retries - 1:
            time.sleep(2 ** attempt)  # 1s, 2s backoff

    logger.error("Webhook to %s failed after %d attempts", webhook_url, max_retries)
    return False
