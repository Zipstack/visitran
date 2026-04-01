"""API Key audit logging dispatcher.

On OSS, this is a no-op. On Cloud, pluggable_apps.api_key_audit.service
provides the real implementation that writes to the APIKeyAuditLog DB
table.
"""

try:
    from pluggable_apps.api_key_audit.service import log_api_key_event
except ImportError:

    def log_api_key_event(*args, **kwargs):
        # OSS mode: audit logging is a cloud-only feature, intentional no-op
        pass
