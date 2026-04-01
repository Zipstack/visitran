# base_urls.py
from django.conf import settings
from django.urls import include, path

try:
    from pluggable_apps.urls import urlpatterns as pluggable_urls

    print("Pluggable Module exists and was imported successfully.")
    _has_pluggable_account = True
except Exception as e:
    print("Pluggable Module does not exist.", e)
    pluggable_urls = []
    _has_pluggable_account = False

# Import urlpatterns from each file
from backend.core.urls import urlpatterns as tenant_urls

# Import webhook and payment URLs that need to be accessible at root level
try:
    from pluggable_apps.subscriptions.routers.webhooks.urls import urlpatterns as webhook_urls
except (ImportError, RuntimeError):
    # RuntimeError occurs when model's app is not in INSTALLED_APPS
    webhook_urls = []

try:
    from pluggable_apps.subscriptions.routers.payments.urls import urlpatterns as payment_urls
except (ImportError, RuntimeError):
    # RuntimeError occurs when model's app is not in INSTALLED_APPS
    payment_urls = []

# Combine the URL patterns
urlpatterns = [
    path("", include(tenant_urls)),
    # Add all pluggable apps at root level
    path("", include(pluggable_urls)),
    # Add tenant-based routing for organization-specific endpoints (with tenant namespace)
    path(f"{settings.PATH_PREFIX}/visitran/<str:org_id>/", include((pluggable_urls, "tenant"), namespace="tenant")),
    # Add webhook URLs at root level for external services (Stripe webhooks)
    path(f"{settings.PATH_PREFIX}/webhooks/", include(webhook_urls)),
    # Add payment URLs at root level for external services
    path(f"{settings.PATH_PREFIX}/payments/", include(payment_urls)),
]

# OSS Auth: Register account URLs when pluggable_apps/account is not available
if not _has_pluggable_account:
    from backend.account.urls import urlpatterns as oss_account_urls

    urlpatterns.insert(0, path(f"{settings.PATH_PREFIX}/", include(oss_account_urls)))

# Internal APIs — AI server validate-key + consume-tokens (Cloud only)
try:
    from pluggable_apps.subscriptions.internal_views import consume_tokens, validate_key

    urlpatterns += [
        path(f"{settings.PATH_PREFIX}/internal/validate-key", validate_key, name="validate-key"),
        path(f"{settings.PATH_PREFIX}/internal/consume-tokens", consume_tokens, name="consume-tokens"),
    ]
except ImportError:
    pass  # OSS — no internal endpoints needed
