"""conftest.py for unit tests — minimal Django settings for local runs.

In CI, pytest-django reads DJANGO_SETTINGS_MODULE from the workflow env.
Locally, this fallback configures just enough settings for visitran imports.
"""
import django.conf

if not django.conf.settings.configured:
    django.conf.settings.configure(
        GS_BUCKET_NAME="test-bucket",
        CELERY_BROKER_URL="memory://",
        DATABASES={},
        INSTALLED_APPS=[],
        DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
        SECRET_KEY="test-secret-key",
        CACHES={"default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache"}},
    )
