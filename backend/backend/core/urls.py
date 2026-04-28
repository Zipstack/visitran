import os

from django.conf import settings
from django.conf.urls import include
from django.urls import path
from rest_framework.decorators import api_view
from rest_framework.response import Response

from backend.core.health_check import health_check
from backend.core.views import (
    get_aggregations_list,
    get_datasource_fields,
    get_datasource_list,
    get_formula_list,
    get_user_profile,
    update_user_profile,
)


@api_view(["GET"])
def _get_feature_flags(request):
    """Return current feature flags, reading fresh from environment."""
    return Response({
        "enable_direct_execution": os.getenv("VISITRAN_ENABLE_DIRECT_EXECUTION", "false").strip().lower() == "true",
        "execution_mode": os.getenv("VISITRAN_EXECUTION_MODE", "legacy").strip().lower(),
        "suppress_python_files": os.getenv("VISITRAN_SUPPRESS_PYTHON_FILES", "false").strip().lower() == "true",
    })


urlpatterns = [
    # server maintenance API
    path(f"{settings.PATH_PREFIX}/health", health_check, name="health-check"),
    # Routers
    path("token", include("backend.core.routers.api_tokens.urls")),
    path("connection", include("backend.core.routers.connection.urls")),
    path("environment", include("backend.core.routers.environment.urls")),
    path("project", include("backend.core.routers.projects.urls")),
    path(
        "project/<str:project_id>/no_code_model/",
        include("backend.core.routers.transformation.urls"),
    ),
    path("onboarding/", include("backend.core.routers.onboarding.urls")),
    path(
        "project/<str:project_id>/connection",
        include("backend.core.routers.project_connection.urls"),
    ),
    path(
        "project/<str:project_id>/explorer",
        include("backend.core.routers.explorer.urls"),
    ),
    path(
        "project/<str:project_id>/execute/direct/",
        include("backend.core.routers.direct_execution.urls"),
    ),
    path(
        "project/<str:project_id>/execute", include("backend.core.routers.execute.urls")
    ),
    # Chat
    path("project/<str:project_id>/chat", include("backend.core.routers.chat.urls")),
    path(
        "project/<str:project_id>/chat-intent",
        include("backend.core.routers.chat_intent.urls"),
    ),
    path(
        "project/<str:project_id>/chat/<str:chat_id>/chat-message",
        include("backend.core.routers.chat_message.urls"),
    ),
    # Security (with tenant prefix)
    path("security", include("backend.core.routers.security.urls")),
    # # Payments (Stripe Integration)
    # path("payments/", include("pluggable_apps.subscriptions.routers.payments.urls")),
    # # Webhooks (API Key Authentication)
    # path("webhooks/", include("pluggable_apps.subscriptions.routers.webhooks.urls")),
    # AI Context Rules
    path("ai-context/", include("backend.core.routers.ai_context.urls")),
    # # Urls about static list data's
    path("datasource", get_datasource_list, name="get-datasource-list"),
    path("profile", get_user_profile, name="get_user_profile"),
    path("profile/update", update_user_profile, name="update_user_profile"),
    path(
        "datasource/<str:datasource_name>/fields",
        get_datasource_fields,
        name="get-datasource-fields",
    ),
    path("aggregations", get_aggregations_list, name="get-available-aggregations"),
    path("formulas", get_formula_list, name="get-available-formulas"),
    # Feature flags endpoint (FE checks execution mode on mount)
    path("feature-flags", _get_feature_flags, name="feature-flags"),
]

try:
    SLACK_INTEGRATION = path("slack", include("pluggable_apps.slack_integration.urls"))
    urlpatterns.append(SLACK_INTEGRATION)
except (ModuleNotFoundError, RuntimeError):
    # RuntimeError occurs when model's app is not in INSTALLED_APPS
    print("Slack Integration Module does not exist.")
