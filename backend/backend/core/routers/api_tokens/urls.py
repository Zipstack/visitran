from django.urls import path

from backend.core.routers.api_tokens.views import (
    create_api_key,
    delete_api_key,
    generate_token,
    get_api_key,
    list_api_keys,
    regenerate_api_key,
    toggle_api_key,
)

urlpatterns = [
    # Legacy endpoint
    path("/generate", generate_token, name="generate_token"),
    # CRUD endpoints
    path("/api-keys", list_api_keys, name="list_api_keys"),
    path("/api-keys/create", create_api_key, name="create_api_key"),
    path("/api-keys/<str:key_id>", get_api_key, name="get_api_key"),
    path("/api-keys/<str:key_id>/delete", delete_api_key, name="delete_api_key"),
    path("/api-keys/<str:key_id>/toggle", toggle_api_key, name="toggle_api_key"),
    path("/api-keys/<str:key_id>/regenerate", regenerate_api_key, name="regenerate_api_key"),
]
