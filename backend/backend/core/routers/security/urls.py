from django.urls import path

from backend.core.routers.security.views import get_public_key

urlpatterns = [
    path("/public-key", get_public_key, name="get-public-key"),
]
