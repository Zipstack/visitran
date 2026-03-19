"""URL patterns for account module - authentication endpoints."""

from django.urls import path

from backend.account.views import (
    callback,
    create_organization,
    forgot_password,
    get_organizations,
    get_session_data,
    landing,
    login,
    logout,
    reset_password,
    set_organization,
    signup,
    validate_reset_token,
)

urlpatterns = [
    # Landing page
    path("landing", landing, name="landing"),

    # Authentication endpoints
    path("signup", signup, name="signup"),
    path("login", login, name="login"),
    path("logout", logout, name="logout"),
    path("callback", callback, name="callback"),
    path("forgot-password", forgot_password, name="forgot-password"),
    path("reset-password", reset_password, name="reset-password"),
    path("validate-reset-token", validate_reset_token, name="validate-reset-token"),

    # Session endpoints
    path("session", get_session_data, name="session"),

    # Organization endpoints
    path("organization", get_organizations, name="get_organizations"),
    path("organization/<str:id>/set", set_organization, name="set_organization"),
    path("organization/create", create_organization, name="create_organization"),
]
