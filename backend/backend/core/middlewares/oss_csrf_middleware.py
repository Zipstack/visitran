"""Custom CSRF middleware for OSS that exempts authentication endpoints."""

from django.middleware.csrf import CsrfViewMiddleware


class OSSCsrfMiddleware(CsrfViewMiddleware):
    """CSRF middleware that exempts authentication endpoints.

    In OSS mode, login/signup/logout endpoints need to work without
    CSRF tokens since they're called before a session exists.
    """

    EXEMPT_PATHS = [
        "/api/v1/login",
        "/api/v1/logout",
        "/api/v1/signup",
        "/api/v1/callback",
        "/api/v1/session",
        "/api/v1/forgot-password",
        "/api/v1/reset-password",
    ]

    def _should_exempt(self, request):
        """Check if the request path should be exempt from CSRF."""
        return any(request.path.startswith(path) for path in self.EXEMPT_PATHS)

    def process_view(self, request, callback, callback_args, callback_kwargs):
        """Skip CSRF check for exempt paths."""
        if self._should_exempt(request):
            return None
        return super().process_view(request, callback, callback_args, callback_kwargs)
