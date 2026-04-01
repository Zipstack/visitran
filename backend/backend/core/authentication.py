"""Custom authentication classes for Django REST Framework."""

from rest_framework.authentication import SessionAuthentication


class CsrfExemptSessionAuthentication(SessionAuthentication):
    """Session authentication without CSRF enforcement.

    Used in OSS/dev mode where CSRF protection is handled at
    the middleware level for specific paths only.
    """

    def enforce_csrf(self, request):
        """Skip CSRF check - handled by OSSCsrfMiddleware instead."""
        return None
