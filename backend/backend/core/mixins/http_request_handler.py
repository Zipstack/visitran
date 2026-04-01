import logging
import traceback

from django.http import JsonResponse

from backend.errors.exceptions import VisitranBackendBaseException


class RequestHandlingMixin:
    """Global exception handling and response formatting."""

    def dispatch(self, request, *args, **kwargs):
        try:
            return super().dispatch(request, *args, **kwargs)

        except VisitranBackendBaseException as e:
            logging.error(f"Backend Exception: {e.__class__.__name__}")
            logging.error(f"Details: {repr(e)}, Args: {e.error_args()}")
            logging.error(traceback.format_exc())

            return e.to_response()

        except Exception as e:
            logging.critical(f"Unhandled Exception: {repr(e)}")
            logging.critical(traceback.format_exc())

            return JsonResponse(
                {"status": "error", "message": "An unexpected error occurred"},
                status=500,
            )

        finally:
            if hasattr(request, "resolver_match") and request.resolver_match:
                logging.info(f"Completed request processing for {request.resolver_match.view_name}")
