from django.apps import AppConfig
from django.conf import settings
from django.db import connection
from django.db.utils import ProgrammingError, OperationalError


class CoreAppConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "backend.core"

    def ready(self):

        from backend.execution_log_utils import (
            create_log_consumer_scheduler_if_not_exists,
        )

        try:
            # Check if the app is ready and migrations are not running
            if connection.introspection.table_names():  # Ensures DB is initialized
                create_log_consumer_scheduler_if_not_exists()

            # Start the web socket server if the manager explicitly set to eventlet
            if settings.WEBSOCKET_MANAGER == "eventlet":
                from threading import Thread
                from backend.core.web_socket import run_socket_server

                thread = Thread(target=run_socket_server, daemon=True)
                thread.start()
        except (ProgrammingError, OperationalError):
            # Database is not fully migrated yet; skip task scheduling
            pass
