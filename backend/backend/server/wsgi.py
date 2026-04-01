"""WSGI config for server project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/4.1/howto/deployment/wsgi/
"""

import os

from django.conf import settings
from django.core.wsgi import get_wsgi_application

from backend.core.web_socket import start_server

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "backend.server.settings.dev")

django_app = get_wsgi_application()

application = start_server(django_app, f"{settings.PATH_PREFIX}/socket")
