import os
import sys

# Ensure backend/ is on sys.path so both `visitran.*` and `backend.*` are importable
backend_dir = os.path.join(os.path.dirname(__file__), "..", "..", "backend")
backend_dir = os.path.abspath(backend_dir)
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

# Configure Django before any Django-dependent import
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "backend.server.settings.dev")

import django  # noqa: E402

django.setup()
