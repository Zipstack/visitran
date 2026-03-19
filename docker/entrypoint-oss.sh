#!/usr/bin/env bash
set -e

echo "=== Visitran OSS Docker Entrypoint ==="

# --- SQLite volume symlink ---
# dev.py sets DB at BASE_DIR / "db.sqlite3" = /app/backend/server/db.sqlite3
# We symlink to /app/db/ which is a Docker volume for persistence
mkdir -p /app/db
DB_EXPECTED="/app/backend/server/db.sqlite3"
DB_ACTUAL="/app/db/db.sqlite3"

if [ -f "$DB_EXPECTED" ] && [ ! -L "$DB_EXPECTED" ]; then
    mv "$DB_EXPECTED" "$DB_ACTUAL"
fi
ln -sf "$DB_ACTUAL" "$DB_EXPECTED"

# --- Create empty plugins package (excluded by .dockerignore) ---
if [ ! -d "/app/plugins" ]; then
    mkdir -p /app/plugins
    touch /app/plugins/__init__.py
    echo "Created empty plugins package for OSS mode"
fi

# --- Run migrations ---
echo "Running migrations..."
.venv/bin/python manage.py migrate --no-input

# --- Create admin superuser ---
echo "Ensuring admin user exists..."
.venv/bin/python manage.py shell -c "
from django.contrib.auth import get_user_model
import os
User = get_user_model()
email = os.environ.get('SYSTEM_ADMIN_EMAIL', 'admin@abc.com')
password = os.environ.get('SYSTEM_ADMIN_PASSWORD', 'admin')
if not User.objects.filter(username=email).exists():
    User.objects.create_superuser(username=email, email=email, password=password)
    print(f'Created admin user: {email}')
else:
    print(f'Admin user already exists: {email}')
"

# --- Collect static files ---
.venv/bin/python manage.py collectstatic --no-input 2>/dev/null || true

# --- Start gunicorn ---
echo "Starting gunicorn on port 8000..."
exec .venv/bin/gunicorn \
    --bind 0.0.0.0:8000 \
    --workers 3 \
    --threads 32 \
    --worker-class gthread \
    --log-level debug \
    --keep-alive 620 \
    --timeout 900 \
    --graceful-timeout 960 \
    --access-logfile - \
    --access-logformat '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s %(D)s"' \
    --reuse-port \
    --backlog 1024 \
    --preload \
    backend.server.wsgi:application
