#!/usr/bin/env bash

cmd=$1
if [ "$cmd" = "migrate" ]; then
    echo "Migration initiated"
    .venv/bin/python manage.py migrate
fi

# NOTE: Leaving below for reference incase required in the future
# python manage.py runserver 0.0.0.0:8000 --insecure
# NOTE updated socket threads
.venv/bin/gunicorn \
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
    backend.server.wsgi:application
