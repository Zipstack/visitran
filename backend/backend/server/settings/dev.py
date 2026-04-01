from backend.server.settings.base import *

DEBUG = True

USE_REMOTE_FS = False

ALLOWED_HOSTS = ["*"]

RAISE_ERROR = bool(os.environ.get("RAISE_ERROR", False))

# OSS: Use shared apps directly (no cloud-specific apps needed)
INSTALLED_APPS = SHARED_APPS

OSS_AUTH_MIDDLEWARE = "backend.core.middlewares.oss_auth_middleware.OSSAuthMiddleware"
OSS_CSRF_MIDDLEWARE = "backend.core.middlewares.oss_csrf_middleware.OSSCsrfMiddleware"
LOGGING_MIDDLEWARE = "backend.core.middlewares.log_aggregator.LogAggregatorMiddleware"
LOGGING_CONFIGURATION_MIDDLEWARE = "backend.core.middlewares.log_configuration.LogConfigurationMiddleware"

SILENCED_SYSTEM_CHECKS = ["urls.W002"]

MIDDLEWARE = [
    "corsheaders.middleware.CorsMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    OSS_CSRF_MIDDLEWARE,  # Custom CSRF that exempts auth endpoints
    "whitenoise.middleware.WhiteNoiseMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    OSS_AUTH_MIDDLEWARE,  # After auth so request.user is available
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
    LOGGING_CONFIGURATION_MIDDLEWARE,
    LOGGING_MIDDLEWARE,
]

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [os.path.join(BASE_DIR, "build")],  # noqa: F405
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

# Database
# https://docs.djangoproject.com/en/4.1/ref/settings/#databases
# Use PostgreSQL if DB_HOST is set (Docker), otherwise SQLite (local dev)
if os.environ.get("DB_HOST"):
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": os.environ.get("DB_NAME", "postgres"),
            "USER": os.environ.get("DB_USER", "postgres"),
            "PASSWORD": os.environ.get("DB_PASSWORD", "postgres"),
            "HOST": os.environ.get("DB_HOST"),
            "PORT": os.environ.get("DB_PORT", "5432"),
        },
    }
else:
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": os.environ.get("SQLITE_DB_PATH", BASE_DIR / "db.sqlite3"),
        },
    }

# Optional sample database connection (for demo projects)
if os.environ.get("DB_SAMPLE_HOST"):
    DATABASES["postgres_db"] = {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.environ.get("DB_SAMPLE_DBNAME"),
        "USER": os.environ.get("DB_SAMPLE_USER"),
        "PASSWORD": os.environ.get("DB_SAMPLE_PASSWORD"),
        "HOST": os.environ.get("DB_SAMPLE_HOST"),
        "PORT": os.environ.get("DB_SAMPLE_PORT"),
        "OPTIONS": {
            "options": f"-c search_path={os.environ.get('DB_SAMPLE_SCHEMA')}",
        },
    }

# Cache
# Redis is required for Celery, WebSocket, and AI streaming — always use it for cache too.
# LocMemCache was causing stale data with gunicorn's multiple workers (per-process isolation).
_redis_db = REDIS_DB if REDIS_DB not in (None, "") else "1"  # default to db1 to avoid Celery on db0
if REDIS_PASSWORD:
    _redis_url = f"redis://{REDIS_USER}:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/{_redis_db}"
else:
    _redis_url = f"redis://{REDIS_HOST}:{REDIS_PORT}/{_redis_db}"
CACHES = {
    "default": {
        "BACKEND": "django.core.cache.backends.redis.RedisCache",
        "LOCATION": _redis_url,
    }
}


X_FRAME_OPTIONS = "DENY"

CORS_ALLOWED_ORIGINS = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://frontend.visitran.localhost",
    # Other allowed origins if needed
]

CORS_ORIGIN_WHITELIST = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://frontend.visitran.localhost",
]

CORS_ALLOW_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]

CORS_ALLOW_HEADERS = [
    "authorization",
    "content-type",
    "x-csrftoken",
    "x-requested-with",
]

CSRF_TRUSTED_ORIGINS = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://frontend.visitran.localhost",
]

# API Authentication Whitelists (paths that don't require authentication)
WHITELISTED_PATHS_LIST = [
    "/login",
    "/home",
    "/callback",
    "/favicon.ico",
    "/logout",
    "/signup",
    "/static",
    "/health",
    "/forgot-password",
    "/reset-password",
    "/validate-reset-token",
]

WHITELISTED_PATHS = [f"/{PATH_PREFIX}{PATH}" for PATH in WHITELISTED_PATHS_LIST]

# OSS Auth: Override secure cookie for local dev (no HTTPS)
SESSION_COOKIE_SECURE = False
CSRF_COOKIE_SECURE = False
CSRF_COOKIE_SAMESITE = "Lax"
SESSION_COOKIE_SAMESITE = "Lax"
# Allow CSRF cookie to be read by JavaScript for AJAX requests
CSRF_COOKIE_HTTPONLY = False

# Django REST Framework settings for OSS
# Use custom CSRF-exempt session authentication for dev mode
REST_FRAMEWORK = {
    "DEFAULT_AUTHENTICATION_CLASSES": [
        "backend.core.authentication.CsrfExemptSessionAuthentication",
    ],
    "DEFAULT_PERMISSION_CLASSES": [
        "rest_framework.permissions.IsAuthenticated",
    ],
    # Disable browsable API - return JSON only
    "DEFAULT_RENDERER_CLASSES": [
        "rest_framework.renderers.JSONRenderer",
    ],
}
