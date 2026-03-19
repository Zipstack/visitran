import os

from dotenv import load_dotenv

load_dotenv()
os.environ.setdefault(
    "DJANGO_SETTINGS_MODULE",
    os.environ.get("DJANGO_SETTINGS_MODULE", "backend.server.settings.dev"),
)


def load_conf():
    print("Loading Django settings")
