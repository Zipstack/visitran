try:
    from importlib.metadata import version
    __version__ = version(__name__)
except Exception:
    __version__ = "0.0.0-dev"
