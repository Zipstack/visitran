import logging
import sys


class LogConfigurationMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response
        # self.setup_logging()

    def setup_logging(self):
        """Sets up the logging configuration based on the environment."""
        # Production environment: Stream logs to Google Cloud Logging
        try:
            from backend.core.log_handler import RequestLogHandler

            logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

            # Get the root logger
            root_logger = logging.getLogger()
            root_logger.setLevel(logging.INFO)

            # Create a CloudLoggingHandler
            handler = RequestLogHandler()

            root_logger.addHandler(handler)
            print("Logging configured for Google Cloud Logging.")

        except Exception as e:
            print("Warning: google-cloud-logging library not found. Falling back to local file logging.")
            print(f"{e.__str__()}")
            self._setup_local_logging()

    @staticmethod
    def _setup_local_logging():
        """Sets a logger for terminal output with color."""
        from backend.core.log_handler import LogsCustomFormatter

        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(LogsCustomFormatter())

        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        logger.addHandler(handler)
        print("Logging configured for terminal output.")
        return logger

    def __call__(self, request):
        return self.get_response(request)
