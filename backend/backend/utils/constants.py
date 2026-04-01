from collections import OrderedDict
from os import path

from django.conf import settings

from backend.utils.common_utils import CommonUtils
from visitran.constants import BaseConstant


class RouterConstants(BaseConstant):
    PROJECTS = "projects"


class FileConstants:
    # These constants are used for persisting the python and seed files
    PROJECT_PATH = "projects"
    MODELS = "models"
    SEEDS = "seeds"
    PY = ".py"
    SLASH = str(path.sep)
    DOT = "."


class TransformationConstants:
    sequence_orders = OrderedDict(
        [
            ("joins", 0),
            ("unions", 0),
            ("pivot", 0),
            ("combine_columns", 0),
            ("synthesize", 0),
            ("filters", 0),
            ("groups", 0),
            ("aggregate", 0),
            ("havings", 0),
            ("aggregate_filter", 0),
            ("find_and_replace", 0),
            ("distinct", 0),
            ("rename", 0),
            ("sort", 0),
            ("hidden_columns", 0),
        ]
    )

    key_mapper = {
        "filter": "filters",
        "rename_column": "rename",
        "union": "unions",
        "join": "joins",
    }


class HTTPMethods:
    HEAD = "HEAD"
    GET = "GET"
    PUT = "PUT"
    POST = "POST"
    DELETE = "DELETE"


class ExecutionLogConstants:
    """Constants for ExecutionLog.

    Attributes:
        IS_ENABLED (bool): Whether to enable log history.
        CONSUMER_INTERVAL (int): The interval (in seconds) between log history
            consumers.
        LOG_QUEUE_NAME (str): The name of the queue to store log history.
        LOGS_BATCH_LIMIT (str): The maximum number of logs to store in a batch.
        CELERY_QUEUE_NAME (str): The name of the Celery queue to schedule log
            history consumers.
        PERIODIC_TASK_NAME (str): The name of the Celery periodic task to schedule
            log history consumers.
        TASK (str): The name of the Celery task to schedule log history consumers.
    """

    IS_ENABLED: bool = CommonUtils.str_to_bool(settings.ENABLE_LOG_HISTORY)
    CONSUMER_INTERVAL: int = settings.LOG_HISTORY_CONSUMER_INTERVAL
    LOGS_BATCH_LIMIT: int = settings.LOGS_BATCH_LIMIT
    LOG_QUEUE_NAME: str = "log_history_queue"
    CELERY_QUEUE_NAME = "celery_periodic_logs"
    PERIODIC_TASK_NAME = "workflow_log_history"
    TASK = "execution_log_utils.consume_log_history"


class JaffleShopProjectConstants:
    name = "Jaffle Shop"
    description = (
        "Jaffle Shop is a sample project created by the visitran team. \n"
        "It provides insights that help businesses analyse customer behavior, "
        "revenue and order trends, turning raw data into valuable information for "
        "reporting and decision-making. \n"
        "This is to help you understand and explore the product's capabilities."
    )

    connection_name = "Jaffle Shop Postgres DB"
    connection_desc = "This connection is created for a trial purpose to understand the product and cannot be reused."
    datasource = "postgres"

    schema = "raw"

    csv_files = ["raw_customers.csv", "raw_orders.csv", "raw_payments.csv"]
    model_list = [
        "dev_customers",
        "dev_orders",
        "dev_payments",
        "stg_aggr_order_payments",
        "stg_order_summaries",
        "stg_payments_by_type",
        "prod_order_details",
        "prod_customer_ltv",
    ]


class DvdRentalProjectConstants:
    name = "DVD Rental"

    description = (
        "DVD Rental is a sample project created by the visitran team. \n"
        "It provides a prebuilt data model and sample data to help you understand and explore the product's capabilities."
    )

    connection_name = "Dvd Rental Postgres DB"
    connection_desc = "This connection is created for a trial purpose to understand the product and cannot be reused."
    datasource = "postgres"

    schema = "raw"

    csv_files = [
        "actor.csv",
        "address.csv",
        "category.csv",
        "city.csv",
        "country.csv",
        "customer.csv",
        "film.csv",
        "film_actor.csv",
        "film_category.csv",
        "inventory.csv",
        "language.csv",
        "payment.csv",
        "rental.csv",
        "staff.csv",
        "store.csv",
    ]

    model_list = [
        "staff_contact_info",
        "store_inventory_counts",
        "store_active_customer",
        "store_active_customer_counts",
        "customer_email_count",
        "store_unique_film_count",
        "total_unique_film_categories",
        "film_replacement_cost_summary",
        "payment_amount_summary",
        "customer_rental_activity",
        "store_manager_locations",
        "store_inventory_details",
        "store_inventory_rating_summary",
        "store_inventory_category_cost_summary",
        "customer_details_with_address",
        "customer_lifetime_value",
    ]


class LLMServerConstants:
    SEND_PROMPT_URL = f"{settings.AI_SERVER_BASE_URL}/api/v1/prompt-message"
    LLM_EVENT_STREAMER_NAME = settings.LLM_EVENT_STREAMER_NAME
