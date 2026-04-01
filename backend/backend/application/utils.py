import json
import logging
import threading
import uuid
from collections import OrderedDict
from typing import Any

import requests

from backend.core.redis_client import RedisClient
from backend.utils.constants import LLMServerConstants, TransformationConstants
from visitran.adapters.connection import BaseConnection
from visitran.errors import ConnectionFailedError, VisitranBaseExceptions
from visitran.utils import get_adapter_connection_cls


def is_already_visited(child_node, parent_node, is_visited) -> bool:
    if parent_node + "_to_" + child_node in is_visited:
        return True

    is_visited[parent_node + "_to_" + child_node] = True
    return False


def set_transformation_sequence(parser):
    sequence_orders = OrderedDict(TransformationConstants.sequence_orders.copy())
    counter = 1

    node_list = []
    edge_list = []
    node = create_lineage_node(parser.source_table_name)
    node["type"] = "input"
    node_list.append(node)

    for transform_parser in parser.transform_parser.get_transforms():
        transform_type = transform_parser.transform_type
        sequence_key = TransformationConstants.key_mapper.get(transform_type) or transform_type
        sequence_orders[sequence_key] = counter
        counter += 1
        node_list.append(create_lineage_node(transform_parser.transform_type))
        edge_list.append(create_lineage_edge(node_list[-2], node_list[-1]))

    if parser.presentation_parser.sort:
        sequence_orders["sort"] = counter
        counter += 1
        node_list.append(create_lineage_node("sort"))
        edge_list.append(create_lineage_edge(node_list[-2], node_list[-1]))

    if parser.presentation_parser.hidden_columns:
        sequence_orders["hidden_columns"] = counter
        counter += 1
        node_list.append(create_lineage_node("hidden_columns"))
        edge_list.append(create_lineage_edge(node_list[-2], node_list[-1]))

    node = create_lineage_node(parser.destination_table_name)
    node["type"] = "output"
    node_list.append(node)
    edge_list.append(create_lineage_edge(node_list[-2], node_list[-1]))

    return sequence_orders, {"data": {"nodes": node_list, "edges": edge_list}}


def create_lineage_node(label_name):
    node_data = {"label": label_name}
    node_id = uuid.uuid1()
    node = {"id": node_id, "data": node_data}
    return node


def create_lineage_edge(prev_node, current_node):
    edge_id = uuid.uuid1()
    return {
        "id": edge_id,
        "source": prev_node["id"],
        "target": current_node["id"],
    }


def get_filter() -> dict[str, Any]:
    _filter = {
        "is_deleted": False,
        "is_archived": False,
    }
    return _filter


def get_connection_data(datasource: str, connection_data: dict[str, Any]) -> dict[str, Any]:
    """Returns the normalized (unredacted) connection data for the specified
    datasource.

    This is used before persisting to the database — sensitive fields must NOT
    be masked here because the model's save() encrypts them with Fernet.
    Masking is handled separately by ``masked_connection_details`` on the model
    when returning data in API responses.

    :param datasource: The type of database being connected to.
    :param connection_data: A dictionary containing connection details.
    :return: A dictionary containing the raw connection data.
    """
    connection_cls: type[BaseConnection] = get_adapter_connection_cls(datasource)
    connection_cls.connection_details = connection_data
    con = connection_cls(**connection_data)
    return con.get_raw_connection_details()


def test_connection_data(datasource: str, connection_data: dict[str, Any]):
    """Raises an exception if the connection fails, mentioning incorrect
    credentials if applicable. Returns None if the connection is successful.

    :param datasource: The type of database being connected to.
    :param connection_data: A dictionary containing connection details.
    """
    connection_cls: type[BaseConnection] = get_adapter_connection_cls(datasource)

    try:
        connection_cls.connection_details = connection_data
        con = connection_cls(**connection_data)
        con.validate()
        con.list_all_schemas()
    except VisitranBaseExceptions as visitran_error:
        raise visitran_error
    except Exception as err:
        raise ConnectionFailedError(db_type=datasource, error_message=str(err))


def create_schema_if_not_exist(datasource: str, connection_data: dict[str, Any]):
    connection_cls: type[BaseConnection] = get_adapter_connection_cls(datasource)
    if schema_name := str(connection_data.get("schema", "")):
        try:
            connection_cls.connection_details = connection_data
            con = connection_cls(**connection_data)
            con.create_schema(schema_name=schema_name)
        except Exception as err:
            raise ConnectionFailedError(db_type=datasource, error_message=str(err)) from err


def build_parent_child_relationships(model_graph, node_parents):
    for edge in model_graph["edges"]:
        source = edge["source"]
        target = edge["target"]
        node_parents[target].append(source)


def assign_ids_to_nodes(model_graph, node_parents, node_ids):
    current_id = 1
    for node in model_graph["nodes"]:
        node_id = node["id"]
        parents = node_parents[node_id]

        if not parents:  # Input node (no parents)
            node_ids[node_id] = current_id
            current_id += 1
        else:
            assign_id_to_node_with_parents(node_id, parents, current_id, node_ids)


def assign_id_to_node_with_parents(node_id, parents, current_id, node_ids):
    if len(parents) == 1:
        if parents[0] in node_ids:
            node_ids[node_id] = node_ids[parents[0]]
        else:
            node_ids[node_id] = current_id
            current_id += 1
    else:  # Multiple parents
        parent_node_ids = [
            node_id
            for parent in parents
            for node_id in (node_ids[parent] if isinstance(node_ids[parent], list) else [node_ids[parent]])
        ]
        node_ids[node_id] = list(set(parent_node_ids))


def assign_id_to_model_graph(model_graph: dict[str, Any]) -> dict[str, Any]:
    node_ids: dict[str, Any] = {}
    node_parents = {node["id"]: [] for node in model_graph["nodes"]}

    build_parent_child_relationships(model_graph, node_parents)

    def build_result():
        result = {}
        for node in model_graph["nodes"]:
            result[node["data"]["label"]] = node_ids[node["id"]]
        return result

    build_parent_child_relationships(model_graph, node_parents)
    assign_ids_to_nodes(model_graph, node_parents, node_ids)
    return build_result()


def get_class_name(file_name: str) -> str:
    """This method converts file name to python class name."""
    return file_name.replace("_", " ").replace("-", " ").title().replace(" ", "")


def do_request(url: str, method: str, data: dict[str, Any] = None) -> dict[str, Any]:
    try:
        logging.info(f"Sending request to {url} with method {method} and the payload")
        response = requests.request(method=method, url=url, json=data, timeout=50)
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"Request failed with status code {response.status_code}")
            raise Exception(f"Request failed with status code {response.status_code}")
    except Exception as e:
        logging.critical(f"Error occurred while making request to flask ai server: {e}")
        raise e


def get_prompt_response_from_llm(payload: dict[str, Any], run_background: bool = False) -> dict[str, Any] | None:
    try:
        kwargs = {"url": LLMServerConstants.SEND_PROMPT_URL, "method": "POST", "data": payload}
        if run_background:
            thread = threading.Thread(name="send_request_thread", target=do_request, kwargs=kwargs, daemon=True)
            thread.start()
            return None
        response = do_request(**kwargs)
        return response
    except Exception as e:
        logging.critical(f"Error occurred while making request to LLM server: {e}")
        raise e


def send_event_to_llm_server(payload: dict[str, Any]) -> dict[str, Any] | None:
    # Check if OSS WebSocket mode is active (API key configured)
    from backend.application.ws_client import check_oss_api_key_configured, is_ws_mode

    if is_ws_mode():
        from backend.application.ws_client import send_event_via_websocket

        logging.info("Using WebSocket transport to AI server (OSS mode)")
        return send_event_via_websocket(payload)

    # Scenario 1: OSS mode but no API key configured — fail early with helpful message
    check_oss_api_key_configured()

    try:
        redis_client = RedisClient()
        if redis_client := redis_client.redis_client:
            serialized_payload = json.dumps(payload)
            redis_client.xadd(LLMServerConstants.LLM_EVENT_STREAMER_NAME, {"data": serialized_payload})
            logging.info(f"Sent event to LLM server in stream id: {LLMServerConstants.LLM_EVENT_STREAMER_NAME}")
        else:
            raise ValueError("Failed to streaming event to LLM server")
    except Exception as e:
        logging.critical(f"Error occurred while making request to LLM server: {e}")
        raise e


# Handle NOTIN(x, a, b, c) → AND(x <> a, x <> b, x <> c)
def replace_notin(match):
    parts = [p.strip() for p in match.group(1).split(",")]
    col, *values = parts
    conditions = [f"{col} <> {v}" for v in values]
    return f"AND({', '.join(conditions)})"


# Handle IN(x, a, b, c) → OR(x = a, x = b, x = c)
def replace_in(match):
    parts = [p.strip() for p in match.group(1).split(",")]
    col, *values = parts
    conditions = [f"{col} = {v}" for v in values]
    return f"OR({', '.join(conditions)})"
