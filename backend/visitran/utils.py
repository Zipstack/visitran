from __future__ import annotations

import base64
import importlib
import inspect
import io
import os
from codecs import BOM_UTF8
from collections.abc import ValuesView
from glob import glob
from pathlib import Path
from typing import TYPE_CHECKING, Any, Union
from urllib.parse import urlparse

import matplotlib
import matplotlib.pyplot as plt
import networkx as nx
import yaml
from django.conf import settings
from django.core.cache import cache
from google.cloud import storage

from visitran.constants import CloudConstants
from visitran.errors import ModelIncludedIsExcluded

if TYPE_CHECKING:  # pragma: no cover
    # we need below import only when mypy type checks project
    # if we do a normal import it will result in circular import
    from adapters.adapter import BaseAdapter
    from adapters.connection import BaseConnection

    from visitran.visitran import VisitranModel


BOM = BOM_UTF8.decode("utf-8")


def get_object_for_class(model_objs: ValuesView[VisitranModel], cls: type) -> Union[VisitranModel, None]:
    """Returns a class from list of classes which if it inherits from given
    class."""
    for model in model_objs:
        if model.__class__ is cls:
            return model

    return None


def get_adapter_cls(adapter_name: str) -> type[BaseAdapter]:
    module_file = importlib.import_module(f"visitran.adapters.{adapter_name}.adapter")
    adapter_members = inspect.getmembers(module_file)
    adapter_cls: type[BaseAdapter] = [
        clobj for (cn, clobj) in adapter_members if adapter_name in cn.lower() and "adapter" in cn.lower()
    ].pop()
    return adapter_cls


def get_adapter_connection_cls(adapter_name: str) -> type[BaseConnection]:
    module_file = importlib.import_module(f"visitran.adapters.{adapter_name}.connection")
    adapter_members = inspect.getmembers(module_file)
    adapter_cls: type[BaseConnection] = [
        clobj for (cn, clobj) in adapter_members if adapter_name in cn.lower() and "connection" in cn.lower()
    ].pop()
    return adapter_cls


def download_gcs_file_to_memory(gcs_url):
    # Parse the GCS URL
    parsed_url = urlparse(gcs_url)
    bucket_name = CloudConstants.BUCKET_NAME

    # Initialize a storage client and get the bucket and blob
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(parsed_url.path.split(bucket_name)[-1].lstrip("/"))

    # Download the blob content into a bytes object
    file_obj = io.BytesIO()
    blob.download_to_file(file_obj)
    file_obj.seek(0)  # Reset the file pointer to the beginning

    # Decode bytes to string and wrap in StringIO
    csv_content = file_obj.read().decode("utf-8")
    string_obj = io.StringIO(csv_content)
    return string_obj


def generate_tmp_name(prefix: str) -> str:
    """Generates unique string with 8 chars."""
    return prefix + base64.b64encode(os.urandom(6)).decode("ascii")


def _add_matching_nodes_to_includes(graph: nx.DiGraph, includes: list[str]) -> list[str]:
    new_includes = []
    for node in graph.nodes():
        node_str = str(node)
        for include in includes:
            if include in node_str:
                new_includes.append(node_str)
                break  # No need to check other includes for this node
    if len(new_includes) == 0:
        new_includes = includes
    return new_includes


def select_dag_based_on_include_exclude(
    graph: nx.DiGraph, include_list: list[str], exclude_list: list[str]
) -> nx.DiGraph:
    """Selects a directed acyclic graph (DAG) based on a list of included and
    excluded nodes."""
    # Create a new graph to store the filtered nodes and ancestors
    include_graph = nx.DiGraph()

    includes = _add_matching_nodes_to_includes(graph, include_list)
    excludes = _add_matching_nodes_to_includes(graph, exclude_list)

    if includes == []:
        include_graph = graph.copy()

    # Add all nodes in the include list and their ancestors to the filtered graph
    for node in includes:
        ancestors = nx.ancestors(graph, node)
        include_graph.add_nodes_from([node] + list(ancestors))

    for node in excludes:
        descendants = nx.descendants(graph, node)
        include_graph.remove_nodes_from([node] + list(descendants))

    for node in includes:
        if not include_graph.has_node(node):
            raise ModelIncludedIsExcluded(includes=str(includes), excludes=str(excludes))

    output_graph = nx.DiGraph(graph.subgraph(include_graph.nodes))

    return output_graph


def draw_dag(dag: nx.DiGraph) -> None:
    """Draws a DAG using matplotlib."""
    nx.draw(dag, with_labels=True)
    matplotlib.use("TkAgg")
    plt.show()


def get_test_methods_from_test_obj(test_obj: VisitranModel) -> list[str]:
    return [method for method in dir(test_obj) if method.lower().startswith("test")]


def get_method_from_object(obj: Any, method: Any) -> Any:
    return getattr(obj, method)


def import_file(name: str) -> Any:
    return importlib.import_module(name=name)


def get_adapters_list() -> list[str]:
    """Returns the list of adapters which are available."""
    adapters_dir_path = Path(__file__).parent / "adapters"
    adapters_path = f"{adapters_dir_path}/*[!.py][!__]"
    db_list = list(map(os.path.basename, glob(pathname=adapters_path)))
    if settings.IS_CLOUD:
        db_list.remove("duckdb")
    return sorted(db_list)


def get_adapter_connection_fields(adapter_name: str) -> dict[str, Any]:
    fields = cache.get(adapter_name)
    if not fields:
        connection_cls = get_adapter_connection_cls(adapter_name)
        fields = connection_cls.connection_fields()
        cache.set(adapter_name, fields)
    return fields
