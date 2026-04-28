import logging
import uuid
from typing import Any

from backend.application.context.base_context import BaseContext


class ModelGraph(BaseContext):
    def update_model_graph(self, no_code_data: dict[str, Any], model_name: str):
        old_model_data = self.session.fetch_model_data(model_name)
        old_model_reference = old_model_data.get("reference", [])
        new_model_reference = no_code_data.get("reference", [])

        added_references = list(set(new_model_reference) - set(old_model_reference))
        deleted_references = list(set(old_model_reference) - set(new_model_reference))

        for reference in deleted_references:
            self.session.model_graph.remove_edge(reference, model_name)

        for reference in added_references:
            self.session.model_graph.add_edge(reference, model_name)

        self.session.project_instance.project_model_graph = self.session.model_graph.serialize()
        self.session.project_instance.save()

    def get_lineage_relation(self):
        edge_list = []
        model_to_path_dict = self.build_model_path_dict()
        is_visited = {}
        no_code_models = self.session.fetch_all_models(fetch_all=True)
        node_list = []
        id_to_node = {}
        self.get_no_code_nodes(id_to_node, no_code_models, node_list)
        logging.info(f"id to nodes: {id_to_node}")
        for model in no_code_models:
            model_name = model.model_name
            self.find_child_model(no_code_models, model_name, id_to_node)

            # Create a temporary set to track the current path and prevent cycles
            visited_in_path = set()
            self.find_parent_model(model, edge_list, model_to_path_dict, is_visited, visited_in_path, id_to_node)
        return {"nodes": node_list, "edges": edge_list}

    @staticmethod
    def get_no_code_nodes(id_to_node, no_code_models, node_list):
        for model in no_code_models:
            model_name = model.model_name
            node_data = {"label": model_name}
            node_id = uuid.uuid1()
            node = {"id": node_id, "data": node_data}
            node_list.append(node)
            id_to_node[model_name] = node

    @staticmethod
    def find_child_model(no_code_models, current_model, id_to_node):
        for model in no_code_models:
            _dict = model.model_data or {}
            if len(_dict) != 0 and "reference" in _dict and current_model in _dict["reference"]:
                return

        node = id_to_node[current_model]
        node["type"] = "output"

    def find_parent_model(self, model, edge_list, model_to_path_dict, is_visited, visited_in_path, id_to_node):
        model_name = model.model_name
        _dict = model.model_data or {}
        if model_name in visited_in_path:
            logging.info(f"model name {model_name} is already in {visited_in_path}")
            return

        visited_in_path.add(model_name)
        is_visited[model_name] = True

        if "reference" in _dict:
            for parent_node in _dict["reference"]:
                if not self.is_already_visited(model_name, parent_node, is_visited):
                    edge_id = uuid.uuid1()
                    edge = {
                        "id": edge_id,
                        "source": id_to_node[parent_node]["id"],
                        "target": id_to_node[model_name]["id"],
                    }
                    edge_list.append(edge)
                    if parent_node in model_to_path_dict:
                        self.find_parent_model(
                            model_to_path_dict[parent_node],
                            edge_list,
                            model_to_path_dict,
                            is_visited,
                            visited_in_path,
                            id_to_node,
                        )
        else:
            node = id_to_node[model_name]
            node["type"] = "input"

    def build_model_path_dict(self):
        model_to_path_dict = {}
        no_code_models = self.session.fetch_all_models(fetch_all=True)
        for model in no_code_models:
            model_name = model.model_name
            model_to_path_dict[model_name] = model

        return model_to_path_dict

    @staticmethod
    def is_already_visited(child_node, parent_node, is_visited) -> bool:
        if parent_node + "_to_" + child_node in is_visited:
            return True

        is_visited[parent_node + "_to_" + child_node] = True
        return False
