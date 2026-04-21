import logging
import uuid
from typing import Any

from backend.application.context.application import ApplicationContext
from backend.core.models.config_models import ConfigModels
from backend.errors import InvalidModelConfigError


class NoCodeModel(ApplicationContext):
    def __init__(self, project_id: str, environment_id: str = "") -> None:
        super().__init__(project_id, environment_id)

    def _reset_model_run_status(self, model_name: str) -> None:
        """Reset run status when the model spec changes so the explorer
        doesn't keep showing a stale failure indicator."""
        try:
            model_instance = ConfigModels.objects.get(
                project_instance__project_uuid=self.session.project_id,
                model_name=model_name,
            )
            if model_instance.run_status == ConfigModels.RunStatus.FAILED:
                model_instance.run_status = ConfigModels.RunStatus.NOT_STARTED
                model_instance.failure_reason = None
                model_instance.save(update_fields=["run_status", "failure_reason"])
        except ConfigModels.DoesNotExist:
            pass

    def _validate_and_update_model(
            self,
            model_data: dict[str, Any],
            model_name: str,
            config_type: str,
            transformation_type: str = None,
            transformation_id: str = None,
    ) -> dict[str, Any]:
        """
        Validating the model data before persisting and updating
        Possible transformation type
        - model_config
        - join
        - merge
        - pivot
        -
        Action_type:
            - create
            - delete
            - clear
        Args:
            model_data (dict[str, Any]): The dict of the metadata of the model
            model_name (str): Name of the model
            transformation_type (str, optional): Type of the transformation
            transformation_id (str, optional):
            config_type: Explains about Create or update and delete operations
        Returns:
            None: This method performs validation and updates the new_model_data dict
                in-place. Specifically, it may modify the "reference" key in new_model_data
                to resolve any Method Resolution Order (MRO) issues in the dependency graph.
        """
        self.validate_model(
            new_model_data=model_data,
            model_name=model_name,
            transformation_type=transformation_type,
            transformation_id=transformation_id,
            config_type=config_type
        )
        # Converting the current model to python.
        result = self.update_model(model_name=model_name, model_data=model_data)
        # Only reset stale run status for changes that affect model execution
        if config_type not in ("presentation",):
            self._reset_model_run_status(model_name)
        return result

    def set_model_config_and_reference(self, no_code_data: dict[str, Any], model_name: str):
        """
        Update or initialize the model configuration in the session for a given model name.
        """
        try:
            model_config = no_code_data.get("model_config")
            reference_config = no_code_data.get("reference_config")

            if not isinstance(model_config, dict):
                raise InvalidModelConfigError(failure_reason="'model_config' must be a dictionary.")
            if not isinstance(reference_config, list):
                raise InvalidModelConfigError(failure_reason="'reference_config' must be a list.")

            model_dict = model_config.get("model")
            source_dict = model_config.get("source")

            if not isinstance(model_dict, dict) or not isinstance(source_dict, dict):
                raise InvalidModelConfigError(
                    failure_reason="'model' and 'source' must be dictionaries in 'model_config'.")

            # Fetch existing session model data
            existing_data = self.session.fetch_model_data(model_name=model_name) or {}
            # If there's a change in source or no existing model data
            config_type = "source"
            if existing_data.get("source") != source_dict:
                config_type = "source"
                model_data = {
                    "model": model_dict,
                    "source": source_dict,
                    "reference": reference_config,
                    "transform": {},
                    "transform_order": [],
                    "presentation": {"sort": [], "hidden_columns": ["*"]},
                }
            elif existing_data.get("model") != model_dict:
                config_type = "model"
                # Merge model changes only
                model_data = existing_data
                model_data["model"] = model_dict
            elif existing_data.get("reference") != reference_config:
                config_type = "reference"
                # Merge reference config only
                model_data = existing_data
                model_data["reference"] = reference_config
            else:
                raise InvalidModelConfigError(
                    failure_reason="No changes detected in the given spec YAML"
                )

            return self._validate_and_update_model(
                model_data=model_data,
                model_name=model_name,
                config_type=config_type,
                transformation_type="model",
            )

        except (KeyError, TypeError) as e:
            logging.error(f"Error while setting model config: {e}")
            raise InvalidModelConfigError(failure_reason=f"Invalid 'no_code_data' structure: {e}")


    def set_model_transformation(self, no_code_data: dict[str, Any], model_name: str):
        """
        Adds or updates the transformation in the model, based on the given transformation config.
        """
        transformation_config = no_code_data["step_config"]
        transformation_type = transformation_config["type"]
        model_data = self.session.fetch_model_data(model_name=model_name)
        config_type = "create_transformation"
        # Check for existing transformation_type in transform_order
        for transform_id in model_data["transform_order"]:
            if transform_id.startswith(transformation_type):
                if "step_id" not in no_code_data:
                    raise ValueError(
                        f"Transformation of type '{transformation_type}' already exists. "
                        f"Each transformation type must be unique unless a step_id is provided for an update."
                    )

        if "step_id" in no_code_data:
            config_type = "update_transformation"
            transformation_id = no_code_data["step_id"]
            if transformation_id not in model_data["transform_order"]:
                raise ValueError(
                    f"Transformation with step_id '{transformation_id}' does not exist. "
                    f"Please provide a valid step_id for an update."
                )
        else:
            # Generate a unique transformation id if not in the model
            transformation_id = f"{transformation_type}_{uuid.uuid4()}"
            model_data["transform_order"].append(transformation_id)

        model_data["transform"][transformation_id] = transformation_config
        update_model_data: dict[str, Any] = self._validate_and_update_model(
            model_data,
            model_name,
            config_type=config_type,
            transformation_type=transformation_type,
            transformation_id=transformation_id
        )
        update_model_data["step_id"] = transformation_id
        return update_model_data

    def delete_model_transformation(self, model_name: str, transformation_id: str, is_clear_all: bool = False):
        """
        This method deletes a transformation from the model.
        :param is_clear_all:
        :param model_name: The name of the model.
        :param transformation_id: The ID of the transformation to be deleted.
        :return: The validation result after updating the model.
        """
        # Fetch the current model data from the session
        model_data = self.session.fetch_model_data(model_name=model_name)
        transformation_type = ""

        if is_clear_all:
            model_data["transform"] = {}
            model_data["transform_order"] = []
            model_data["presentation"] = {"sort": [], "hidden_columns": ["*"]}
            return self._validate_and_update_model(
                model_data,
                model_name,
                config_type="clear_all",
                transformation_type="clear_all",
            )

        # Remove the transformation from the transform dictionary
        if transformation_id in model_data["transform"]:
            transformation_type = model_data["transform"][transformation_id]["type"]
            del model_data["transform"][transformation_id]

        # Remove the transformation_id from a transform_order list if it exists
        if transformation_id in model_data["transform_order"]:
            model_data["transform_order"].remove(transformation_id)

        # Validate and persist the changes
        return self._validate_and_update_model(
            model_data=model_data,
            model_name=model_name,
            config_type="delete_transformation",
            transformation_type=transformation_type,
            transformation_id=transformation_id,
        )

    def set_model_presentation(self, no_code_data: dict[str, Any], model_name: str):
        """
        Updates the 'presentation' configuration of the model. Only updates keys present in 'no_code_data'
        without affecting other keys.
        """
        model_data = self.session.fetch_model_data(model_name=model_name)
        presentation_config = model_data.get("presentation", {})

        # Update only the keys provided in `no_code_data`
        if "sort" in no_code_data:
            presentation_config["sort"] = no_code_data["sort"]
        if "hidden_columns" in no_code_data:
            presentation_config["hidden_columns"] = no_code_data["hidden_columns"]
        if "column_order" in no_code_data:
            presentation_config["column_order"] = no_code_data["column_order"]

        model_data["presentation"] = presentation_config
        return self._validate_and_update_model(
            model_data,
            model_name,
            config_type="presentation",
            transformation_type=""
        )

    def get_transformation_columns(
            self,
            model_name: str,
            transformation_id: str,
            transformation_type: str
    ) -> dict[str, Any]:
        """
        This method will return the list of available columns, and it’s metadata in the response.  If the
        transformation
        ID is sent, then the list of columns which are available at that particular step will be sent in the response.
        """

        # If the transformation id is present, this will return the available columns for the specified columns.
        return self.get_model_table_details(
            model_name=model_name,
            transformation_id=transformation_id,
            transformation_type=transformation_type
        )
