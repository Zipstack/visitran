import logging

import yaml

from backend.application.context.token_cost_service import TokenCostService
from backend.core.models.chat_message import ChatMessage
from backend.core.redis_client import RedisClient
from backend.core.routers.chat_message.constants import ChatMessageStatus
from backend.core.web_socket import (
    send_socket_message,
)


class ChatAiContext(TokenCostService):
    def __init__(self, project_id: str) -> None:
        """
        Initialize the ChatMessageContext with a specific project_id.

        Args:
            project_id (str): The UUID of the project context.
        """
        super().__init__(project_id)
        self.project_id = project_id
        self.generate_model_list = []
        self.byte_counter = {}
        self.pubsub = self.session.redis_client.pubsub()
        self.redis_client = RedisClient()

    def send_and_persist_thought_chain(
        self,
        sid: str,
        channel_id: str,
        chat_id: str,
        chat_message_id: str,
        content: str,
        prompt_status: str
    ):
        self.persist_thought_chain(
            chat_id=chat_id, chat_message_id=chat_message_id, thought_chain=content)
        send_socket_message(
            sid=sid,
            channel_id=channel_id,
            chat_id=chat_id,
            chat_message_id=chat_message_id,
            thought_chain_content=content,
            prompt_status=prompt_status,
        )

    def send_and_persist_error(
            self,
            sid: str,
            channel_id: str,
            chat_id: str,
            chat_message_id: str,
            error_msg_content: dict,
            prompt_status: str,
            error_state: str,
    ):
        # Update status to FAILED if there's an error
        self.persist_prompt_status(
            chat_message_id=chat_message_id,
            status=prompt_status,
            error_message=error_msg_content,
        )
        send_socket_message(
            sid=sid,
            channel_id=channel_id,
            chat_id=chat_id,
            chat_message_id=chat_message_id,
            error_msg_content=error_msg_content,
            error_state=error_state,
            prompt_status=prompt_status,
        )

    def send_and_persist_response(
            self,
            sid: str,
            channel_id: str,
            chat_id: str,
            chat_message_id: str,
            content: str,
            response: str,
            chat_name: str,
            is_append_response: bool = False,
    ):
        self.persist_response(
            chat_id=chat_id,
            chat_message_id=chat_message_id,
            technical_content=content,
            response=response,
            is_append_response=is_append_response,
            response_time=1,
            chat_name=chat_name,
        )
        send_socket_message(
            sid=sid,
            channel_id=channel_id,
            chat_id=chat_id,
            chat_message_id=chat_message_id,
            summary=content,
        )

    @staticmethod
    def extract_yaml_text(raw_response: str):
        """
        Parses all YAML content into a single flat list of dict objects.

        Supports:
        - Single YAML object
        - List of objects
        - Multiple YAML documents (with ---)
        - Combination of lists and dicts in documents

        Returns:
            List[Dict]: Flattened list of all model objects.
        """

        # Remove triple backticks
        cleaned_response = raw_response.strip("`").strip()
        if cleaned_response.startswith("yaml"):
            cleaned_response = cleaned_response[4:].strip()
        models = list(yaml.safe_load_all(cleaned_response))

        # Flatten the list of documents
        flattened_models = []
        for model in models:
            if isinstance(model, list):
                flattened_models.extend(model)
            elif isinstance(model, dict):
                flattened_models.append(model)
            elif model is not None:
                raise ValueError(
                    f"Parsing Failure: Unexpected yaml response content type {type(model)}")

        return flattened_models

    def _load_visitran_models(self):
        all_models = []
        for model in self.session.fetch_all_models():
            model_data = {
                "reference": model.model_data.get("reference", []),
                "source": model.model_data.get("source", ""),
                "model": model.model_data.get("model", ""),
                "presentation": model.model_data.get("presentation", {}),
                "transform": model.model_data.get("transform", {}),
            }
            # Fetch SQL query for this model
            try:
                sql_data = self.session.get_model_dependency_data(
                    model_name=model.model_name,
                    transformation_id="sql",
                    default=None
                )
                sql_query = sql_data.get("sql") if sql_data else None
            except Exception:
                sql_query = None
            model_entry = {
                "model_name": model.model_name,
                "model": model_data,
                "sql": sql_query
            }
            all_models.append(model_entry)
        visitran_models = yaml.dump(
            all_models, default_flow_style=False, sort_keys=False)
        self.session.redis_client.set(self.redis_model_key, visitran_models)
        return visitran_models

    def get_visitran_models(self) -> str:
        visitran_models = self._load_visitran_models()
        return visitran_models

    def _process_thought_chain(self, *args, **kwargs):
        self.send_and_persist_thought_chain(
            sid=kwargs["sid"],
            channel_id=kwargs["channel_id"],
            chat_id=kwargs["chat_id"],
            chat_message_id=kwargs["chat_message_id"],
            content=kwargs["content"],
            prompt_status=ChatMessageStatus.RUNNING,
        )

    def _process_prompt_response(self, *args, **kwargs):
        sid = kwargs["sid"]
        channel_id = kwargs["channel_id"]
        chat_intent = kwargs["chat_intent"]
        chat_id = kwargs["chat_id"]
        chat_message_id = kwargs["chat_message_id"]
        content = kwargs["content"]
        chat_name = kwargs.get("chat_name")
        discussion_status = kwargs.get("discussion_status")
        if chat_intent == "SQL":
            self.send_and_persist_response(
                sid=sid,
                channel_id=channel_id,
                chat_id=chat_id,
                chat_message_id=chat_message_id,
                content=content,
                response=content,
                is_append_response=True,
                chat_name=chat_name,
            )
        elif chat_intent == "TRANSFORM":
            if chat_message_id not in self.byte_counter:
                self.byte_counter[chat_message_id] = 0

            content_bytes = content.encode("utf-8")
            byte_size = len(content_bytes)
            self.byte_counter[chat_message_id] += byte_size

            logging.info(
                f"Accumulated bytes: {self.byte_counter[chat_message_id]}, Current:, {byte_size}")

            self.persist_response(
                chat_id=chat_id,
                chat_message_id=chat_message_id,
                technical_content=content,
                response_time=1,
                chat_name=chat_name,
                discussion_status=discussion_status,
            )
            self.persist_prompt_status(
                chat_message_id=chat_message_id, status=ChatMessageStatus.RUNNING)
            send_socket_message(
                sid=sid,
                channel_id=channel_id,
                chat_id=chat_id,
                chat_message_id=chat_message_id,
                bytes_data=self.byte_counter[chat_message_id],
                discussion_status=discussion_status,
            )

    def _process_summary(self, *args, **kwargs):
        sid = kwargs["sid"]
        channel_id = kwargs["channel_id"]
        chat_id = kwargs["chat_id"]
        chat_message_id = kwargs["chat_message_id"]
        content = kwargs["content"]
        chat_name = kwargs.get("chat_name")
        discussion_status = kwargs.get("discussion_status")
        self.persist_response(
            chat_id=chat_id,
            chat_message_id=chat_message_id,
            response=content,
            is_append_response=True,
            response_time=1,
            chat_name=chat_name,
            discussion_status=discussion_status,
        )
        send_socket_message(
            sid=sid, channel_id=channel_id, chat_id=chat_id, chat_message_id=chat_message_id, summary=content, discussion_status=discussion_status
        )

    def _process_chat_name(self, *args, **kwargs):
        chat_id = kwargs["chat_id"]
        chat_message_id = kwargs["chat_message_id"]
        chat_name = kwargs.get("chat_name")
        self.persist_response(
            chat_id=chat_id,
            chat_message_id=chat_message_id,
            response_time=1,
            chat_name=chat_name,
        )

    def _process_completed(self, *args, **kwargs):
        sid = kwargs["sid"]
        channel_id = kwargs["channel_id"]
        chat_id = kwargs["chat_id"]
        chat_intent = kwargs["chat_intent"]
        chat_message_id = kwargs["chat_message_id"]

        # Get token cost data if provided from visitran_ai
        content = kwargs.get("content")
        token_usage_data = {}
        processing_time_ms = 0

        # Check if content is a dictionary and contains token_info
        if isinstance(content, dict):
            token_usage_data = content.get("token_info", {})
            processing_time_ms = token_usage_data.get("processing_time_ms", 0)
        elif isinstance(content, str):
            # If content is a string, try to parse it as JSON
            try:
                import json
                parsed_content = json.loads(content)
                if isinstance(parsed_content, dict):
                    token_usage_data = parsed_content.get("token_info", {})
                    processing_time_ms = token_usage_data.get("processing_time_ms", 0)
            except (json.JSONDecodeError, AttributeError):
                # If parsing fails, use empty defaults
                pass

        # Update status to SUCCESS when prompt response is complete
        chat_message = self.persist_prompt_status(
            chat_message_id=chat_message_id, status=ChatMessageStatus.SUCCESS
        )

        # Store token cost information if provided
        if token_usage_data:
            try:
                # Create token cost record
                token_cost = self.create_token_cost_record(
                    chat_message=chat_message,
                    token_data=token_usage_data,
                    chat_intent=chat_intent,
                    session_id=chat_id,
                    processing_time_ms=processing_time_ms
                )

                if token_cost:
                    logging.info(f"Token cost persisted: ${token_cost.total_estimated_cost}")

            except Exception as e:
                logging.error(f"Error persisting token cost: {e}")

        # Frontend will now detect SQL ACTION marker to show Run button
        send_socket_message(
            sid=sid,
            channel_id=channel_id,
            chat_id=chat_id,
            chat_message_id=chat_message_id,
            prompt_status=ChatMessageStatus.SUCCESS,
        )
        # After sending final response to frontend
        self.byte_counter.pop(chat_message_id, None)

        # On successful completion, Closing the event thread
        raise StopIteration

    def process_event(self, *args, **kwargs):
        supported_events = ["thought_chain", "prompt_response",
                            "summary", "chat_name", "completed"]
        event_type = kwargs.get("event_type")
        if event_type not in supported_events:
            raise ValueError(f"Unsupported event type: {event_type}")
        getattr(self, f"_process_{event_type}")(*args, **kwargs)
