import json
import logging
from typing import Any

import eventlet
import redis
from django.conf import settings

from backend.application.context.chat_ai_context import ChatAiContext
from backend.application.context.no_code_model import NoCodeModel
from backend.application.utils import send_event_to_llm_server
from backend.core.redis_client import RedisClient
from backend.core.routers.chat_message.constants import ChatMessageStatus
from backend.core.web_socket import send_socket_message, sio
from backend.errors import LLMModelFailure, AIRaisedException
from backend.errors.visitran_backend_base_exceptions import VisitranBackendBaseException

from backend.utils.tenant_context import _get_tenant_context, TenantContext
from backend.core.models.ai_context_rules import UserAIContextRules, ProjectAIContextRules


class LLMServerContext(ChatAiContext):
    def __init__(self, project_id: str) -> None:
        """
        Initialize the ChatMessageContext with a specific project_id.

        Args:
            project_id (str): The UUID of the project context.
        """
        super().__init__(project_id)
        self.chat_name = None
        self.project_id = project_id
        self.generate_model_list = []
        self.byte_counter = {}
        self.pubsub = self.session.redis_client.pubsub()
        self.redis_client = RedisClient().redis_client
        self.no_code_model = NoCodeModel(project_id)

    @staticmethod
    def extract_content_and_chat_name(content):
        if isinstance(content, dict):
            return content["answer"], content.get("chat_name", "")
        return content, ""

    def create_redis_xgroup(self, channel_id, group_id):
        try:
            self.redis_client.xgroup_create(channel_id, group_id, id="0", mkstream=True)
            logging.info(f"Consumer group '{group_id}' created.")
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" in str(e):
                pass  # group already exists
            else:
                raise

    def process_message(
        self,
        sid: str,
        channel_id: str,
        chat_id: str,
        chat_intent: str,
        payload: dict[str, Any],
        discussion_status: str
    ):
        data = json.loads(payload["data"])
        if payload.get("type") == "status" and payload.get("status") == "failed":
            payload = json.loads(payload["data"])
            if payload and "error_message" in payload:
                error_message = payload["error_message"]
                raise AIRaisedException(error_message=error_message, failure_reason=payload)
            raise LLMModelFailure(error_message="Visitran AI is unreachable", failure_reason=payload)

        event_type_map = {
            0: "thought_chain",
            1: "prompt_response",
            2: "summary",
            3: "chat_name",
            4: "completed",
            99: "stop",
        }

        event_type = event_type_map.get(data["event_type"], "completed")
        logging.info(f"Processing event for {event_type}")
        chat_message_id = data["chat_message_id"]
        content = data["content"]

        if event_type == "chat_name":
            self.chat_name = data["content"]
            self.persist_response(
                chat_id=chat_id,
                chat_message_id=chat_message_id,
                response=content,
                is_append_response=True,
                response_time=1,
                chat_name=self.chat_name,
                discussion_status=discussion_status,
            )

        if chat_intent == "INFO" and event_type == "prompt_response":
            event_type = "summary"

        self.process_event(
            event_type=event_type,
            channel_id=channel_id,
            chat_id=chat_id,
            chat_message_id=chat_message_id,
            sid=sid,
            content=content,
            chat_name=self.chat_name,
            chat_intent=chat_intent,
            discussion_status=discussion_status,
        )

    def _validate_message(self, group_id, channel_id):
        messages = self.redis_client.xreadgroup(
            groupname=group_id,
            consumername=settings.REDIS_STREAMER_CONSUMER,
            streams={channel_id: ">"},
            count=1,
            block=5000,  # wait up to 5 seconds
        )
        return messages

    def _handle_redis_message(self, sid, channel_id, chat_id, chat_intent, group_id, messages, discussion_status: str):
        for _, msg_list in messages:
            for message_id, payload in msg_list:
                logging.info(f" === Message ID: {message_id} ===")
                try:
                    self.process_message(
                        sid=sid,
                        channel_id=channel_id,
                        chat_id=chat_id,
                        chat_intent=chat_intent,
                        payload=payload,
                        discussion_status=discussion_status
                    )
                    eventlet.sleep(0.1)
                finally:
                    self.redis_client.xack(channel_id, group_id, message_id)

    def __stream_listener(
        self, sid: str, channel_id: str, chat_id: str, chat_message_id: str, chat_intent: str, group_id: str, discussion_status: str
    ):

        while True:
            try:
                messages = self._validate_message(group_id, channel_id)

                if not messages:
                    continue

                self._handle_redis_message(sid, channel_id, chat_id, chat_intent, group_id, messages, discussion_status)

            except redis.exceptions.RedisError as e:
                logging.error(f"[REDIS ERROR] {e}")
                eventlet.sleep(1)  # prevent tight retry loop on error
            except StopIteration:
                # This is success state, Stopping the iteration loop
                logging.info("Stopping Redis listener thread after completion of all messages in the stream.")
                break
            except VisitranBackendBaseException as error:
                logging.error(f"[ERROR] Failed in llm server: {error.error_response()}")
                self.persist_prompt_status(
                    chat_message_id=chat_message_id,
                    status=ChatMessageStatus.FAILED,
                    error_message=error.error_response(),
                )
                send_socket_message(
                    sid=sid,
                    channel_id=channel_id,
                    chat_id=chat_id,
                    chat_message_id=chat_message_id,
                    error_msg_content=error.error_response(),
                    prompt_status=ChatMessageStatus.FAILED,
                )
                break
            except Exception as error:
                # All the exceptions are handled internally
                logging.critical("Stopping Redis listener thread due to some failure")
                logging.exception(error)
                error_payload = {
                    "status": "failed",
                    "error_message": str(error),
                }
                self.persist_prompt_status(
                    chat_message_id=chat_message_id,
                    status=ChatMessageStatus.FAILED,
                    error_message=error_payload,
                )
                send_socket_message(
                    sid=sid,
                    channel_id=channel_id,
                    chat_id=chat_id,
                    chat_message_id=chat_message_id,
                    error_msg_content=error_payload,
                    prompt_status=ChatMessageStatus.FAILED,
                )
                break

    def listen_to_redis_stream(self, sid: str, channel_id: str, chat_id: str, chat_message_id: str, chat_intent: str, discussion_status: str):
        """Listens to the Redis stream from llm server and processes the messages."""
        group_id = f"group_{chat_id}_{chat_message_id}"
        self.create_redis_xgroup(channel_id, group_id)
        self.__stream_listener(sid, channel_id, chat_id, chat_message_id, chat_intent, group_id, discussion_status)

    def stream_prompt_response(self, sid: str, channel_id: str, chat_id: str, chat_message_id: str, chat_intent: str, discussion_status: str):
        """Starts a background thread to listen redis pubsub channel from AI server"""
        args = (sid, channel_id, chat_id, chat_message_id, chat_intent, discussion_status)
        try:
            sio.start_background_task(self.listen_to_redis_stream, *args)
        except Exception as e:
            logging.error(f"[ERROR] Failed to start background thread: {e}")
            raise e

    def process_prompt(self, sid: str, channel_id: str, chat_id: str, chat_message_id: str, is_retry: bool, org_id: str):
        """
        Returns the prompt response from the chat message
        :param is_retry: second attempt
        :param sid: The socket client id
        :param channel_id: The channel id
        :param chat_id: The chat id
        :param chat_message_id: The chat message id
        :return: The prompt response
        """
        try:
            chat_message = self._get_chat_message(chat_id=chat_id, chat_message_id=chat_message_id)
            chat_id = str(chat_message.chat.chat_id)
            chat_message_id = str(chat_message.chat_message_id)
            chat_name = chat_message.chat_name
            chat_name = None if chat_name == "Untitled Chat" else chat_name
            discussion_status = chat_message.discussion_type
            transformation_type = chat_message.transformation_type
            final_discussion_chat_id = chat_message.last_discussion_id

            DISCUSSION_STATUS_MAP = {
                "APPROVED": ChatMessageStatus.APPROVED,
                "DISAPPROVED": ChatMessageStatus.DISAPPROVED,
                "GENERATE": ChatMessageStatus.GENERATE,
            }
            if is_retry:
                chat_intent = ChatMessageStatus.TRANSFORM_RETRY
                prompt = (
                    f"Faulty yaml:{chat_message.technical_content} \n Error:{chat_message.transformation_error_message}"
                )
            else:
                chat_intent = chat_message.chat_intent.name
                prompt = chat_message.prompt

            if discussion_status in DISCUSSION_STATUS_MAP:
                chat_message.discussion_type = DISCUSSION_STATUS_MAP[discussion_status]
                if discussion_status == "GENERATE":
                    transformation_type = 'TRANSFORM'
                    chat_message.transformation_type = transformation_type

            # Fail fast if OSS mode lacks API key — before any DB work
            from backend.application.ws_client import check_oss_api_key_configured
            check_oss_api_key_configured()

            self.persist_prompt_status(chat_message_id=chat_message_id, status=ChatMessageStatus.RUNNING)
            content = "Preparing database information..."
            self.send_and_persist_thought_chain(
                sid=sid,
                channel_id=channel_id,
                chat_id=chat_id,
                chat_message_id=chat_message_id,
                content=content,
                prompt_status=ChatMessageStatus.RUNNING,
            )

            db_metadata: str = self.visitran_context.get_db_metadata()
            visitran_models: str = self.get_visitran_models()

            tenant_context: TenantContext = _get_tenant_context()

            # API key comes from .env (VISITRAN_AI_KEY)
            api_token = getattr(settings, "VISITRAN_AI_KEY", "")

            # Get user for context rules lookup
            user = None
            try:
                from backend.core.models.user_model import User
                user = User.objects.get(user_id=tenant_context.user.get("user_id"))
            except Exception:
                pass

            # Fetch user's personal context rules
            user_context_rules = ""
            try:
                user_rules = UserAIContextRules.objects.filter(user=user).first() if user else None
                if user_rules and user_rules.context_rules:
                    user_context_rules = user_rules.context_rules
            except Exception as e:
                logging.warning(f"Failed to fetch user context rules: {e}")

            # Fetch project context rules
            project_context_rules = ""
            try:
                project_rules = ProjectAIContextRules.objects.filter(project=self.project_instance).first()
                if project_rules and project_rules.context_rules:
                    project_context_rules = project_rules.context_rules
            except Exception as e:
                logging.warning(f"Failed to fetch project context rules: {e}")

            llm_payload = {
                "tenant_id": self.session.tenant_id,
                "org_id": org_id,
                "project_id": self.project_id,
                "channel_id": channel_id,
                "chat_id": chat_id,
                "chat_message_id": chat_message_id,
                "prompt": prompt,
                "db_map": db_metadata,
                "visitran_model": visitran_models,
                "chat_name": chat_name,
                "chat_intent": chat_intent,
                "db_type": self.project_instance.database_type,
                "llm_model_architect": chat_message.llm_model_architect,
                "llm_model_developer": chat_message.llm_model_developer,
                "transformation_type": transformation_type,
                "discussion_type": chat_message.discussion_type,
                "api_access_token": api_token,
                "user_context_rules": user_context_rules,
                "project_context_rules": project_context_rules,
            }

            # In WebSocket mode, send_event_to_llm_server() blocks the
            # current thread while it relays AI response chunks into local
            # Redis.  Start the Redis stream listener FIRST (in a parallel
            # thread) so it can deliver chunks to the frontend in real-time
            # instead of all at once after the blocking call returns.
            from backend.application.ws_client import is_ws_mode
            ws_mode = is_ws_mode()
            if ws_mode:
                self.stream_prompt_response(
                    sid=sid,
                    channel_id=channel_id,
                    chat_id=chat_id,
                    chat_message_id=chat_message_id,
                    chat_intent=chat_intent,
                    discussion_status=chat_message.discussion_type,
                )

            send_event_to_llm_server(payload=llm_payload)

            if not ws_mode:
                self.stream_prompt_response(
                    sid=sid,
                    channel_id=channel_id,
                    chat_id=chat_id,
                    chat_message_id=chat_message_id,
                    chat_intent=chat_intent,
                    discussion_status=chat_message.discussion_type,
                )
            logging.info(f"process_prompt: chat_intent={chat_intent}, sid={sid}, channel_id={channel_id}")
            chat_message = self._get_chat_message(chat_id=chat_id, chat_message_id=chat_message_id)

            return chat_message
        except Exception as e:
            self.persist_prompt_status(
                chat_message_id=chat_message_id,
                status=ChatMessageStatus.FAILED,
                error_message={"error_message": str(e)},
            )
            raise e

    def transform_retry(self, sid: str, channel_id: str, chat_id: str, chat_message_id: str):
        send_socket_message(
            sid=sid,
            channel_id=channel_id,
            chat_id=chat_id,
            chat_message_id=chat_message_id,
            transformation_status=ChatMessageStatus.RUNNING,
        )
        self.process_prompt(
            sid=sid,
            channel_id=channel_id,
            chat_id=chat_id,
            chat_message_id=chat_message_id,
            is_retry=True,
        )
        self.transformation_save(sid=sid, channel_id=channel_id, chat_id=chat_id, chat_message_id=chat_message_id)

    def transformation_save(self, chat_id: str, chat_message_id: str, channel_id: str, sid: str) -> dict:
        """
        Get chat message details and extract the response for transformation.

        Args:
            chat_id (str): The unique ID of the chat.
            chat_message_id (str): The unique ID of the chat message.

        Returns:
            dict: Dictionary containing the response from the chat message

        Raises:
            VisitranValidationExceptions: If the chat message doesn't exist
        """
        # Get the chat message using the existing function
        chat_message = self._get_chat_message(chat_id=chat_id, chat_message_id=chat_message_id)

        # Get and validate a response
        if not chat_message.technical_content:
            send_socket_message(
                sid=sid,
                channel_id=channel_id,
                chat_id=chat_id,
                chat_message_id=chat_message_id,
                error_msg_content={"error_message": "No response found in chat message"},
                error_state="Transformation Apply",
                transformation_status=ChatMessageStatus.FAILED,
            )
            raise ValueError("No response found in chat message")

        send_socket_message(
            sid=sid,
            channel_id=channel_id,
            chat_id=chat_id,
            chat_message_id=chat_message_id,
            transformation_status=ChatMessageStatus.RUNNING,
        )

        # Get raw response
        raw_response = chat_message.technical_content
        # self.persist_transformation_status(chat_message_id, ChatMessageStatus.RUNNING)
        parsed_yaml = self.extract_yaml_text(raw_response)
        stats = {
            "generated_models": [],
            "generated_model_count": 0,
            "updated_models": [],
            "updated_model_count": 0,
        }

        for model in parsed_yaml:
            model_name = model.get("model_name")
            is_model_exists = self.session.check_model_exists(model_name=model_name)
            generate_model = not bool(is_model_exists)
            update_model = bool(is_model_exists)

            response = {"file": model}
            if generate_model:
                try:
                    stats["generated_models"].append(model_name)
                    stats["generated_model_count"] += 1
                    print("generating model", generate_model, "----", model_name)
                    new_model_name = model_name
                    try:
                        new_model_name = self.create_a_model(model_name=model_name, is_generate_ai_request=True)
                        self.generate_model_list.append(new_model_name)
                    except Exception as e:
                        # Model already exist, Updating the model
                        pass
                    send_socket_message(
                        sid=sid,
                        channel_id=channel_id,
                        chat_id=chat_id,
                        chat_message_id=chat_message_id,
                        transformation_status=ChatMessageStatus.RUNNING,
                        generate_model_list=self.generate_model_list,
                        generate_model_status=ChatMessageStatus.MODEL_CREATED,
                    )
                    # Save the response

                    self.save_model_file(response, model_name=new_model_name, is_chat_response=True)
                    self.persist_transformation_status(
                        chat_message_id, ChatMessageStatus.RUNNING, {}, self.generate_model_list)
                except Exception as error_message:
                    print(f"Error on Create and Saving visitran model: {error_message}")

                    # Determine attempt number based on whether error already exists
                    attempt_number = 2 if chat_message.transformation_error_message else 1

                    # Send thought chain message with error details
                    self.send_and_persist_thought_chain(
                        sid=sid,
                        channel_id=channel_id,
                        chat_id=chat_id,
                        chat_message_id=chat_message_id,
                        content=f"⚠️ Attempt {attempt_number} failed: {str(error_message)}",
                        prompt_status=ChatMessageStatus.RUNNING
                    )

                    if chat_message.transformation_error_message:
                        self.persist_transformation_status(
                            chat_message_id, ChatMessageStatus.FAILED, {
                                "error_message": str(error_message)}, self.generate_model_list
                        )
                        send_socket_message(
                            sid=sid,
                            channel_id=channel_id,
                            chat_id=chat_id,
                            chat_message_id=chat_message_id,
                            transformation_status=ChatMessageStatus.FAILED,
                            generate_model_list=self.generate_model_list,
                            generate_model_status=ChatMessageStatus.MODEL_GENERATION_FAILED,
                            error_msg_content={"error_message": str(error_message)},
                            error_state="Model Creation",
                            is_retry_transform=False,
                        )
                    elif not chat_message.transformation_error_message:
                        self.persist_transformation_status(
                            chat_message_id, ChatMessageStatus.FAILED, {
                                "error_message": str(error_message)}, self.generate_model_list
                        )
                        send_socket_message(
                            sid=sid,
                            channel_id=channel_id,
                            chat_id=chat_id,
                            chat_message_id=chat_message_id,
                            error_msg_content={"error_message": str(error_message)},
                            error_state="Parsing Error",
                            transformation_status=ChatMessageStatus.FAILED,
                            generate_model_list=self.generate_model_list,
                            is_retry_transform=True,
                        )
                        # self.transform_retry(sid=sid, channel_id=channel_id, chat_id=chat_id,chat_message_id=chat_message_id)

                    raise
            elif update_model:
                stats["updated_models"].append(model_name)
                stats["updated_model_count"] += 1
                print("updating model", update_model, "---", model_name)
                # Execute visitran model(save)
                try:
                    # Save the response
                    self.save_model_file(response, model_name=model_name, is_chat_response=True, is_update=True)
                    send_socket_message(
                        sid=sid,
                        channel_id=channel_id,
                        chat_id=chat_id,
                        chat_message_id=chat_message_id,
                        transformation_status=ChatMessageStatus.RUNNING,
                        generate_model_status=ChatMessageStatus.MODEL_UPDATED,
                    )
                except Exception as error_message:
                    print(f"Error on Updating visitran model: {error_message}")

                    # Determine attempt number based on whether error already exists
                    attempt_number = 2 if chat_message.transformation_error_message else 1

                    # Send thought chain message with error details
                    self.send_and_persist_thought_chain(
                        sid=sid,
                        channel_id=channel_id,
                        chat_id=chat_id,
                        chat_message_id=chat_message_id,
                        content=f"⚠️ Attempt {attempt_number} failed: {str(error_message)}",
                        prompt_status=ChatMessageStatus.RUNNING
                    )

                    if chat_message.transformation_error_message:
                        self.persist_transformation_status(
                            chat_message_id, ChatMessageStatus.FAILED, {
                                "error_message": str(error_message)}, self.generate_model_list
                        )
                        send_socket_message(
                            sid=sid,
                            channel_id=channel_id,
                            chat_id=chat_id,
                            chat_message_id=chat_message_id,
                            transformation_status=ChatMessageStatus.FAILED,
                            generate_model_status=ChatMessageStatus.MODEL_UPDATE_FAILED,
                            error_msg_content={"error_message": str(error_message)},
                            error_state="Model Update",
                            is_retry_transform=False,
                        )
                    elif not chat_message.transformation_error_message:
                        self.persist_transformation_status(
                            chat_message_id, ChatMessageStatus.FAILED, {
                                "error_message": str(error_message)}, self.generate_model_list
                        )
                        send_socket_message(
                            sid=sid,
                            channel_id=channel_id,
                            chat_id=chat_id,
                            chat_message_id=chat_message_id,
                            error_msg_content={"error_message": str(error_message)},
                            error_state="Parsing Error",
                            transformation_status=ChatMessageStatus.FAILED,
                            is_retry_transform=True,
                        )
                        for models in self.generate_model_list:
                            try:
                                logging.info(f"Deleting model: {models}")
                                self.delete_a_file_or_folder(file_path=f"models/no_code/{models}")
                                self.generate_model_list = []
                            except Exception as error_message:
                                print(f"Error on deleting model: {error_message}")
                                pass
                        # self.transform_retry(sid=sid, channel_id=channel_id, chat_id=chat_id,chat_message_id=chat_message_id)
                    raise

        try:
            # Execute only the generated/updated models and their downstream children
            affected_models = stats["generated_models"] + stats["updated_models"]
            if affected_models:
                self.execute_visitran_run_command(current_models=affected_models)
            else:
                self.execute_visitran_run_command()
            self.backup_current_no_code_model()
            self.persist_transformation_status(chat_message_id, ChatMessageStatus.SUCCESS, {}, self.generate_model_list)
            send_socket_message(
                sid=sid,
                channel_id=channel_id,
                chat_id=chat_id,
                chat_message_id=chat_message_id,
                transformation_status=ChatMessageStatus.SUCCESS,
                generate_model_list=self.generate_model_list,
                generate_model_status=ChatMessageStatus.MODEL_SAVE,
            )
        except Exception as error_message:
            print(f"Error on executing visitran models: {error_message}")
            if chat_message.transformation_error_message:
                self.persist_transformation_status(
                    chat_message_id, ChatMessageStatus.FAILED, {"error_message": str(error_message)}
                )
                send_socket_message(
                    sid=sid,
                    channel_id=channel_id,
                    chat_id=chat_id,
                    chat_message_id=chat_message_id,
                    transformation_status=ChatMessageStatus.FAILED,
                    generate_model_status=ChatMessageStatus.MODEL_UPDATE_FAILED,
                    error_msg_content={"error_message": str(error_message)},
                    error_state="Model Run",
                    is_retry_transform=False,
                )
            elif not chat_message.transformation_error_message:
                self.persist_transformation_status(
                    chat_message_id, ChatMessageStatus.FAILED, {"error_message": str(error_message)}
                )
                send_socket_message(
                    sid=sid,
                    channel_id=channel_id,
                    chat_id=chat_id,
                    chat_message_id=chat_message_id,
                    error_msg_content={"error_message": str(error_message)},
                    error_state="Parsing Error",
                    transformation_status=ChatMessageStatus.FAILED,
                    is_retry_transform=True,
                )
                # self.transform_retry(sid=sid, channel_id=channel_id, chat_id=chat_id,chat_message_id=chat_message_id)
            raise

        # Return as dictionary
        result = {
            "response": "parsed_yaml",
            "status": "success",
            "message": "Transformation applied and executed successfully",
        }
        return result
