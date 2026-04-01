import logging
from typing import List

from backend.application.context.application import ApplicationContext
from backend.core.models.chat import Chat
from backend.core.models.chat_intent import ChatIntent
from backend.core.models.chat_message import ChatMessage
from backend.core.routers.chat.constants import CHAT_LLM_MODELS
from backend.core.routers.chat_message.constants import ChatMessageStatus
from backend.core.web_socket import send_socket_message
from backend.errors import ChatMessageNotFound, ChatNotFound, InvalidChatMessageStatus, InvalidChatPrompt


class ChatMessageContext(ApplicationContext):
    """Context class for creating, updating, listing, and deleting
    Chat/ChatMessage records within a given project."""

    def __init__(self, project_id: str) -> None:
        """Initialize the ChatMessageContext with a specific project_id.

        Args:
            project_id (str): The UUID of the project context.
        """
        super().__init__(project_id)
        self.project_id = project_id
        self.generate_model_list = []
        self.byte_counter = {}
        self.pubsub = self.session.redis_client.pubsub()

    def _get_chat_or_raise(self, chat_id: str, must_be_active: bool = True) -> Chat:
        """Retrieve a single Chat within the given project.

        Raise an error if not found. If must_be_active=True, the chat
        must not be soft-deleted.
        """
        filters = {"chat_id": chat_id, "project": self.project_instance}
        if must_be_active:
            filters["is_deleted"] = False

        try:
            return Chat.objects.get(**filters)
        except Chat.DoesNotExist:
            raise ChatNotFound(chat_id=chat_id)

    def _get_chat_message(self, chat_id: str, chat_message_id: str) -> ChatMessage:
        chat = self._get_chat_or_raise(chat_id=chat_id, must_be_active=True)
        try:
            filters = {"chat": chat, "chat_message_id": chat_message_id}
            return ChatMessage.objects.get(**filters)
        except ChatMessage.DoesNotExist:
            raise ChatMessageNotFound(
                chat_id=chat_id,
                chat_name=chat.chat_name,
                chat_message_id=chat_message_id,
            )

    def get_all_chats(self):
        """Return all non-deleted chats for self.project_id."""
        return Chat.objects.filter(project=self.project_instance, is_deleted=False).order_by("-modified_at")

    def get_single_chat(self, chat_id: str):
        """Return a single non-deleted chat matching chat_id."""
        return self._get_chat_or_raise(chat_id=chat_id, must_be_active=True)

    def delete_chat(self, chat_id: str) -> None:
        """Soft-delete the specified chat."""
        chat = self._get_chat_or_raise(chat_id=chat_id, must_be_active=False)
        chat.is_deleted = True
        chat.save()

    def update_chat_name(self, chat_id: str, chat_name: str) -> Chat:
        """Update the name of the specified chat.

        Args:
            chat_id (str): The unique ID of the chat to update.
            chat_name (str): The new name for the chat.

        Returns:
            Chat: The updated chat instance.
        """
        chat = self._get_chat_or_raise(chat_id=chat_id, must_be_active=True)
        # Truncate to 255 chars to match database field max_length
        chat.chat_name = chat_name[:255]
        chat.save(update_fields=["chat_name"])
        return chat

    def get_chat_messages(self, chat_id: str) -> list[ChatMessage]:
        """Return all messages for the given chat, ensuring the chat is valid
        and active, sorted by creation time (ascending)."""
        chat = self._get_chat_or_raise(chat_id)
        return ChatMessage.objects.filter(chat=chat).order_by("created_at")

    def stop_chat(self, chat_id, chat_message_id, channel_id, sid):
        channel = f"{channel_id}_cancel_channel"

        try:
            # Subscribe to the channel
            self.pubsub.subscribe(channel)
            logging.info(f"Subscribed to Redis channel: {channel}")

            # Publish the STOP message
            self.session.redis_client.publish(channel, "STOP")
            logging.info(f"Published STOP message to channel: {channel}")
            send_socket_message(
                sid=sid,
                channel_id=channel_id,
                chat_id=chat_id,
                chat_message_id=chat_message_id,
                prompt_status=ChatMessageStatus.FAILED,
                transformation_status=ChatMessageStatus.FAILED,
            )

        except Exception as e:
            logging.error(f"[Error] Failed to send STOP message: {e}")

        finally:
            if self.pubsub:
                self.pubsub.close()
                logging.error(f"Closed pubsub for channel: {channel}")

        return True

    def persist_prompt(
        self,
        prompt: str,
        discussion_type: str,
        llm_model_architect: str,
        llm_model_developer: str,
        generated_chat_res_id: str = None,
        chat_intent_id: str = None,
        chat_id: str = None,
        user=None,
    ) -> ChatMessage:
        """Create a new prompt within a Chat.

        If chat_id is None, create a new Chat. Return the
        chat_message_id of the newly created ChatMessage.
        """
        if not prompt.strip():
            raise InvalidChatPrompt()

        chat_intent = None
        transformation_type = "TRANSFORM" if discussion_type == "GENERATE" else "DISCUSSION"
        if chat_intent_id:
            try:
                chat_intent = ChatIntent.objects.get(chat_intent_id=chat_intent_id)
            except ChatIntent.DoesNotExist:
                chat_intent = None

        if not chat_id:
            chat = Chat.objects.create(
                project=self.project_instance,
                chat_name="Untitled Chat",
                chat_intent=chat_intent,
                llm_model_architect=llm_model_architect,
                llm_model_developer=llm_model_developer,
                user=user,
            )
        else:
            chat = self._get_chat_or_raise(chat_id=chat_id, must_be_active=True)
            chat.chat_intent = chat_intent
            chat.llm_model_architect = llm_model_architect
            chat.llm_model_developer = llm_model_developer
            chat.discussion_type = discussion_type
            chat.last_discussion_id = generated_chat_res_id
            chat.transformation_type = transformation_type
            chat.save()

        chat_message = ChatMessage.objects.create(
            chat=chat,
            prompt=prompt,
            chat_intent=chat_intent,
            llm_model_architect=llm_model_architect,
            llm_model_developer=llm_model_developer,
            discussion_type=discussion_type,
            last_discussion_id=generated_chat_res_id,
            transformation_type=transformation_type,
            user=user,
        )

        return chat_message

    @staticmethod
    def get_llm_models() -> list:
        """Return the list of LLM models from CHAT_LLM_MODELS constant.

        Returns:
            list: A list of LLM model definitions from the JSON file.
        """
        return CHAT_LLM_MODELS

    @staticmethod
    def get_chat_intents() -> list[ChatIntent]:
        """Retrieve all available chat intents.

        Returns:
            list[ChatIntent]: A list of all defined ChatIntent objects.
        """
        return ChatIntent.objects.all()

    def persist_response(
        self,
        chat_id: str,
        chat_message_id: str,
        response: str = None,
        is_append_response: bool = False,
        technical_content: str = None,
        response_time: int = None,
        chat_name: str = None,
        discussion_status: str = None,
    ) -> ChatMessage:
        """Update a ChatMessage with a response. Optionally rename the Chat.

        Args:
            chat_id (str): The unique ID of the chat to update.
            chat_message_id (str): The unique ID of the chat message to update (Chat messages inside chat).
            response (str): The new response content.
            is_append_response(bool): If True, append the response to the existing response.
            technical_content (str): The new technical content (YAML, SQL, etc.); may be empty for some intents.
            response_time (int): The time taken (in ms) to generate the response (mandatory).
            chat_name (str, optional): If provided, rename the Chat.
            :param discussion_status:
        """
        chat_message = self._get_chat_message(chat_id=chat_id, chat_message_id=chat_message_id)
        fields_to_update = []

        if discussion_status:
            chat_message.discussion_type = discussion_status
            fields_to_update.append("discussion_type")
            if discussion_status == "GENERATE":
                chat_message.transformation_type = "TRANSFORM"
                fields_to_update.append("transformation_type")
        if response:
            if is_append_response:
                chat_message.response = (chat_message.response or "") + response
            else:
                chat_message.response = response
            fields_to_update.append("response")

        if technical_content:
            chat_message.technical_content = technical_content
            fields_to_update.append("technical_content")

        # Always update response_time (it's mandatory).
        chat_message.response_time = response_time
        fields_to_update.append("response_time")
        fields_to_update.append("discussion_type")

        # Save ChatMessage only once for updated fields
        chat_message.save(update_fields=fields_to_update)

        # If chat_name is provided, rename Chat (separate model => separate save)
        # Truncate to 255 chars to match database field max_length
        if chat_name:
            chat_message.chat.chat_name = chat_name[:255]
            chat_message.chat.save(update_fields=["chat_name"])

        return chat_message

    @staticmethod
    def _persist_status_field(
        chat_message_id: str,
        status_field: str,
        error_field: str,
        status: str,
        error_message: dict = None,
        generated_models: list = None,
    ) -> ChatMessage:
        valid_statuses = [
            ChatMessageStatus.YET_TO_START,
            ChatMessageStatus.RUNNING,
            ChatMessageStatus.SUCCESS,
            ChatMessageStatus.FAILED,
        ]

        if status not in valid_statuses:
            raise InvalidChatMessageStatus(invalid_status=status, valid_status=valid_statuses)

        chat_message = ChatMessage.objects.get(chat_message_id=chat_message_id)
        setattr(chat_message, status_field, status)
        setattr(chat_message, error_field, error_message if status == ChatMessageStatus.FAILED else None)
        setattr(chat_message, "generated_models", generated_models)
        chat_message.save()
        return chat_message

    def persist_prompt_status(self, chat_message_id: str, status: str, error_message: dict = None) -> ChatMessage:
        """Update the prompt_status and prompt_error_message fields in
        ChatMessage."""
        return self._persist_status_field(
            chat_message_id=chat_message_id,
            status_field="prompt_status",
            error_field="prompt_error_message",
            status=status,
            error_message=error_message,
        )

    def persist_transformation_status(
        self,
        chat_message_id: str,
        status: str,
        error_message: dict = None,
        generated_models: list = None,
    ) -> ChatMessage:
        """Update the transformation_status and transformation_error_message
        fields in ChatMessage."""
        return self._persist_status_field(
            chat_message_id=chat_message_id,
            status_field="transformation_status",
            error_field="transformation_error_message",
            status=status,
            error_message=error_message,
            generated_models=generated_models,
        )

    def persist_thought_chain(self, chat_id: str, chat_message_id: str, thought_chain: str):
        """thought_chain (str): The thoughts that went into generating the
        response.

        :param chat_id:
        :param chat_message_id:
        :param thought_chain:
        :return:
        """
        chat_message = self._get_chat_message(chat_id=chat_id, chat_message_id=chat_message_id)
        chat_message.thought_chain.append(thought_chain)
        chat_message.save()
