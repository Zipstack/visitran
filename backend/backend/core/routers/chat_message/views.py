from rest_framework import status, viewsets
from rest_framework.request import Request
from rest_framework.response import Response

from backend.application.context.chat_message_context import ChatMessageContext
from backend.core.routers.chat_message.serializers import ChatMessageSerializer


class ChatMessageView(viewsets.ViewSet):
    """Custom ViewSet handling retrieval of ChatMessages through
    ChatMessageContext."""

    def list_messages(self, request, project_id=None, chat_id=None, *args, **kwargs) -> Response:
        """Retrieve all chat messages for the given project_id and chat_id."""
        user_id = str(request.user.id)
        ctx = ChatMessageContext(project_id=project_id)
        messages = ctx.get_chat_messages(chat_id=chat_id)

        serializer = ChatMessageSerializer(messages, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

    def persist_prompt(self, request: Request, *args, **kwargs) -> Response:
        """Create a new prompt (ChatMessage) using data from the request body.

        Expects JSON with keys:
          - "project_id"
          - "user_id"
          - "chat_id" (optional)
          - "prompt"
        Returns:
          - chat_message_id (UUID)
        """
        data = request.data
        project_id = data.get("project_id")
        chat_id = data.get("chat_id")
        prompt = data.get("prompt")
        user_id = str(request.user.id)
        discussion_type = data.get("discussion_status")
        if discussion_type is None:
            discussion_type == "INPROGRESS"

        chat_message_context = ChatMessageContext(project_id=project_id)
        chat_message_id = chat_message_context.persist_prompt(
            prompt=prompt, chat_id=chat_id, discussion_type=discussion_type
        )

        return Response(data={"chat_message_id": chat_message_id}, status=status.HTTP_200_OK)
