from rest_framework import viewsets, status
from rest_framework.request import Request
from rest_framework.response import Response

from backend.application.context.chat_message_context import ChatMessageContext
from backend.core.routers.chat_message.serializers import ChatMessageSerializer
from backend.core.web_socket import get_token_usage_data


class ChatMessageView(viewsets.ViewSet):
    """
    Custom ViewSet handling retrieval of ChatMessages through ChatMessageContext.
    """

    def list_messages(self, request, project_id=None, chat_id=None, *args, **kwargs) -> Response:
        """
        Retrieve all chat messages for the given project_id and chat_id.
        """
        user_id = str(request.user.id)
        ctx = ChatMessageContext(project_id=project_id)
        messages = ctx.get_chat_messages(chat_id=chat_id)

        serializer = ChatMessageSerializer(messages, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

    def persist_prompt(self, request: Request, *args, **kwargs) -> Response:
        """
        Create a new prompt (ChatMessage) using data from the request body.

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
        user = request.user if request.user.is_authenticated else None
        discussion_type = data.get('discussion_status')
        if discussion_type is None:
            discussion_type == 'INPROGRESS'

        chat_message_context = ChatMessageContext(project_id=project_id)
        chat_message_id = chat_message_context.persist_prompt(prompt=prompt, chat_id=chat_id, discussion_type=discussion_type, user=user)

        return Response(data={"chat_message_id": chat_message_id}, status=status.HTTP_200_OK)

    def get_token_usage(self, request, project_id=None, chat_id=None, chat_message_id=None, *args, **kwargs) -> Response:
        """
        Get token usage data for a specific chat message.

        Returns:
            - remaining_balance: Current token balance
            - total_consumed: Total tokens consumed
            - message_tokens_consumed: Tokens consumed for this specific message
            - token_usage_found: Whether token usage was found for this message
        """
        try:
            # Get organization ID from header
            org_id = request.META.get('HTTP_X_ORGANIZATION')
            if not org_id:
                return Response(
                    {"error": "Organization header is required"},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Get token usage data using the existing function
            token_data = get_token_usage_data(org_id, str(chat_message_id), str(chat_id))

            if token_data is None:
                return Response(
                    {"error": "Failed to retrieve token usage data"},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )

            return Response(token_data, status=status.HTTP_200_OK)

        except Exception as e:
            return Response(
                {"error": f"Error retrieving token usage: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
