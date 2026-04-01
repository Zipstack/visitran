from rest_framework import status, viewsets
from rest_framework.response import Response

from backend.application.context.chat_message_context import ChatMessageContext
from backend.core.routers.chat_intent.serializers import ChatIntentSerializer


class ChatIntentView(viewsets.ViewSet):
    def list_chat_intents(self, request, project_id=None, *args, **kwargs):
        """
        Retrieve all available chat intents.

        This method instantiates a ChatMessageContext using the current user's ID
        and the given project_id, then fetches and returns all chat intents.
        """
        chat_ctx = ChatMessageContext(project_id=project_id)

        # Fetch and serialize chat intents
        chat_intents = chat_ctx.get_chat_intents()
        serializer = ChatIntentSerializer(chat_intents, many=True)

        return Response(serializer.data, status=status.HTTP_200_OK)

