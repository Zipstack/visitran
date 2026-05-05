from rest_framework import serializers
from backend.core.models.chat import Chat
from backend.core.routers.chat_message.serializers import UserMinimalSerializer


class ChatSerializer(serializers.ModelSerializer):
    user = UserMinimalSerializer(read_only=True)
    chat_intent_name = serializers.CharField(
        source='chat_intent.display_name', read_only=True, default=None
    )

    class Meta:
        model = Chat
        fields = [
            'chat_id',
            'project_id',
            'chat_name',
            'chat_intent',
            'chat_intent_name',
            'created_at',
            'modified_at',
            'is_deleted',
            'user',
            'llm_model_architect',
            'llm_model_developer',
        ]
