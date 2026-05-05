from rest_framework import serializers
from backend.core.models.chat_message import ChatMessage
from backend.core.models.user_model import User


class UserMinimalSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = ['user_id', 'email', 'profile_picture_url']


class ChatMessageSerializer(serializers.ModelSerializer):
    user = UserMinimalSerializer(read_only=True)
    chat_intent_name = serializers.CharField(source='chat_intent.name', read_only=True, default=None)

    class Meta:
        model = ChatMessage
        fields = [
            'chat_message_id',
            'chat',
            'chat_name',
            'user',
            'prompt',
            'thought_chain',
            'response',
            'technical_content',
            'response_time',
            'prompt_status',
            'prompt_error_message',
            'transformation_status',
            'transformation_error_message',
            'chat_intent',
            'chat_intent_name',
            'llm_model_architect',
            'llm_model_developer',
            'created_at',
            'modified_at',
            'transformation_type',
            'discussion_type',
            'last_discussion_id',
            'generated_models'
        ]
