from rest_framework import serializers
from backend.core.models.chat_message import ChatMessage


class ChatMessageSerializer(serializers.ModelSerializer):
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
            'llm_model_architect',
            'llm_model_developer',
            'created_at',
            'modified_at',
            'transformation_type',
            'discussion_type',
            'last_discussion_id',
            'generated_models',
        ]
