from rest_framework import serializers
from backend.core.models.chat_intent import ChatIntent

class ChatIntentSerializer(serializers.ModelSerializer):
    class Meta:
        model = ChatIntent
        fields = [
            'chat_intent_id',
            'name',
            'display_name',
        ]
