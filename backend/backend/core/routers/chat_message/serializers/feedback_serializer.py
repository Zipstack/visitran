from rest_framework import serializers
from backend.core.models.chat_message import ChatMessage


class ChatMessageFeedbackSerializer(serializers.ModelSerializer):
    """
    Serializer for submitting feedback on a chat message response.
    """
    class Meta:
        model = ChatMessage
        fields = ['has_feedback', 'feedback', 'feedback_comment']
        read_only_fields = ['has_feedback']

    def validate(self, attrs):
        """
        Validates the feedback value.
        """
        feedback_value = attrs.get('feedback', None)
        
        if not feedback_value:
            raise serializers.ValidationError(
                {"feedback": "This field is required for providing feedback."}
            )
        
        # Validate the value matches our choices
        if feedback_value not in ['0', 'P', 'N']:
            raise serializers.ValidationError(
                {"feedback": "Must be one of '0' (neutral), 'P' (positive), or 'N' (negative)."}
            )
            
        return attrs
