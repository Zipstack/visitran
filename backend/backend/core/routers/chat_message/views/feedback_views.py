import logging
from datetime import datetime
from django.utils import timezone
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import IsAuthenticated

from backend.errors.error_codes import BackendErrorMessages
from backend.core.routers.chat_message.serializers.feedback_serializer import ChatMessageFeedbackSerializer
from backend.core.models.chat_message import ChatMessage
from backend.utils.tenant_context import get_organization


class ChatMessageFeedbackView(APIView):
    """API view for submitting and retrieving feedback (thumbs up/down) on a
    chat message response."""
    permission_classes = [IsAuthenticated]

    def post(self, request, chat_message_id, project_id=None, chat_id=None, **kwargs):
        """Submit feedback for a specific chat message.

        Args:
            request: The HTTP request
            org_id: Organization ID
            chat_message_id: UUID of the chat message to provide feedback for
        """
        try:
            # Get organization ID from header
            org_id = request.META.get('HTTP_X_ORGANIZATION')
            if not org_id:
                return Response(
                    {"error": BackendErrorMessages.ORGANIZATION_REQUIRED},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Find the chat message
            chat_message = ChatMessage.objects.filter(
                chat_message_id=chat_message_id
            ).first()

            if not chat_message:
                return Response(
                    {"error": BackendErrorMessages.CHAT_MESSAGE_NOT_FOUND},
                    status=status.HTTP_404_NOT_FOUND
                )

            # Validate and save feedback
            serializer = ChatMessageFeedbackSerializer(data=request.data)
            if serializer.is_valid():
                # Update the chat message with feedback data
                chat_message.has_feedback = True
                chat_message.feedback = serializer.validated_data.get('feedback')
                chat_message.feedback_comment = serializer.validated_data.get('feedback_comment', None)
                chat_message.feedback_timestamp = timezone.now()
                chat_message.save(
                    update_fields=[
                        'has_feedback', 'feedback',
                        'feedback_comment', 'feedback_timestamp'
                    ]
                )

                logging.info(
                    f"Feedback submitted for chat message {chat_message_id}: "
                    f"feedback={chat_message.feedback}"
                )

                return Response(
                    {"success": True, "message": "Feedback submitted successfully"},
                    status=status.HTTP_200_OK
                )

            # Use INVALID_FEEDBACK_FORMAT for serializer validation errors
            return Response(
                {"error": BackendErrorMessages.INVALID_FEEDBACK_FORMAT},
                status=status.HTTP_400_BAD_REQUEST
            )

        except Exception as e:
            logging.exception(f"Error submitting feedback for chat message {chat_message_id}")
            error_message = BackendErrorMessages.FEEDBACK_SUBMISSION_FAILED.format(
                chat_message_id=chat_message_id
            )
            return Response(
                {"error": error_message},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    def get(self, request, chat_message_id, project_id=None, chat_id=None, **kwargs):
        """Retrieve feedback status for a specific chat message.

        Args:
            request: The HTTP request
            chat_message_id: UUID of the chat message to retrieve feedback for
        """
        try:
            # Get organization ID from header
            org_id = request.META.get('HTTP_X_ORGANIZATION')
            if not org_id:
                return Response(
                    {"error": BackendErrorMessages.ORGANIZATION_REQUIRED},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Find the chat message - don't filter by organization_id which is causing the error
            chat_message = ChatMessage.objects.filter(
                chat_message_id=chat_message_id
            ).first()

            if not chat_message:
                return Response(
                    {"error": BackendErrorMessages.CHAT_MESSAGE_NOT_FOUND},
                    status=status.HTTP_404_NOT_FOUND
                )

            # Return feedback status
            response_data = {
                'has_feedback': chat_message.has_feedback,
            }

            # Only include feedback details if feedback exists
            if chat_message.has_feedback:
                response_data.update({
                    'feedback': chat_message.feedback,
                    'feedback_comment': chat_message.feedback_comment or '',
                    'feedback_timestamp': chat_message.feedback_timestamp
                })

            return Response(response_data, status=status.HTTP_200_OK)

        except Exception as e:
            logging.exception(f"Error retrieving feedback for chat message {chat_message_id}")
            error_message = BackendErrorMessages.FEEDBACK_RETRIEVAL_FAILED.format(
                chat_message_id=chat_message_id
            )
            return Response(
                {"error": error_message},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
