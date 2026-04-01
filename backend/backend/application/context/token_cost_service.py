import logging
from decimal import Decimal
from typing import Optional, Dict, Any

from backend.application.context.chat_message_context import ChatMessageContext
from backend.core.models.chat_message import ChatMessage
from backend.core.models.chat_session_cost import ChatSessionCost
from backend.core.models.chat_token_cost import ChatTokenCost


class TokenCostService(ChatMessageContext):
    """Service to handle token cost persistence and session management."""

    def create_token_cost_record(
            self,
            chat_message: ChatMessage,
            token_data: dict[str, Any],
            chat_intent: str,
            session_id: str,
            processing_time_ms: int = 0
    ) -> Optional[ChatTokenCost]:
        """Create a ChatTokenCost record from token data received from
        visitran_ai.

        Args:
            chat_message: The ChatMessage instance
            token_data: Token usage data from visitran_ai
            chat_intent: Type of the chat [CHAT, SQL, TRANSFORM]
            session_id: Session identifier
            processing_time_ms: Processing time in milliseconds

        Returns:
            ChatTokenCost instance if successful, None otherwise
        """
        try:
            # Extract token usage data
            architect_usage = token_data.get("architect_usage", {})
            developer_usage = token_data.get("developer_usage", {})

            # Create the token cost record
            token_cost = ChatTokenCost.objects.create(
                project=self.project_instance,
                chat_message=chat_message,
                chat=chat_message.chat,
                user=chat_message.user,
                session_id=session_id,
                chat_intent=chat_intent,

                # Architect LLM data
                architect_model_name=architect_usage.get("model_name", chat_message.llm_model_architect),
                architect_input_tokens=architect_usage.get("input_tokens", 0),
                architect_output_tokens=architect_usage.get("output_tokens", 0),
                architect_total_tokens=architect_usage.get("total_tokens", 0),
                architect_estimated_cost=Decimal(str(architect_usage.get("estimated_cost", 0))),

                # Developer LLM data
                developer_model_name=developer_usage.get("model_name", chat_message.llm_model_developer),
                developer_input_tokens=developer_usage.get("input_tokens", 0),
                developer_output_tokens=developer_usage.get("output_tokens", 0),
                developer_total_tokens=developer_usage.get("total_tokens", 0),
                developer_estimated_cost=Decimal(str(developer_usage.get("estimated_cost", 0))),

                # Processing metadata
                processing_time_ms=processing_time_ms,
                pricing_config=token_data.get("pricing_config", {})
            )

            logging.info(
                f"Created token cost record for message {chat_message.chat_message_id}: "
                f"${token_cost.total_estimated_cost} ({token_cost.total_tokens} tokens)"
            )

            # Update session totals
            TokenCostService.update_session_totals(session_id, str(token_cost.token_cost_id))

            return token_cost

        except Exception as e:
            logging.error(f"Error creating token cost record: {e}")
            return None

    @staticmethod
    def update_session_totals(session_id: str, token_id: str) -> Optional[ChatSessionCost]:
        """Update session totals based on all token costs for the session."""
        try:
            return ChatSessionCost.update_session_totals(session_id, token_id)
        except Exception as e:
            logging.error(f"Error updating session totals for {session_id}: {e}")
            return None

    @staticmethod
    def get_session_summary(session_id: str) -> Optional[dict[str, Any]]:
        """Get session cost summary."""
        try:
            session_cost = ChatSessionCost.objects.filter(session_id=session_id).first()
            if session_cost:
                return {
                    'session_id': session_id,
                    'total_messages': session_cost.total_messages,
                    'total_tokens': session_cost.total_tokens,
                    'total_cost': float(session_cost.total_estimated_cost),
                    'architect_cost': float(session_cost.architect_total_cost),
                    'developer_cost': float(session_cost.developer_total_cost),
                    'is_active': session_cost.is_active
                }
            return None
        except Exception as e:
            logging.error(f"Error getting session summary for {session_id}: {e}")
            return None
