import uuid
from decimal import Decimal

from django.core.validators import MinValueValidator
from django.db import models
from django.db.models import Sum, Count

from backend.core.models.chat import Chat
from backend.core.models.user_model import User
from utils.models.base_model import BaseModel


class ChatSessionCost(BaseModel):
    """
    Aggregates token costs at the session level for better analytics.
    Auto-updated when ChatTokenCost records are created/updated.
    """

    session_cost_id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False,
        help_text="Unique identifier for this session cost record."
    )

    session_id = models.CharField(
        max_length=255,
        unique=True,
        help_text="Session identifier - must be unique."
    )

    chat = models.ForeignKey(
        Chat,
        on_delete=models.CASCADE,
        related_name="session_costs",
        help_text="Primary chat for this session."
    )

    user = models.ForeignKey(
        User,
        null=True,
        on_delete=models.CASCADE,
        related_name="session_costs",
        help_text="User who owns this session."
    )

    # Aggregated totals
    total_messages = models.PositiveIntegerField(
        default=0,
        help_text="Total number of messages in this session."
    )

    total_input_tokens = models.PositiveIntegerField(
        default=0,
        validators=[MinValueValidator(0)],
        help_text="Total input tokens across all messages in session."
    )

    total_output_tokens = models.PositiveIntegerField(
        default=0,
        validators=[MinValueValidator(0)],
        help_text="Total output tokens across all messages in session."
    )

    total_tokens = models.PositiveIntegerField(
        default=0,
        validators=[MinValueValidator(0)],
        help_text="Total tokens across all messages in session."
    )

    total_estimated_cost = models.DecimalField(
        max_digits=12,
        decimal_places=8,
        default=Decimal('0.00000000'),
        validators=[MinValueValidator(Decimal('0'))],
        help_text="Total estimated cost for the entire session."
    )

    # Architect-specific totals
    architect_total_tokens = models.PositiveIntegerField(
        default=0,
        help_text="Total tokens used by architect LLM in session."
    )

    architect_total_cost = models.DecimalField(
        max_digits=10,
        decimal_places=8,
        default=Decimal('0.00000000'),
        help_text="Total cost for architect LLM in session."
    )

    # Developer-specific totals
    developer_total_tokens = models.PositiveIntegerField(
        default=0,
        help_text="Total tokens used by developer LLM in session."
    )

    developer_total_cost = models.DecimalField(
        max_digits=10,
        decimal_places=8,
        default=Decimal('0.00000000'),
        help_text="Total cost for developer LLM in session."
    )

    # Session metadata
    session_start_time = models.DateTimeField(
        auto_now_add=True,
        help_text="When the session started."
    )

    last_message_time = models.DateTimeField(
        auto_now=True,
        help_text="When the last message was processed."
    )

    is_active = models.BooleanField(
        default=True,
        help_text="Whether the session is still active."
    )

    @classmethod
    def update_session_totals(cls, session_id: str, token_id: str):
        """Update session totals based on related ChatTokenCost records."""
        from backend.core.models.chat_token_cost import ChatTokenCost

        # Get all token costs for this session
        token_costs = ChatTokenCost.objects.filter(session_id=session_id)

        if not token_costs.exists():
            return None

        # Calculate aggregates
        aggregates = token_costs.aggregate(
            total_messages=Count('token_cost_id'),
            total_input_tokens=Sum('total_input_tokens'),
            total_output_tokens=Sum('total_output_tokens'),
            total_tokens=Sum('total_tokens'),
            total_estimated_cost=Sum('total_estimated_cost'),
            architect_total_tokens=Sum('architect_total_tokens'),
            architect_total_cost=Sum('architect_estimated_cost'),
            developer_total_tokens=Sum('developer_total_tokens'),
            developer_total_cost=Sum('developer_estimated_cost'),
        )

        # Get session info from first token cost record
        first_record = token_costs.first()

        # Update or create session cost record
        session_cost, created = cls.objects.update_or_create(
            session_id=session_id,
            defaults={
                'chat': first_record.chat,
                'user': first_record.user,
                'total_messages': aggregates['total_messages'] or 0,
                'total_input_tokens': aggregates['total_input_tokens'] or 0,
                'total_output_tokens': aggregates['total_output_tokens'] or 0,
                'total_tokens': aggregates['total_tokens'] or 0,
                'total_estimated_cost': aggregates['total_estimated_cost'] or Decimal('0'),
                'architect_total_tokens': aggregates['architect_total_tokens'] or 0,
                'architect_total_cost': aggregates['architect_total_cost'] or Decimal('0'),
                'developer_total_tokens': aggregates['developer_total_tokens'] or 0,
                'developer_total_cost': aggregates['developer_total_cost'] or Decimal('0'),
            }
        )

        return session_cost

    @property
    def average_cost_per_message(self):
        """Calculate average cost per message."""
        if self.total_messages > 0:
            return self.total_estimated_cost / self.total_messages
        return Decimal('0.00000000')

    @property
    def average_tokens_per_message(self):
        """Calculate average tokens per message."""
        if self.total_messages > 0:
            return self.total_tokens / self.total_messages
        return 0

    def __str__(self):
        return f"Session {self.session_id} - {self.total_messages} msgs, ${self.total_estimated_cost}"

    class Meta:
        verbose_name = "Chat Session Cost"
        verbose_name_plural = "Chat Session Costs"
        ordering = ['-last_message_time']
        indexes = [
            models.Index(fields=['session_id']),
            models.Index(fields=['user', 'last_message_time']),
            models.Index(fields=['chat', 'last_message_time']),
            models.Index(fields=['total_estimated_cost']),
            models.Index(fields=['is_active']),
        ]
