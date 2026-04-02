import uuid
from django.db import models
from django.contrib.postgres.fields import ArrayField

from backend.core.routers.chat_message.constants import ChatMessageStatus
from backend.core.models.user_model import User
from utils.models.base_model import BaseModel
from backend.core.models.chat import Chat
from backend.core.models.chat_intent import ChatIntent


STATUS_CHOICES = [
    (ChatMessageStatus.YET_TO_START, ChatMessageStatus.YET_TO_START),
    (ChatMessageStatus.RUNNING, ChatMessageStatus.RUNNING),
    (ChatMessageStatus.SUCCESS, ChatMessageStatus.SUCCESS),
    (ChatMessageStatus.FAILED, ChatMessageStatus.FAILED),
]

TRANSFORMATION_TYPE_CHOICES = [
    (ChatMessageStatus.DISCUSSION, ChatMessageStatus.DISCUSSION),
    (ChatMessageStatus.TRANSFORM, ChatMessageStatus.TRANSFORM)
]

DISCUSSION_TYPE_CHOICES = [
    (ChatMessageStatus.INPROGRESS, ChatMessageStatus.INPROGRESS),
    (ChatMessageStatus.APPROVED, ChatMessageStatus.APPROVED),
    (ChatMessageStatus.GENERATE, ChatMessageStatus.GENERATE),
    (ChatMessageStatus.DISAPPROVED, ChatMessageStatus.DISAPPROVED)
]


class ChatMessageManager(models.Manager):
    """
    Default manager excluding messages from soft-deleted chats.
    Returns only ChatMessages where chat.is_deleted=False.
    """

    def get_queryset(self):
        return super().get_queryset().filter(chat__is_deleted=False)


class ChatMessage(BaseModel):
    """
    Represents a single message in a Chat, with a prompt, optional response,
    and status tracking for prompt and transformation stages.
    """

    chat_message_id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False,
        help_text="Unique identifier for this ChatMessage."
    )

    chat = models.ForeignKey(
        Chat,
        on_delete=models.CASCADE,
        related_name="messages",
        help_text="Parent Chat to which this message belongs.",
        null=False,
        blank=False
    )

    user = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        null=True,
        related_name="chat_messages",
        help_text="User who created this message."
    )

    chat_intent = models.ForeignKey(
        ChatIntent,
        on_delete=models.SET_NULL,
        editable=False,
        null=True,
        blank=True,
        related_name="chat_messages",
        help_text="Optional intent associated with this message. Cannot be edited."
    )

    prompt = models.CharField(
        max_length=65000,
        help_text="Text input or question from the user.",
        null=False,
        blank=False
    )

    response = models.JSONField(
        null=True,
        blank=True,
        help_text="JSON response generated for this message."
    )

    technical_content = models.TextField(
        null=True,
        blank=True,
        help_text="Optional field to store technical data (e.g. YAML for 'Transform' or SQL query for 'SQL'). Empty for 'Info' intent."
    )

    response_time = models.PositiveIntegerField(
        help_text="Time taken in milliseconds to generate the response.",
        null=False,
        blank=False,
        default=0
    )

    thought_chain = models.JSONField(
        default=list,
        null=True,
        blank=True,
        help_text="List of thought processes before generating the response."
    )

    prompt_status = models.CharField(
        max_length=20,
        choices=STATUS_CHOICES,
        default=ChatMessageStatus.YET_TO_START,
        help_text="Status of prompt processing."
    )

    prompt_error_message = models.JSONField(
        null=True,
        blank=True,
        help_text="Error message for prompt processing."
    )

    transformation_type = models.CharField(
        max_length=50,
        choices=TRANSFORMATION_TYPE_CHOICES,
        default=ChatMessageStatus.DISCUSSION,
        help_text="Type of transformation stage. Could be 'Discussion' or 'Transform'."
    )

    discussion_type = models.CharField(
        max_length=50,
        choices=DISCUSSION_TYPE_CHOICES,
        default=ChatMessageStatus.INPROGRESS,
        help_text="Marks stages within discussion flow."
    )

    last_discussion_id = models.UUIDField(
        primary_key=False,
        default=uuid.uuid4,
        editable=True,
        help_text="Unique identifier for final approved discussion ChatMessage ID."
    )

    transformation_status = models.CharField(
        max_length=20,
        choices=STATUS_CHOICES,
        default=ChatMessageStatus.YET_TO_START,
        help_text="Status of transformation processing."
    )

    generated_models = models.JSONField(
        default=list,
        null=True,
        blank=True,
        help_text="List of generated models after transformation Applied."
    )

    transformation_error_message = models.JSONField(
        null=True,
        blank=True,
        help_text="Error message for transformation processing."
    )

    llm_model_architect = models.CharField(
        max_length=255,
        default="anthropic/claude-3-7-sonnet",
        null=False,
        blank=False,
        editable=True,
        help_text="String identifier of the architect LLM model used for this chat."
    )

    llm_model_developer = models.CharField(
        max_length=255,
        default="anthropic/claude-3-7-sonnet",
        null=False,
        blank=False,
        editable=True,
        help_text="String identifier of the developer LLM model used for this chat."
    )

    # Feedback fields for response quality
    has_feedback = models.BooleanField(
        default=False,
        help_text="Indicates whether this message has received user feedback."
    )

    FEEDBACK_CHOICES = [
        ('0', 'Neutral'),
        ('P', 'Positive'),
        ('N', 'Negative')
    ]

    feedback = models.CharField(
        max_length=1,
        choices=FEEDBACK_CHOICES,
        default='0',
        help_text="Feedback value: 0=Neutral, P=Positive, N=Negative"
    )

    feedback_timestamp = models.DateTimeField(
        null=True,
        blank=True,
        help_text="When the feedback was provided."
    )

    feedback_comment = models.TextField(
        null=True,
        blank=True,
        help_text="Optional comment provided with feedback."
    )

    objects = ChatMessageManager()   # Excludes messages of soft-deleted chats
    all_objects = models.Manager()   # Includes all messages

    @property
    def chat_name(self):
        return self.chat.chat_name

    def __str__(self) -> str:
        """
        Descriptive name showing the message UUID and the parent Chat UUID (or 'None' if missing).
        """
        chat_id = self.chat.chat_id if self.chat else 'None'
        return f"Message {self.chat_message_id} for Chat {chat_id}"

    class Meta:
        ordering = ['-created_at']
        verbose_name = "Chat Message"
        verbose_name_plural = "Chat Messages"
