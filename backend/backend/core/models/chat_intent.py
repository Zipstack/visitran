from django.db import models
from utils.models.base_model import BaseModel


class ChatIntent(BaseModel):
    """
    Represents a fixed set of intents used in the chat system,
    such as info queries, content generation, SQL tasks, etc.
    """

    NAME_CHOICES = [
        ('INFO', 'INFO'),
        ('TRANSFORM', 'TRANSFORM'),
        ('SQL', 'SQL'),
    ]

    DISPLAY_NAME_CHOICES = [
        ('Chat', 'Chat'),
        ('Transform', 'Transform'),
        ('SQL', 'SQL'),
    ]

    chat_intent_id = models.UUIDField(
        primary_key=True,
        editable=False,
        help_text="Unique identifier for this ChatIntent."
    )

    name = models.CharField(
        max_length=20,
        choices=NAME_CHOICES,
        editable=False,
        null=False,
        blank=False,
        unique=True,
        help_text="Internal name of the intent. Must be one of INFO, GENERATE, SQL, NOTA, AUTO."
    )

    display_name = models.CharField(
        max_length=20,
        choices=DISPLAY_NAME_CHOICES,
        editable=False,
        null=False,
        blank=False,
        unique=True,
        help_text="User-facing display name for the intent."
    )

    objects = models.Manager()

    def __str__(self) -> str:
        """
        Descriptive name combining internal name and display name.
        """
        return f"{self.name} ({self.display_name})"

    class Meta:
        verbose_name = "Chat Intent"
        verbose_name_plural = "Chat Intents"
        ordering = ['name']
