import uuid
from django.db import models

from backend.core.models.user_model import User
from backend.core.models.project_details import ProjectDetails
from utils.models.base_model import BaseModel
from backend.core.models.chat_intent import ChatIntent


class ChatManager(models.Manager):
    """Default manager that excludes soft-deleted chats (is_deleted=True)."""

    def get_queryset(self):
        return super().get_queryset().filter(is_deleted=False)


class Chat(BaseModel):
    """Represents a chat session for a given project, with optional soft
    deletion.

    Soft Delete:
      - delete(hard_delete=False) sets is_deleted=True but keeps the record in DB (hidden by ChatManager).
      - delete(hard_delete=True) removes the record physically from the DB.
    """

    chat_id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False
    )
    chat_name = models.CharField(
        max_length=255,
        help_text="Human-readable name for the chat."
    )
    is_deleted = models.BooleanField(
        default=False,
        help_text="Flag for soft deletion. True means the chat is hidden."
    )
    project = models.ForeignKey(
        ProjectDetails,
        on_delete=models.CASCADE,
        null=True,
        related_name="chat_project",
        help_text="Project to which this chat belongs."
    )
    user = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        null=True,
        related_name="chats_created",
        help_text="User who created (owns) this chat."
    )
    chat_intent = models.ForeignKey(
        ChatIntent,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        editable=True,
        related_name="chats",
        help_text="Optional intent associated with this chat."
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

    # Custom Managers
    objects = ChatManager()         # Hides soft-deleted chats
    all_objects = models.Manager()  # Returns all chats (including soft-deleted)

    def delete(self, hard_delete: bool = False, *args, **kwargs) -> None:
        """Soft or hard delete this Chat.

        Args:
            hard_delete (bool): If True, permanently remove this Chat from DB.
                                If False, set is_deleted=True for soft deletion.
        """
        if hard_delete:
            super().delete(*args, **kwargs)
        else:
            self.is_deleted = True
            self.save()

    def restore(self) -> None:
        """Restore this Chat if it was soft-deleted (is_deleted=True)."""
        self.is_deleted = False
        self.save()

    def __str__(self) -> str:
        return f"{self.chat_name} ({'Deleted' if self.is_deleted else 'Active'})"

    class Meta:
        ordering = ["-created_at"]
        verbose_name = "Chat"
        verbose_name_plural = "Chats"
