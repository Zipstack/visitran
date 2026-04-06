from django.db import models
from django.utils import timezone

from backend.core.models.project_details import ProjectDetails
from backend.core.models.user_model import User
from utils.models.base_model import BaseModel
from utils.models.organization_mixin import (
    DefaultOrganizationMixin,
    DefaultOrganizationManagerMixin,
)


class OnboardingTemplateManager(models.Manager):
    pass


class OnboardingTemplate(BaseModel):
    """Master templates for different onboarding flows - Global templates"""
    template_id = models.CharField(max_length=50, unique=True)
    title = models.CharField(max_length=200)
    description = models.TextField()
    welcome_message = models.TextField()
    template_data = models.JSONField()  # Your JSON structure
    is_active = models.BooleanField(default=True)

    # Manager
    objects = OnboardingTemplateManager()

    def __str__(self) -> str:
        return f"{self.template_id}: {self.title}"

    class Meta:
        db_table = 'core_onboarding_template'


class ProjectOnboardingSessionManager(DefaultOrganizationManagerMixin, models.Manager):
    pass


class ProjectOnboardingSession(DefaultOrganizationMixin, BaseModel):
    """Active onboarding session for a project"""
    project = models.ForeignKey(
        ProjectDetails,
        on_delete=models.CASCADE,
        related_name="onboarding_sessions"
    )
    user = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name="onboarding_sessions"
    )
    template = models.ForeignKey(
        OnboardingTemplate,
        on_delete=models.CASCADE,
        related_name="sessions"
    )

    # Progress tracking (no more sequential ordering)
    completed_tasks = models.JSONField(default=list)  # List of task IDs
    skipped_tasks = models.JSONField(default=list)    # List of task IDs

    # Session state
    is_active = models.BooleanField(default=True)
    is_completed = models.BooleanField(default=False)
    started_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    # Manager
    objects = ProjectOnboardingSessionManager()

    def __str__(self) -> str:
        return f"Onboarding: {self.project.project_name} - {self.user.email}"

    class Meta:
        db_table = 'core_project_onboarding_session'
        unique_together = ('project', 'user')

    @property
    def progress_percentage(self) -> float:
        """Calculate completion percentage based on completed + skipped tasks"""
        # Get template to calculate total tasks
        try:
            items = self.template.template_data.get('items', [])
        except:
            return 0.0

        total_tasks = len(items)
        if total_tasks == 0:
            return 0.0

        completed_count = len(self.completed_tasks)
        skipped_count = len(self.skipped_tasks)
        total_progress = completed_count + skipped_count

        return round((total_progress / total_tasks) * 100, 2)

    def reset_session(self):
        """Reset the onboarding session"""
        self.completed_tasks = []
        self.skipped_tasks = []
        self.is_completed = False
        self.completed_at = None
        self.is_active = True
        self.save()
