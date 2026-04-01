from django.db import models
from django.contrib.auth import get_user_model
from backend.core.models.project_details import ProjectDetails

User = get_user_model()

class UserAIContextRules(models.Model):
    """Model for storing user's personal AI context rules"""
    user = models.OneToOneField(
        User, 
        on_delete=models.CASCADE,
        related_name='ai_context_rules'
    )
    context_rules = models.TextField(default='', blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'core_user_ai_context_rules'
        verbose_name = 'User AI Context Rules'
        verbose_name_plural = 'User AI Context Rules'
    
    def __str__(self):
        return f"AI Context Rules for {self.user.username}"

class ProjectAIContextRules(models.Model):
    """Model for storing project-specific AI context rules (single entry per project)"""
    project = models.OneToOneField(
        ProjectDetails,
        on_delete=models.CASCADE,
        related_name='ai_context_rules'
    )
    context_rules = models.TextField(default='', blank=True)
    created_by = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name='created_project_ai_context_rules'
    )
    updated_by = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name='updated_project_ai_context_rules'
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'core_project_ai_context_rules'
        verbose_name = 'Project AI Context Rules'
        verbose_name_plural = 'Project AI Context Rules'
    
    def __str__(self):
        return f"AI Context Rules for {self.project.project_name}"
