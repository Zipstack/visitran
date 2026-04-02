"""Additional models for watermark tracking."""

from django.db import models
from utils.models.base_model import BaseModel
from utils.models.organization_mixin import DefaultOrganizationMixin, DefaultOrganizationManagerMixin


class WatermarkHistoryManager(DefaultOrganizationManagerMixin, models.Manager):
    pass


class WatermarkHistory(DefaultOrganizationMixin, BaseModel):
    """Track watermark execution history for incremental processing."""

    user_task = models.ForeignKey(
        'UserTaskDetails',
        on_delete=models.CASCADE,
        related_name='watermark_history',
        help_text='Reference to the job that executed this watermark'
    )

    watermark_value = models.TextField(
        help_text='Watermark value at time of execution'
    )

    execution_time = models.DateTimeField(
        help_text='When this watermark was recorded'
    )

    records_processed = models.IntegerField(
        default=0,
        help_text='Number of records processed in this run'
    )

    execution_duration_seconds = models.FloatField(
        blank=True,
        null=True,
        help_text='Time taken for this incremental run'
    )

    strategy_used = models.CharField(
        max_length=20,
        help_text='Watermark strategy used for this execution'
    )

    metadata = models.JSONField(
        blank=True,
        default=dict,
        help_text='Additional execution metadata'
    )

    objects = WatermarkHistoryManager()
    raw_objects = models.Manager()

    class Meta:
        app_label = "job_scheduler"
        db_table = "job_scheduler_watermarkhistory"
        verbose_name = 'Watermark History'
        verbose_name_plural = 'Watermark Histories'
        ordering = ['-execution_time']
        indexes = [
            models.Index(fields=['user_task', 'execution_time'], name='wm_hist_task_time_idx'),
            models.Index(fields=['organization_id', 'execution_time'], name='wm_hist_org_time_idx'),
        ]

    def __str__(self):
        return f"WatermarkHistory(task={self.user_task.task_name}, value={self.watermark_value[:50]}...)"
