from django.contrib.auth import get_user_model
from rest_framework import serializers

from backend.core.scheduler.models import TaskRunHistory

User = get_user_model()


class TaskRunHistorySerializer(serializers.ModelSerializer):
    duration = serializers.SerializerMethodField()
    duration_ms = serializers.SerializerMethodField()
    run_number = serializers.SerializerMethodField()
    triggered_by = serializers.SerializerMethodField()
    model_count = serializers.SerializerMethodField()
    failed_models = serializers.SerializerMethodField()
    skipped_count = serializers.SerializerMethodField()

    class Meta:
        model = TaskRunHistory
        fields = "__all__"

    def get_duration(self, obj):
        """Human-readable duration string."""
        if obj.start_time and obj.end_time:
            delta = obj.end_time - obj.start_time
            total_ms = int(delta.total_seconds() * 1000)
            if total_ms < 1000:
                return f"{total_ms}ms"
            elif total_ms < 60000:
                return f"{total_ms / 1000:.1f}s"
            else:
                minutes = total_ms // 60000
                seconds = (total_ms % 60000) / 1000
                return f"{minutes}m {seconds:.0f}s"
        return None

    def get_duration_ms(self, obj):
        """Duration in milliseconds for sorting/comparison."""
        if obj.start_time and obj.end_time:
            return int((obj.end_time - obj.start_time).total_seconds() * 1000)
        return None

    def get_run_number(self, obj):
        """Sequential run number within the job (1 = oldest)."""
        if not hasattr(self, "_run_number_cache"):
            self._run_number_cache = {}
        task_detail_id = obj.user_task_detail_id
        if task_detail_id not in self._run_number_cache:
            # Get all run IDs for this job ordered by start_time ASC
            run_ids = list(
                TaskRunHistory.objects.filter(user_task_detail_id=task_detail_id)
                .order_by("start_time")
                .values_list("id", flat=True)
            )
            self._run_number_cache[task_detail_id] = {
                rid: idx + 1 for idx, rid in enumerate(run_ids)
            }
        return self._run_number_cache[task_detail_id].get(obj.id, 0)

    def get_triggered_by(self, obj):
        """Resolve user_id from kwargs to username."""
        if not obj.kwargs:
            return None
        user_id = obj.kwargs.get("user_id")
        if not user_id:
            return None
        try:
            user = User.objects.get(id=user_id)
            return {
                "id": str(user.id),
                "username": user.get_full_name() or user.username or user.email,
            }
        except (User.DoesNotExist, ValueError):
            return {"id": str(user_id), "username": str(user_id)}

    def get_model_count(self, obj):
        """Total model count from result."""
        if obj.result and isinstance(obj.result, dict):
            return obj.result.get("total", 0)
        return 0

    def get_failed_models(self, obj):
        """List of failed model names."""
        if obj.result and isinstance(obj.result, dict):
            models = obj.result.get("models", [])
            return [m["name"] for m in models if m.get("end_status") == "FAIL" or m.get("status") == "failure"]
        return []

    def get_skipped_count(self, obj):
        """Count of skipped models (total - passed - failed)."""
        if obj.result and isinstance(obj.result, dict):
            total = obj.result.get("total", 0)
            passed = obj.result.get("passed", 0)
            failed = obj.result.get("failed", 0)
            return max(0, total - passed - failed)
        return 0
