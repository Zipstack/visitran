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
        fields = [
            "id", "task_id", "status", "start_time", "end_time",
            "trigger", "scope", "error_message", "result", "retry_num",
            "user_task_detail",
            "duration", "duration_ms", "run_number", "triggered_by",
            "model_count", "failed_models", "skipped_count",
        ]

    def _get_user_cache(self):
        """Batch-load users for all runs in one query, cached per serializer instance."""
        if not hasattr(self, "_user_cache"):
            user_ids = set()
            for obj in self.instance if hasattr(self.instance, '__iter__') else [self.instance]:
                if obj and isinstance(obj.kwargs, dict) and obj.kwargs.get("user_id"):
                    user_ids.add(obj.kwargs["user_id"])
            if user_ids:
                self._user_cache = {
                    str(u.id): u for u in User.objects.filter(id__in=user_ids)
                }
            else:
                self._user_cache = {}
        return self._user_cache

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
        """Sequential run number from view context (total - offset - idx)."""
        run_numbers = self.context.get("run_numbers", {})
        return run_numbers.get(obj.id, 0)

    def get_triggered_by(self, obj):
        """Resolve user_id from kwargs to username using batch-loaded cache."""
        if not isinstance(obj.kwargs, dict):
            return None
        user_id = obj.kwargs.get("user_id")
        if not user_id:
            return None
        cache = self._get_user_cache()
        user = cache.get(str(user_id))
        if user:
            return {
                "id": str(user.id),
                "username": user.get_full_name() or user.username or user.email,
            }
        return {"id": str(user_id), "username": "Unknown user"}

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
