from rest_framework import serializers

from backend.core.scheduler.models import TaskRunHistory


class TaskRunHistorySerializer(serializers.ModelSerializer):
    duration = serializers.SerializerMethodField()

    class Meta:
        model = TaskRunHistory
        fields = "__all__"  # Include all fields or specify fields like ['id', 'start_time', 'end_time', 'status']

    def get_duration(self, obj):
        """Calculate duration (end_time - start_time)"""
        if obj.start_time and obj.end_time:
            return str(obj.end_time - obj.start_time)  # Convert timedelta to string
        return None  # If end_time is missing, return None
