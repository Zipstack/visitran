from rest_framework import serializers


class CommitProjectSerializer(serializers.Serializer):
    commit_message = serializers.CharField(max_length=500, required=False, default="")


class RollbackSerializer(serializers.Serializer):
    version_number = serializers.IntegerField(required=True)
    reason = serializers.CharField(max_length=500, required=False, default="")


class RetryGitSyncSerializer(serializers.Serializer):
    version_id = serializers.UUIDField(required=True)


class CommitFromDraftSerializer(serializers.Serializer):
    commit_message = serializers.CharField(max_length=500, required=False, default="")
    lock_token = serializers.CharField(required=False, allow_blank=True, default="")


class ResolveConflictSerializer(serializers.Serializer):
    conflict_id = serializers.UUIDField(required=True)
    strategy = serializers.ChoiceField(
        choices=["accepted", "rejected", "merged"], required=True,
    )
    resolved_data = serializers.DictField(required=False, default=dict)


class FinalizeResolutionsSerializer(serializers.Serializer):
    commit_message = serializers.CharField(max_length=500, required=False, default="")
