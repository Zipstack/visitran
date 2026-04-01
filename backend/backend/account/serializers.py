"""Serializers for account module - signup, login, session handling."""

from django.contrib.auth import get_user_model
from django.contrib.auth.password_validation import validate_password
from django.core.exceptions import ValidationError
from rest_framework import serializers

User = get_user_model()


class SignupSerializer(serializers.Serializer):
    """Serializer for user signup with validation."""

    email = serializers.EmailField(required=True)
    password = serializers.CharField(
        required=True,
        write_only=True,
        min_length=8,
        style={"input_type": "password"},
    )
    confirm_password = serializers.CharField(
        required=True,
        write_only=True,
        style={"input_type": "password"},
    )
    display_name = serializers.CharField(required=False, max_length=64, allow_blank=True)

    def validate_email(self, value: str) -> str:
        """Validate email is not already registered."""
        email = value.lower().strip()
        if User.objects.filter(email=email).exists():
            raise serializers.ValidationError("A user with this email already exists.")
        return email

    def validate_password(self, value: str) -> str:
        """Validate password strength using Django's validators."""
        try:
            validate_password(value)
        except ValidationError as e:
            raise serializers.ValidationError(list(e.messages))
        return value

    def validate(self, attrs: dict) -> dict:
        """Validate password confirmation matches."""
        if attrs.get("password") != attrs.get("confirm_password"):
            raise serializers.ValidationError({"confirm_password": "Passwords do not match."})
        return attrs


class LoginSerializer(serializers.Serializer):
    """Serializer for user login."""

    email = serializers.EmailField(required=True)
    password = serializers.CharField(
        required=True,
        write_only=True,
        style={"input_type": "password"},
    )

    def validate_email(self, value: str) -> str:
        """Normalize email to lowercase."""
        return value.lower().strip()


class UserInfoSerializer(serializers.Serializer):
    """Serializer for user information in session."""

    id = serializers.IntegerField()
    user_id = serializers.CharField()
    name = serializers.CharField()
    display_name = serializers.CharField()
    email = serializers.EmailField()


class OrganizationSerializer(serializers.Serializer):
    """Serializer for organization data."""

    id = serializers.CharField()
    name = serializers.CharField()
    display_name = serializers.CharField()
    organization_id = serializers.CharField()


class SessionInfoSerializer(serializers.Serializer):
    """Serializer for session information response."""

    id = serializers.IntegerField()
    user_id = serializers.CharField()
    email = serializers.EmailField()
    user = UserInfoSerializer()
    organization_id = serializers.CharField(allow_null=True)
    user_role = serializers.CharField(allow_blank=True, default="")
    is_org_admin = serializers.BooleanField(default=False)


class SignupResponseSerializer(serializers.Serializer):
    """Serializer for signup response."""

    message = serializers.CharField()
    user = UserInfoSerializer()
    organization = OrganizationSerializer()


class ForgotPasswordSerializer(serializers.Serializer):
    """Serializer for forgot password request."""

    email = serializers.EmailField(required=True)

    def validate_email(self, value):
        return value.lower().strip()


class ResetPasswordSerializer(serializers.Serializer):
    """Serializer for password reset with token validation."""

    uid = serializers.CharField(required=True)
    token = serializers.CharField(required=True)
    password = serializers.CharField(required=True, write_only=True, min_length=8)
    confirm_password = serializers.CharField(required=True, write_only=True)

    def validate(self, data):
        if data["password"] != data["confirm_password"]:
            raise serializers.ValidationError({"confirm_password": "Passwords do not match."})
        # Validate password strength
        try:
            validate_password(data["password"])
        except ValidationError as e:
            raise serializers.ValidationError({"password": list(e.messages)})
        return data
