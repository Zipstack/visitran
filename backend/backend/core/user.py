import logging
import uuid
from datetime import timedelta
from typing import Any, Optional

from django.db import IntegrityError
from django.db import transaction
from django.utils.timezone import now

from backend.core.models.api_tokens import APIToken
from backend.core.models.organization_model import Organization
from backend.core.models.user_model import User

Logger = logging.getLogger(__name__)


class UserService:
    def __init__(
        self,
    ) -> None:
        pass

    def create_user(self, email: str, user_id: str, first_name: str, last_name: str) -> User:
        try:
            user: User = User(email=email, user_id=user_id, username=email, first_name=first_name, last_name=last_name)
            user.save()
        except IntegrityError as error:
            Logger.info(f"[Duplicate Id] Failed to create User Error: {error}")
            raise error
        return user

    def get_user_by_email(self, email: str) -> Optional[User]:
        try:
            user: User = User.objects.get(email=email)
            return user
        except User.DoesNotExist:
            return None

    def get_user_by_user_id(self, user_id: str) -> Any:
        try:
            return User.objects.get(user_id=user_id)
        except User.DoesNotExist:
            return None

    def update_user_display_names(self, user: User, first_name: str, last_name: str):
        user.first_name = first_name
        user.last_name = last_name
        user.save()
        return user

    def update_user(self, user: User, user_id: str, first_name: str, last_name: str, profile_picture_url: str) -> User:
        user.user_id = user_id
        user.first_name = first_name
        user.last_name = last_name
        user.profile_picture_url = profile_picture_url
        user.save()
        return user

    def get_user_by_id(self, id: str) -> Any:
        """Retrieve a user by their ID, taking into account the schema context.

        Args:
            id (str): The ID of the user.

        Returns:
            Any: The user object if found, or None if not found.
        """
        try:
            return User.objects.get(id=id)
        except User.DoesNotExist:
            return None

    def get_or_create_valid_token(self, user: User, organization: Organization):
        with transaction.atomic():
            try:
                logging.info(f"Fetching api token for user: {user} and tenant: {organization}")
                token = (
                    APIToken.raw_objects.select_for_update()
                    .filter(user=user, organization=organization)
                    .order_by("-created_at")
                    .first()
                )
                if token and not token.is_valid():
                    logging.info(
                        f"Api token for user: {user} and tenant: {organization} is invalid. generating new token...."
                    )
                    token.delete()
                    token = None
                if token is None:
                    token = APIToken.objects.create(
                        user=user,
                        organization=organization,
                        token=str(uuid.uuid4().hex),
                        expires_at=now() + timedelta(days=90),
                    )
                    logging.info(f"A new api token for user: {user} and tenant: {organization} is created")
            except Exception as e:
                logging.critical(
                    f"failed to create/fetch api token for user {user} and tenant: {organization}, error {e}"
                )
                token = ""
            return token

    def fetch_token(self, **kwargs):
        user = kwargs.get("user")
        api_token = APIToken.objects.filter(user=user).first()
        return api_token
