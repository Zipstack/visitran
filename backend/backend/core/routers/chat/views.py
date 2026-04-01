import uuid
import logging

from pydantic import UUID1
from rest_framework import status, viewsets
from rest_framework.request import Request
from rest_framework.response import Response

from backend.application.context.chat_message_context import ChatMessageContext
from backend.core.models.project_details import ProjectDetails
from backend.core.routers.chat.serializers import ChatSerializer
from backend.core.routers.chat_message.serializers import ChatMessageSerializer
from backend.core.mixins.http_request_handler import RequestHandlingMixin
from backend.errors.chat_exceptions import InsufficientTokenBalance
from backend.utils.calculate_chat_tokens import calculate_chat_tokens


class ChatView(RequestHandlingMixin, viewsets.ViewSet):
    """
    Custom ViewSet handling chat listing (with optional chat_id) and deletion
    through ChatMessageContext.
    """

    def list_chats(self, request, project_id=None, *args, **kwargs):
        """
        If 'chat_id' is provided in query params (?chat_id=xxx), return that specific chat.
        Otherwise, return all chats belonging to the given project_id.
        """
        chat_id = request.query_params.get("chat_id")

        chat_ctx = ChatMessageContext(project_id=project_id)

        if chat_id:
            # Return a specific chat by chat_id
            single_chat = chat_ctx.get_single_chat(chat_id=chat_id)
            serializer = ChatSerializer(single_chat)
            return Response(serializer.data, status=status.HTTP_200_OK)
        else:
            # Return all chats for project_id
            chats = chat_ctx.get_all_chats()
            serializer = ChatSerializer(chats, many=True)
            return Response(serializer.data, status=status.HTTP_200_OK)

    def delete_chat(self, request, project_id=None, chat_id=None, *args, **kwargs):
        """
        Soft-delete the chat specified by chat_id, if the current user is the owner.
        """
        chat_ctx = ChatMessageContext(project_id=project_id)
        chat_ctx.delete_chat(chat_id=chat_id)
        return Response(status=status.HTTP_204_NO_CONTENT)

    def update_chat_name(self, request, project_id=None, chat_id=None, *args, **kwargs):
        """
        Update the chat name for the specified chat_id.
        """
        chat_ctx = ChatMessageContext(project_id=project_id)
        new_name = request.data.get('chat_name')

        if not new_name or not new_name.strip():
            return Response(
                {"error": "chat_name is required and cannot be empty"},
                status=status.HTTP_400_BAD_REQUEST
            )

        chat = chat_ctx.update_chat_name(chat_id=chat_id, chat_name=new_name.strip())
        serializer = ChatSerializer(chat)
        return Response(serializer.data, status=status.HTTP_200_OK)

    def fetch_token_balance(
            self,
            llm_model_architect,
            llm_model_developer,
            organization,
            chat_intent_name
    ) -> None:
        try:
            from pluggable_apps.subscriptions.services.token_service import TokenBalanceService

            # Calculate required tokens for this operation
            llm_model = llm_model_architect or llm_model_developer or "anthropic/claude-4-sonnet"
            # Check if organization has sufficient token balance
            tokens_required = calculate_chat_tokens(
                llm_model=llm_model,
                chat_intent=chat_intent_name
            )
            has_sufficient_tokens = TokenBalanceService.check_token_availability(
                organization=organization,
                tokens_needed=tokens_required
            )

            balance_info = TokenBalanceService.get_balance_info(organization)

            if not has_sufficient_tokens:
                current_balance = balance_info.get('current_balance', 0)

                logging.warning(
                    f"Insufficient tokens for organization {organization.organization_id}. "
                    f"Required: {tokens_required}, Available: {current_balance}"
                )

                raise InsufficientTokenBalance(
                    tokens_required=tokens_required,
                    tokens_available=current_balance
                )

            logging.info(
                f"Token balance check passed for organization {organization.organization_id}. "
                f"Required: {tokens_required}, Available: {balance_info.get('current_balance', 0)}"
            )
        except ImportError:
            # OSS mode: pluggable_apps not installed, skip billing check
            pass

    def persist_prompt(self, request: Request, project_id: str, *args, **kwargs) -> Response:
        """
        Create a new prompt (ChatMessage) using data from the request body.

        Expects JSON with keys:
          - "project_id"
          - "chat_id" (optional)
          - "prompt"
        Returns:
          - chat_message_id (UUID)
        """
        from backend.utils.calculate_chat_tokens import calculate_chat_tokens
        from backend.errors.chat_exceptions import InsufficientTokenBalance

        data = request.data
        chat_id = data.get("chat_id")
        prompt = data.get("prompt")
        chat_intent_id = data.get("chat_intent_id")
        llm_model_architect = data.get("llm_model_architect")
        llm_model_developer = data.get("llm_model_developer")
        discussion_type = data.get('discussion_status')
        generated_chat_res_id = uuid.uuid1(1)

        if discussion_type is None:
            discussion_type = 'INPROGRESS'
        if discussion_type == "GENERATE":
            generated_chat_res_id = data.get('final_discussion_id')

        # Check token balance before processing the request
        try:
            project = ProjectDetails.objects.get(project_uuid=project_id)
            organization = project.organization

            # Determine chat intent name for token calculation
            chat_intent_name = "INFO"  # Default
            if chat_intent_id:
                from backend.core.models.chat_intent import ChatIntent
                try:
                    chat_intent = ChatIntent.objects.get(chat_intent_id=chat_intent_id)
                    chat_intent_name = chat_intent.name
                except ChatIntent.DoesNotExist:
                    pass

            self.fetch_token_balance(
                llm_model_architect=llm_model_architect,
                llm_model_developer=llm_model_developer,
                organization=organization,
                chat_intent_name=chat_intent_name
            )

        except ProjectDetails.DoesNotExist:
            logging.error(f"Project {project_id} not found")
            return Response(
                data={"error": "Project not found"},
                status=status.HTTP_404_NOT_FOUND
            )

        chat_message_context = ChatMessageContext(project_id=project_id)
        chat_message = chat_message_context.persist_prompt(
            prompt=prompt,
            chat_id=chat_id,
            chat_intent_id=chat_intent_id,
            llm_model_architect=llm_model_architect,
            llm_model_developer=llm_model_developer,
            discussion_type=discussion_type,
            generated_chat_res_id=generated_chat_res_id,
            user=request.user,
        )
        serializer = ChatMessageSerializer(chat_message)
        return Response(data=serializer.data, status=status.HTTP_200_OK)

    def list_llm_models(self, request: Request, project_id: str, *args, **kwargs) -> Response:
        """
        Retrieve the list of available LLM models configured for use in the system.

        This reads the model definitions from an internal constant variable and returns
        them as a JSON response. Intended for populating model selection options in the UI.

        Returns:
            200 OK: List of LLM models.
        """
        chat_ctx = ChatMessageContext(project_id=project_id)
        llm_models = chat_ctx.get_llm_models()
        return Response(data=llm_models, status=status.HTTP_200_OK)
