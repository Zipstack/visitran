import logging
from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.request import Request
from rest_framework.response import Response
from django.shortcuts import get_object_or_404

from backend.core.utils import handle_http_request
from backend.utils.constants import HTTPMethods
from backend.core.models.ai_context_rules import UserAIContextRules, ProjectAIContextRules
from backend.core.models.project_details import ProjectDetails
from backend.errors.error_codes import BackendErrorMessages, BackendSuccessMessages
from rbac.factory import handle_permission

logger = logging.getLogger(__name__)

# Personal Context Rules APIs
@api_view([HTTPMethods.GET, HTTPMethods.PUT])
@handle_http_request
@handle_permission
def user_ai_context_rules(request: Request) -> Response:
    """Get or update user's personal AI context rules."""
    try:
        user = request.user

        if request.method == HTTPMethods.GET:
            # Get or create user context rules
            context_rules, created = UserAIContextRules.objects.get_or_create(
                user=user,
                defaults={'context_rules': ''}
            )

            return Response({
                "success": True,
                "data": {
                    "user_id": str(user.user_id),
                    "context_rules": context_rules.context_rules,
                    "created_at": context_rules.created_at.isoformat(),
                    "updated_at": context_rules.updated_at.isoformat()
                }
            }, status=status.HTTP_200_OK)

        elif request.method == HTTPMethods.PUT:
            context_rules_text = request.data.get('context_rules', '')

            # Get or create user context rules
            context_rules, created = UserAIContextRules.objects.get_or_create(
                user=user,
                defaults={'context_rules': context_rules_text}
            )

            if not created:
                context_rules.context_rules = context_rules_text
                context_rules.save()

            return Response({
                "success": True,
                "message": BackendSuccessMessages.AI_CONTEXT_RULES_PERSONAL_UPDATED,
                "data": {
                    "user_id": str(user.user_id),
                    "context_rules": context_rules.context_rules,
                    "updated_at": context_rules.updated_at.isoformat()
                }
            }, status=status.HTTP_200_OK)

    except Exception as e:
        logger.error(f"Error with user AI context rules: {str(e)}")
        return Response({
            "error_message": BackendErrorMessages.AI_CONTEXT_RULES_FETCH_FAILED,
            "is_markdown": True,
            "severity": "error"
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# Project Context Rules APIs
@api_view([HTTPMethods.GET, HTTPMethods.PUT])
@handle_http_request
@handle_permission
def project_ai_context_rules(request: Request, project_id: str) -> Response:
    """Get or update project-specific AI context rules (shared by all users)"""
    try:
        try:
            project = ProjectDetails.objects.get(project_uuid=project_id)
        except ProjectDetails.DoesNotExist:
            return Response({
                "error_message": BackendErrorMessages.AI_CONTEXT_RULES_INVALID_PROJECT.format(project_id=project_id),
                "is_markdown": True,
                "severity": "error"
            }, status=status.HTTP_404_NOT_FOUND)

        if request.method == HTTPMethods.GET:
            # Get project context rules (single entry per project)
            try:
                context_rules = ProjectAIContextRules.objects.get(project=project)

                return Response({
                    "success": True,
                    "data": {
                        "project_uuid": str(project.project_uuid),
                        "project_name": project.project_name,
                        "context_rules": context_rules.context_rules,
                        "created_by": {
                            "user_id": str(context_rules.created_by.user_id),
                            "username": context_rules.created_by.username,
                            "full_name": f"{context_rules.created_by.first_name} {context_rules.created_by.last_name}".strip()
                        },
                        "updated_by": {
                            "user_id": str(context_rules.updated_by.user_id),
                            "username": context_rules.updated_by.username,
                            "full_name": f"{context_rules.updated_by.first_name} {context_rules.updated_by.last_name}".strip()
                        },
                        "created_at": context_rules.created_at.isoformat(),
                        "updated_at": context_rules.updated_at.isoformat()
                    }
                }, status=status.HTTP_200_OK)

            except ProjectAIContextRules.DoesNotExist:
                # Return empty context rules if none exist yet
                return Response({
                    "success": True,
                    "data": {
                        "project_uuid": str(project.project_uuid),
                        "project_name": project.project_name,
                        "context_rules": "",
                        "created_by": None,
                        "updated_by": None,
                        "created_at": None,
                        "updated_at": None
                    }
                }, status=status.HTTP_200_OK)

        elif request.method == HTTPMethods.PUT:
            user = request.user
            context_rules_text = request.data.get('context_rules', '')

            # Get or create project context rules (single entry per project)
            context_rules, created = ProjectAIContextRules.objects.get_or_create(
                project=project,
                defaults={
                    'context_rules': context_rules_text,
                    'created_by': user,
                    'updated_by': user
                }
            )

            if not created:
                context_rules.context_rules = context_rules_text
                context_rules.updated_by = user  # Track who updated
                context_rules.save()

            return Response({
                "success": True,
                "message": BackendSuccessMessages.AI_CONTEXT_RULES_PROJECT_UPDATED,
                "data": {
                    "project_uuid": str(project.project_uuid),
                    "context_rules": context_rules.context_rules,
                    "updated_by": {
                        "user_id": str(user.user_id),
                        "username": user.username,
                        "full_name": f"{user.first_name} {user.last_name}".strip()
                    },
                    "updated_at": context_rules.updated_at.isoformat()
                }
            }, status=status.HTTP_200_OK)

    except Exception as e:
        logger.error(f"Error with project AI context rules: {str(e)}")
        return Response({
            "error_message": BackendErrorMessages.AI_CONTEXT_RULES_FETCH_FAILED,
            "is_markdown": True,
            "severity": "error"
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
