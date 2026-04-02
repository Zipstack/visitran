import logging
from typing import Dict, List, Optional

from django.utils import timezone
from rest_framework import status, viewsets
from rest_framework.decorators import action
from rest_framework.request import Request
from rest_framework.response import Response

from backend.core.models.onboarding import OnboardingTemplate, ProjectOnboardingSession
from backend.core.models.project_details import ProjectDetails
from backend.core.models.user_model import User
from backend.utils.tenant_context import get_current_user
from backend.core.mixins.http_request_handler import RequestHandlingMixin

logger = logging.getLogger(__name__)


class OnboardingViewSet(RequestHandlingMixin, viewsets.ViewSet):
    """
    ViewSet for managing onboarding templates and project onboarding sessions
    """

    @action(detail=False, methods=["GET"])
    def get_onboarding_template(self, request: Request, template_id: str) -> Response:
        """Get onboarding template by ID - Global templates"""
        try:
            template = OnboardingTemplate.objects.get(
                template_id=template_id,
                is_active=True
            )
            return Response({
                'template_id': template.template_id,
                'title': template.title,
                'description': template.description,
                'welcome_message': template.welcome_message,
                'items': template.template_data.get('items', [])
            })
        except OnboardingTemplate.DoesNotExist:
            return Response({'error': 'Template not found'}, status=status.HTTP_404_NOT_FOUND)

    @action(detail=False, methods=["GET"])
    def get_project_onboarding_status(self, request: Request, project_id: str) -> Response:
        """Get project onboarding status with auto-initialization and all tasks status"""
        try:
            # Get project details
            try:
                project = ProjectDetails.objects.get(project_uuid=project_id)
            except ProjectDetails.DoesNotExist:
                return Response({'error': 'Project not found'}, status=status.HTTP_404_NOT_FOUND)

            # Check if onboarding is enabled for this project
            if not project.onboarding_enabled:
                return Response({
                    "onboarding_enabled": False,
                    "onboarding_active": False,
                    "message": "Onboarding is not enabled for this project"
                })

            # Get user object from context
            try:
                user = self._get_user_from_context()
            except ValueError as e:
                return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)

            # Get or create onboarding session
            onboarding_session, created = ProjectOnboardingSession.objects.get_or_create(
                project=project,
                user=user,
                defaults={
                    "template": self._get_template_for_project(project),
                    "completed_tasks": [],
                    "skipped_tasks": [],
                }
            )

            if created:
                logger.info(f"Created new onboarding session for project {project_id} and user {user.email}")

            # Get template details
            template = onboarding_session.template
            if not template:
                return Response({'error': 'Onboarding template not found'}, status=status.HTTP_404_NOT_FOUND)

            # Build tasks with status
            tasks = self._build_tasks_with_status(template, onboarding_session)

            # Calculate progress
            total_tasks = len(template.template_data.get('items', []))
            completed_count = len(onboarding_session.completed_tasks)
            skipped_count = len(onboarding_session.skipped_tasks)
            progress_percentage = int((completed_count + skipped_count) / total_tasks * 100) if total_tasks > 0 else 0

            # Check if onboarding is completed (only check session status, not progress)
            is_completed = onboarding_session.is_completed

            response_data = {
                "onboarding_enabled": True,
                "onboarding_active": True,
                "is_completed": is_completed,
                "template": {
                    "template_id": template.template_id,
                    "title": template.title,
                    "description": template.description,
                    "welcome_message": template.welcome_message,
                },
                "tasks": tasks,
                "progress": {
                    "total_tasks": total_tasks,
                    "completed_tasks": completed_count,
                    "skipped_tasks": skipped_count,
                    "progress_percentage": progress_percentage
                }
            }

            return Response(response_data)

        except Exception as e:
            logger.error(f"Error getting project onboarding status: {str(e)}", exc_info=True)
            return Response({'error': 'Internal server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @action(detail=False, methods=["POST"])
    def start_onboarding(self, request: Request, project_id: str) -> Response:
        """Start onboarding for a project"""
        try:

            # Get project details
            try:
                project = ProjectDetails.objects.get(project_uuid=project_id)
            except ProjectDetails.DoesNotExist:
                return Response({'error': 'Project not found'}, status=status.HTTP_404_NOT_FOUND)

            # Check if onboarding is enabled
            if not project.onboarding_enabled:
                return Response({'error': 'Onboarding is not enabled for this project'}, status=status.HTTP_400_BAD_REQUEST)

            # Get user object from context
            try:
                user = self._get_user_from_context()
            except ValueError as e:
                return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)

            # Create or update onboarding session
            onboarding_session, created = ProjectOnboardingSession.objects.get_or_create(
                project=project,
                user=user,
                defaults={
                    "template": self._get_template_for_project(project),
                    "completed_tasks": [],
                    "skipped_tasks": [],
                }
            )

            if not created:
                # Reset existing session
                onboarding_session.completed_tasks = []
                onboarding_session.skipped_tasks = []
                onboarding_session.save()

            # Get template and build response
            template = onboarding_session.template
            tasks = self._build_tasks_with_status(template, onboarding_session)

            response_data = {
                "onboarding_enabled": True,
                "onboarding_active": True,
                "template": {
                    "template_id": template.template_id,
                    "title": template.title,
                    "description": template.description,
                    "welcome_message": template.welcome_message,
                },
                "tasks": tasks,
                "progress": {
                    "total_tasks": len(template.template_data.get('items', [])),
                    "completed_tasks": 0,
                    "skipped_tasks": 0,
                    "progress_percentage": 0
                }
            }

            return Response(response_data)

        except Exception as e:
            logger.error(f"Error starting onboarding: {str(e)}", exc_info=True)
            return Response({'error': 'Internal server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @action(detail=False, methods=["POST"])
    def complete_task(self, request: Request, project_id: str) -> Response:
        """Complete a specific task (random access allowed)"""
        try:

            task_id = request.data.get("task_id")
            if not task_id:
                return Response({'error': 'Task ID is required'}, status=status.HTTP_400_BAD_REQUEST)

            # Get project and user
            project = ProjectDetails.objects.get(project_uuid=project_id)

            try:
                user = self._get_user_from_context()
            except ValueError as e:
                return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)

            try:
                onboarding_session = ProjectOnboardingSession.objects.get(
                    project=project,
                    user=user
                )
            except ProjectOnboardingSession.DoesNotExist:
                return Response({'error': 'Onboarding session not found'}, status=status.HTTP_404_NOT_FOUND)

            # Get template to validate task exists
            template = onboarding_session.template
            if not template:
                return Response({'error': 'Onboarding template not found'}, status=status.HTTP_404_NOT_FOUND)

            # Validate task exists in template
            if not self._task_exists_in_template(template, task_id):
                return Response({'error': 'Task not found'}, status=status.HTTP_404_NOT_FOUND)

            # Update task status
            if task_id not in onboarding_session.completed_tasks:
                onboarding_session.completed_tasks.append(task_id)
                # Remove from skipped if it was there
                if task_id in onboarding_session.skipped_tasks:
                    onboarding_session.skipped_tasks.remove(task_id)
                onboarding_session.save()

            # Build updated response
            tasks = self._build_tasks_with_status(template, onboarding_session)
            total_tasks = len(template.template_data.get('items', []))
            completed_count = len(onboarding_session.completed_tasks)
            skipped_count = len(onboarding_session.skipped_tasks)
            progress_percentage = int((completed_count + skipped_count) / total_tasks * 100) if total_tasks > 0 else 0

            # Don't auto-complete onboarding when progress reaches 100%
            # Use separate API endpoint to mark as complete
            is_completed = onboarding_session.is_completed

            response_data = {
                "onboarding_enabled": True,
                "onboarding_active": True,
                "is_completed": is_completed,
                "template": {
                    "template_id": template.template_id,
                    "title": template.title,
                    "description": template.description,
                    "welcome_message": template.welcome_message,
                },
                "tasks": tasks,
                "progress": {
                    "total_tasks": total_tasks,
                    "completed_tasks": completed_count,
                    "skipped_tasks": skipped_count,
                    "progress_percentage": progress_percentage
                }
            }

            return Response(response_data)

        except ProjectDetails.DoesNotExist:
            return Response({'error': 'Project not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            logger.error(f"Error completing task: {str(e)}", exc_info=True)
            return Response({'error': 'Internal server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @action(detail=False, methods=["POST"])
    def skip_task(self, request: Request, project_id: str) -> Response:
        """Skip a specific task (random access allowed)"""
        try:

            task_id = request.data.get("task_id")
            if not task_id:
                return Response({'error': 'Task ID is required'}, status=status.HTTP_400_BAD_REQUEST)

            # Get project and user
            project = ProjectDetails.objects.get(project_uuid=project_id)

            try:
                user = self._get_user_from_context()
            except ValueError as e:
                return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)

            try:
                onboarding_session = ProjectOnboardingSession.objects.get(
                    project=project,
                    user=user
                )
            except ProjectOnboardingSession.DoesNotExist:
                return Response({'error': 'Onboarding session not found'}, status=status.HTTP_404_NOT_FOUND)

            # Get template to validate task exists
            template = onboarding_session.template
            if not template:
                return Response({'error': 'Onboarding template not found'}, status=status.HTTP_404_NOT_FOUND)

            # Validate task exists in template
            if not self._task_exists_in_template(template, task_id):
                return Response({'error': 'Task not found'}, status=status.HTTP_404_NOT_FOUND)

            # Update task status
            if task_id not in onboarding_session.skipped_tasks:
                onboarding_session.skipped_tasks.append(task_id)
                # Remove from completed if it was there
                if task_id in onboarding_session.completed_tasks:
                    onboarding_session.completed_tasks.remove(task_id)
                onboarding_session.save()

            # Build updated response
            tasks = self._build_tasks_with_status(template, onboarding_session)
            total_tasks = len(template.template_data.get('items', []))
            completed_count = len(onboarding_session.completed_tasks)
            skipped_count = len(onboarding_session.skipped_tasks)
            progress_percentage = int((completed_count + skipped_count) / total_tasks * 100) if total_tasks > 0 else 0

            # Don't auto-complete onboarding when progress reaches 100%
            # Use separate API endpoint to mark as complete
            is_completed = onboarding_session.is_completed

            response_data = {
                "onboarding_enabled": True,
                "onboarding_active": True,
                "is_completed": is_completed,
                "template": {
                    "template_id": template.template_id,
                    "title": template.title,
                    "description": template.description,
                    "welcome_message": template.welcome_message,
                },
                "tasks": tasks,
                "progress": {
                    "total_tasks": total_tasks,
                    "completed_tasks": completed_count,
                    "skipped_tasks": skipped_count,
                    "progress_percentage": progress_percentage
                }
            }

            return Response(response_data)

        except ProjectDetails.DoesNotExist:
            return Response({'error': 'Project not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            logger.error(f"Error skipping task: {str(e)}", exc_info=True)
            return Response({'error': 'Internal server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @action(detail=False, methods=["POST"])
    def reset_onboarding(self, request: Request, project_id: str) -> Response:
        """Reset onboarding session for a project"""
        try:

            # Get project details
            try:
                project = ProjectDetails.objects.get(project_uuid=project_id)
            except ProjectDetails.DoesNotExist:
                return Response({'error': 'Project not found'}, status=status.HTTP_404_NOT_FOUND)

            # Get user object from context
            try:
                user = self._get_user_from_context()
            except ValueError as e:
                return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)

            # Reset onboarding session
            try:
                onboarding_session = ProjectOnboardingSession.objects.get(
                    project=project,
                    user=user
                )
                onboarding_session.completed_tasks = []
                onboarding_session.skipped_tasks = []
                onboarding_session.save()
            except ProjectOnboardingSession.DoesNotExist:
                # Create new session if it doesn't exist
                onboarding_session = ProjectOnboardingSession.objects.create(
                    project=project,
                    user=user,
                    template=self._get_template_for_project(project),
                    completed_tasks=[],
                    skipped_tasks=[],
                )

            # Get template and build response
            template = onboarding_session.template
            tasks = self._build_tasks_with_status(template, onboarding_session)

            response_data = {
                "onboarding_enabled": True,
                "onboarding_active": True,
                "template": {
                    "template_id": template.template_id,
                    "title": template.title,
                    "description": template.description,
                    "welcome_message": template.welcome_message,
                },
                "tasks": tasks,
                "progress": {
                    "total_tasks": len(template.template_data.get('items', [])),
                    "completed_tasks": 0,
                    "skipped_tasks": 0,
                    "progress_percentage": 0
                }
            }

            return Response(response_data)

        except Exception as e:
            logger.error(f"Error resetting onboarding: {str(e)}", exc_info=True)
            return Response({'error': 'Internal server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @action(detail=False, methods=["POST"])
    def toggle_project_onboarding(self, request: Request, project_id: str) -> Response:
        """Toggle onboarding enabled status for a project"""
        try:

            # Get project details
            try:
                project = ProjectDetails.objects.get(project_uuid=project_id)
            except ProjectDetails.DoesNotExist:
                return Response({'error': 'Project not found'}, status=status.HTTP_404_NOT_FOUND)

            # Toggle onboarding status
            project.onboarding_enabled = not project.onboarding_enabled
            project.save()

            return Response({
                "project_id": project_id,
                "onboarding_enabled": project.onboarding_enabled,
                "message": f"Onboarding {'enabled' if project.onboarding_enabled else 'disabled'} for project"
            })

        except Exception as e:
            logger.error(f"Error toggling project onboarding: {str(e)}", exc_info=True)
            return Response({'error': 'Internal server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def _get_template_for_project(self, project: ProjectDetails) -> OnboardingTemplate:
        """Get template based on project type"""
        if not project.project_type:
            # Default to jaffleshop_starter if no project type
            template_id = "jaffleshop_starter"
        else:
            template_id = project.project_type

        try:
            return OnboardingTemplate.objects.get(template_id=template_id)
        except OnboardingTemplate.DoesNotExist:
            # Fallback to jaffle_shop_starter if template not found
            return OnboardingTemplate.objects.get(template_id="jaffleshop_starter")

    def _build_tasks_with_status(self, template: OnboardingTemplate, session: ProjectOnboardingSession) -> List[Dict]:
        """Build tasks list with individual status for each task"""
        tasks = []

        for task in template.template_data.get('items', []):
            task_id = task.get("id")
            if not task_id:
                continue

            # Determine task status
            if task_id in session.completed_tasks:
                status = "completed"
            elif task_id in session.skipped_tasks:
                status = "skipped"
            else:
                status = "pending"

            tasks.append({
                "id": task_id,
                "title": task.get("title", ""),
                "description": task.get("description", ""),
                "prompt": task.get("prompt", ""),
                "mode": task.get("mode", ""),
                "status": status
            })

        return tasks

    @action(detail=False, methods=["POST"])
    def mark_complete(self, request: Request, project_id: str) -> Response:
        """Manually mark onboarding as complete"""
        try:
            # Get project
            try:
                project = ProjectDetails.objects.get(project_uuid=project_id)
            except ProjectDetails.DoesNotExist:
                return Response({'error': 'Project not found'}, status=status.HTTP_404_NOT_FOUND)

            # Get user from context
            try:
                user = self._get_user_from_context()
            except ValueError as e:
                return Response({'error': str(e)}, status=status.HTTP_400_BAD_REQUEST)

            # Get onboarding session
            try:
                onboarding_session = ProjectOnboardingSession.objects.get(
                    project=project,
                    user=user
                )
            except ProjectOnboardingSession.DoesNotExist:
                return Response({'error': 'Onboarding session not found'}, status=status.HTTP_404_NOT_FOUND)

            # Mark as complete
            if not onboarding_session.is_completed:
                onboarding_session.is_completed = True
                onboarding_session.completed_at = timezone.now()
                onboarding_session.save()

            return Response({
                'message': 'Onboarding marked as complete',
                'is_completed': True,
                'completed_at': onboarding_session.completed_at
            }, status=status.HTTP_200_OK)

        except Exception as e:
            logger.error(f"Error marking onboarding complete: {str(e)}", exc_info=True)
            return Response({'error': 'Internal server error'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def _task_exists_in_template(self, template: OnboardingTemplate, task_id: str) -> bool:
        """Check if task exists in template"""
        for task in template.template_data.get('items', []):
            if task.get("id") == task_id:
                return True
        return False

    def _get_user_from_context(self) -> User:
        """Get user object from current context"""
        current_user = get_current_user()
        if not current_user or not current_user.get("username"):
            raise ValueError("User not found in context")

        username = current_user.get("username")
        try:
            return User.objects.get(email=username)
        except User.DoesNotExist:
            raise ValueError(f"User with email {username} not found")
