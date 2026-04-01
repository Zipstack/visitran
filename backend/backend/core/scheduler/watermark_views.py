"""API views for watermark column detection."""

import logging
import json
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework import status

from backend.core.models.project_details import ProjectDetails
from backend.core.models.environment_models import EnvironmentModels
from backend.core.scheduler.watermark_service import WatermarkDetectionService

logger = logging.getLogger(__name__)


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def detect_watermark_columns(request, project_id):
    """Detect suitable watermark columns in project's database tables.

    POST /api/v1/visitran/{org_id}/project/{project_id}/jobs/watermark/detect/
    {
        "environment_id": "uuid",
        "table_name": "optional_table_name"
    }
    """
    try:
        environment_id = request.data.get('environment_id')
        table_name = request.data.get('table_name')

        if not environment_id:
            return Response(
                {'error': 'environment_id is required'},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Validate project exists and user has access
        try:
            project = ProjectDetails.objects.get(project_uuid=project_id)
            environment = EnvironmentModels.objects.get(environment_id=environment_id)
        except (ProjectDetails.DoesNotExist, EnvironmentModels.DoesNotExist):
            return Response(
                {'error': 'Project or environment not found'},
                status=status.HTTP_404_NOT_FOUND
            )

        # Initialize enhanced watermark detection service with project context
        detection_service = WatermarkDetectionService(environment_id, project_id)

        # Use enhanced detection with project-aware intelligence
        watermark_data = detection_service.detect_watermark_columns(table_name)

        # Service always returns flat format:
        # {timestamp_candidates, sequence_candidates, table_info, ?error, ?message}
        if watermark_data.get('error'):
            return Response(watermark_data, status=status.HTTP_400_BAD_REQUEST)

        return Response(watermark_data, status=status.HTTP_200_OK)

    except json.JSONDecodeError:
        return Response(
            {'error': 'Invalid JSON in request body'},
            status=status.HTTP_400_BAD_REQUEST
        )
    except Exception as e:
        logger.error(f"Error detecting watermark columns: {str(e)}")
        return Response(
            {'error': 'Internal server error during column detection'},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )
