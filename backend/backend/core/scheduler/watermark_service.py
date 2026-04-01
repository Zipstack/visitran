"""
Watermark Service for Incremental Job Processing
Handles watermark detection, tracking, and incremental data processing
"""

import logging
from datetime import datetime
from typing import Dict, Any, Optional, Tuple, List
from django.db import connection
from django.utils import timezone

from backend.core.models.environment_models import EnvironmentModels
from backend.application.context.application import ApplicationContext
from backend.core.scheduler.models import UserTaskDetails

try:
    from backend.core.scheduler.watermark_models import WatermarkHistory
except ImportError:
    WatermarkHistory = None


logger = logging.getLogger(__name__)


class WatermarkDetectionService:
    """Service for detecting suitable watermark columns in database tables"""

    # Common timestamp column patterns
    TIMESTAMP_PATTERNS = [
        'created_at', 'updated_at', 'modified_at', 'inserted_at',
        'timestamp', 'date_created', 'date_modified', 'last_updated',
        'creation_time', 'modification_time', 'event_time'
    ]

    # Common ID column patterns
    ID_PATTERNS = [
        'id', 'primary_key', 'pk', 'row_id', 'record_id',
        'sequence_id', 'auto_id', 'unique_id'
    ]

    def __init__(self, environment_id: str, project_id: str = None):
        self.environment_id = environment_id
        self.project_id = project_id
        self.environment = EnvironmentModels.objects.get(environment_id=environment_id)

        # Use ApplicationContext for proper database connection and project integration
        if project_id:
            try:
                self.app_context = ApplicationContext(project_id=project_id, environment_id=environment_id)
                logger.info(f"ApplicationContext initialized for project {project_id}, environment {environment_id}")
            except Exception as e:
                logger.error(f"Failed to initialize ApplicationContext: {e}")
                self.app_context = None
        else:
            logger.warning("No project_id provided, ApplicationContext not initialized")
            self.app_context = None

    def detect_watermark_columns(self, table_name: str = None) -> Dict[str, Any]:
        """
        Detect watermark columns.

        When *table_name* is provided, analyse that single table.
        When *table_name* is ``None``, analyse all project source tables
        and merge the candidates into a flat response the frontend expects:
        ``{timestamp_candidates, sequence_candidates, table_info}``.
        """
        try:
            if table_name:
                # Analyze specific table - parse schema.table format
                if '.' in table_name:
                    schema_name, table_only = table_name.split('.', 1)
                else:
                    schema_name, table_only = "", table_name
                return self._analyze_single_table(table_only, schema_name)

            # --- Auto-detect from project models / environment tables ---
            if not self.app_context:
                return {
                    'error': 'Project context required for auto-detection',
                    'timestamp_candidates': [],
                    'sequence_candidates': [],
                    'table_info': {},
                }

            source_tables = self._get_project_source_tables()
            logger.info(f"Project source tables found: {len(source_tables)}")

            # Fallback: if no source tables in model configs, list
            # all tables from the environment schemas.
            if not source_tables:
                logger.info("No source tables in model configs, falling back to environment tables")
                available = self._get_available_tables_for_selection()
                source_tables = [
                    {
                        'model_name': t.get('table_name', ''),
                        'schema_name': t.get('schema_name', t.get('table_schema', '')),
                        'table_name': t.get('table_name', ''),
                    }
                    for t in available
                ]

            if not source_tables:
                return {
                    'timestamp_candidates': [],
                    'sequence_candidates': [],
                    'table_info': {},
                    'message': 'No tables found in the environment',
                }

            # Merge candidates from all tables into flat lists.
            all_ts: List[Dict] = []
            all_seq: List[Dict] = []
            total_rows = 0
            total_cols = 0

            for tbl in source_tables:
                schema_name = tbl['schema_name']
                tbl_name = tbl['table_name']
                analysis = self._analyze_single_table(tbl_name, schema_name)

                # Tag each candidate with its source table
                for c in analysis.get('timestamp_candidates', []):
                    c['source_table'] = f"{schema_name}.{tbl_name}" if schema_name else tbl_name
                    all_ts.append(c)
                for c in analysis.get('sequence_candidates', []):
                    c['source_table'] = f"{schema_name}.{tbl_name}" if schema_name else tbl_name
                    all_seq.append(c)

                info = analysis.get('table_info', {})
                total_rows += info.get('row_count', 0) or 0
                total_cols += info.get('total_columns', 0) or 0

            # Sort merged lists by confidence
            all_ts.sort(key=lambda x: x.get('confidence', 0), reverse=True)
            all_seq.sort(key=lambda x: x.get('confidence', 0), reverse=True)

            return {
                'timestamp_candidates': all_ts,
                'sequence_candidates': all_seq,
                'table_info': {
                    'table_name': ', '.join(t['table_name'] for t in source_tables),
                    'schema_name': source_tables[0]['schema_name'] if source_tables else '',
                    'total_columns': total_cols,
                    'row_count': total_rows,
                    'tables_analyzed': len(source_tables),
                },
            }

        except Exception as e:
            logger.error(f"Error in watermark detection: {e}")
            return {
                'error': str(e),
                'timestamp_candidates': [],
                'sequence_candidates': [],
                'table_info': {},
            }

    def _get_project_source_tables(self) -> List[Dict[str, str]]:
        """Extract source tables from all project models"""
        if not self.app_context:
            return []

        try:
            # Use the correct ApplicationContext method to fetch all models
            models = self.app_context.session.fetch_all_models(fetch_all=True)
            source_tables = []

            for model in models:
                model_data = model.model_data
                source = model_data.get("source", {})

                # Check if this model has a source table
                if source.get("table_name"):
                    schema_name = source.get("schema_name", "")
                    table_name = source.get("table_name")

                    source_tables.append({
                        "model_name": model.model_name,
                        "schema_name": schema_name,
                        "table_name": table_name,
                        "full_table_name": f"{schema_name}.{table_name}".strip('.')
                    })

            logger.info(f"Found {len(source_tables)} source tables from project models")
            return source_tables

        except Exception as e:
            logger.error(f"Error extracting source tables from project models: {e}")
            return []

    def _analyze_single_table(self, table_name: str, schema_name: str = "") -> Dict[str, Any]:
        """Analyze a single table for watermark candidates"""
        try:
            columns = self._get_table_columns_via_app_context(schema_name, table_name)

            timestamp_candidates = []
            sequence_candidates = []

            for column in columns:
                col_name = column['column_name'].lower()
                col_type = column.get('column_dbtype', column.get('data_type', '')).lower()

                # Check for timestamp columns
                if self._is_timestamp_column(col_name, col_type):
                    timestamp_candidates.append({
                        'column_name': column['column_name'],
                        'data_type': column.get('column_dbtype', column.get('data_type', '')),
                        'is_nullable': column.get('nullable', True),
                        'confidence': self._calculate_timestamp_confidence(col_name, col_type),
                        'sample_values': self._get_sample_values_via_app_context(schema_name, table_name, column['column_name'])
                    })

                # Check for sequence/ID columns
                if self._is_sequence_column(col_name, col_type):
                    sequence_candidates.append({
                        'column_name': column['column_name'],
                        'data_type': column.get('column_dbtype', column.get('data_type', '')),
                        'is_nullable': column.get('nullable', True),
                        'confidence': self._calculate_sequence_confidence(col_name, col_type),
                        'sample_values': self._get_sample_values_via_app_context(schema_name, table_name, column['column_name'])
                    })

            # Sort by confidence score
            timestamp_candidates.sort(key=lambda x: x['confidence'], reverse=True)
            sequence_candidates.sort(key=lambda x: x['confidence'], reverse=True)

            return {
                'timestamp_candidates': timestamp_candidates,
                'sequence_candidates': sequence_candidates,
                'table_info': {
                    'table_name': table_name,
                    'schema_name': schema_name,
                    'total_columns': len(columns),
                    'row_count': self._get_table_row_count_via_app_context(schema_name, table_name)
                }
            }

        except Exception as e:
            logger.error(f"Error analyzing table {schema_name}.{table_name}: {e}")
            return {'timestamp_candidates': [], 'sequence_candidates': [], 'table_info': {}}

    def _get_table_columns_via_app_context(self, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
        """Get column information using ApplicationContext"""
        if self.app_context:
            try:
                return self.app_context.get_table_columns(
                    schema_name=schema_name,
                    table_name=table_name
                )
            except Exception as e:
                logger.warning(f"Failed to get columns via ApplicationContext: {e}")

        # Fallback to direct query
        return self._get_table_columns_fallback(table_name)

    def _get_table_columns_fallback(self, table_name: str) -> List[Dict[str, Any]]:
        """Fallback method for getting table columns"""
        with connection.cursor() as cursor:
            # PostgreSQL-specific query (adapt for other databases)
            cursor.execute("""
                SELECT
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    ordinal_position
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
            """, [table_name])

            columns = cursor.fetchall()
            return [
                {
                    'column_name': col[0],
                    'data_type': col[1],
                    'is_nullable': col[2],
                    'column_default': col[3],
                    'ordinal_position': col[4]
                }
                for col in columns
            ]

    def _is_timestamp_column(self, col_name: str, col_type: str) -> bool:
        """Check if column is likely a timestamp column"""
        # Check name patterns
        name_match = any(pattern in col_name for pattern in self.TIMESTAMP_PATTERNS)

        # Check data type
        type_match = any(ts_type in col_type for ts_type in [
            'timestamp', 'datetime', 'date', 'time'
        ])

        return name_match or type_match

    def _is_sequence_column(self, col_name: str, col_type: str) -> bool:
        """Check if column is likely a sequence/ID column"""
        # Check name patterns
        name_match = any(pattern in col_name for pattern in self.ID_PATTERNS)

        # Check data type
        type_match = any(id_type in col_type for id_type in [
            'serial', 'bigserial', 'integer', 'bigint', 'int', 'uuid'
        ])

        return name_match and type_match

    def _calculate_timestamp_confidence(self, col_name: str, col_type: str) -> float:
        """Calculate confidence score for timestamp column (0-1)"""
        score = 0.0

        # High confidence patterns
        if col_name in ['created_at', 'updated_at', 'timestamp']:
            score += 0.5

        # Medium confidence patterns
        elif any(pattern in col_name for pattern in ['created', 'updated', 'modified', 'time']):
            score += 0.3

        # Data type bonus
        if 'timestamp' in col_type:
            score += 0.4
        elif 'datetime' in col_type:
            score += 0.3
        elif 'date' in col_type:
            score += 0.2

        return min(score, 1.0)

    def _calculate_sequence_confidence(self, col_name: str, col_type: str) -> float:
        """Calculate confidence score for sequence column (0-1)"""
        score = 0.0

        # High confidence patterns
        if col_name in ['id', 'pk', 'primary_key']:
            score += 0.5

        # Medium confidence patterns
        elif col_name.endswith('_id') or 'sequence' in col_name:
            score += 0.3

        # Data type bonus
        if 'serial' in col_type:
            score += 0.4
        elif any(int_type in col_type for int_type in ['integer', 'bigint', 'int']):
            score += 0.3

        return min(score, 1.0)

    def _get_sample_values_via_app_context(self, schema_name: str, table_name: str, column_name: str, limit: int = 5) -> List[Any]:
        """Get sample values using ApplicationContext"""
        if self.app_context:
            try:
                # Get sample records from the table
                records = self.app_context.visitran_context.get_table_records(
                    schema_name=schema_name,
                    table_name=table_name,
                    selective_columns=[column_name],
                    limit=limit,
                    page=1
                )

                # Extract unique values from the column
                values = []
                for record in records:
                    if isinstance(record, dict) and column_name in record:
                        val = record[column_name]
                        if val is not None and val not in values:
                            values.append(val)
                    elif isinstance(record, (list, tuple)) and len(record) > 0:
                        val = record[0]
                        if val is not None and val not in values:
                            values.append(val)

                return values[:limit]

            except Exception as e:
                logger.warning(f"Failed to get sample values via ApplicationContext: {e}")

        # Fallback to direct query
        return self._get_sample_values_fallback(table_name, column_name, limit)

    def _get_sample_values_fallback(self, table_name: str, column_name: str, limit: int = 5) -> List[Any]:
        """Fallback method for getting sample values"""
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT DISTINCT {column_name} FROM {table_name} "
                    f"WHERE {column_name} IS NOT NULL "
                    f"ORDER BY {column_name} DESC LIMIT %s",
                    [limit]
                )
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.warning(f"Could not get sample values for {table_name}.{column_name}: {e}")
            return []

    def _get_table_row_count_via_app_context(self, schema_name: str, table_name: str) -> int:
        """Get table row count using ApplicationContext"""
        if self.app_context:
            try:
                return self.app_context.visitran_context.get_table_record_count(
                    schema_name=schema_name,
                    table_name=table_name
                )
            except Exception as e:
                logger.warning(f"Failed to get row count via ApplicationContext: {e}")

        # Fallback to direct query
        return self._get_table_row_count_fallback(table_name)

    def _get_table_row_count_fallback(self, table_name: str) -> int:
        """Fallback method for getting table row count"""
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                return cursor.fetchone()[0]
        except Exception:
            return 0

    def _get_primary_source_table(self, project_id: str) -> Optional[str]:
        """Get the primary source table from project configuration"""
        try:
            # This would parse the project's transformation configuration
            # to determine the main source table
            # For now, return None to indicate table_name is required
            return None
        except Exception:
            return None

    def _get_available_tables_for_selection(self) -> List[Dict[str, Any]]:
        """Get list of available tables for user selection"""
        if self.app_context:
            try:
                # Get all schemas
                schemas = self.app_context.get_all_schemas()
                available_tables = []

                for schema in schemas:
                    schema_name = schema[0] if isinstance(schema, (list, tuple)) else str(schema)

                    # Get tables in this schema
                    tables = self.app_context.get_all_tables(schema_name)

                    for table in tables:
                        table_name = table[0] if isinstance(table, (list, tuple)) else str(table)

                        available_tables.append({
                            'table_name': table_name,
                            'schema_name': schema_name,
                            'full_table_name': f"{schema_name}.{table_name}",
                            'row_count': self._get_table_row_count_via_app_context(schema_name, table_name)
                        })

                return available_tables

            except Exception as e:
                logger.warning(f"Failed to get available tables via ApplicationContext: {e}")

        # Fallback to direct query
        return self._get_available_tables_fallback()

    def _get_available_tables_fallback(self) -> List[Dict[str, Any]]:
        """Fallback method for getting available tables"""
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT
                        table_name,
                        table_type,
                        table_schema
                    FROM information_schema.tables
                    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                    ORDER BY table_name
                """)

                tables = cursor.fetchall()
                return [
                    {
                        'table_name': table[0],
                        'table_type': table[1],
                        'table_schema': table[2],
                        'full_table_name': f"{table[2]}.{table[0]}",
                        'row_count': self._get_table_row_count_fallback(table[0])
                    }
                    for table in tables
                ]
        except Exception as e:
            logger.error(f"Error getting available tables: {e}")
            return []

    def _validate_watermark_column(self, table_name: str, column_name: str, strategy: str) -> Dict[str, Any]:
        """Validate a specific column for watermark suitability"""
        try:
            # Parse schema and table name
            if '.' in table_name:
                schema_name, table_only = table_name.split('.', 1)
            else:
                schema_name, table_only = "", table_name
            columns = self._get_table_columns_via_app_context(schema_name, table_only)
            target_column = next((col for col in columns if col['column_name'] == column_name), None)

            if not target_column:
                return {
                    'valid': False,
                    'error': f"Column '{column_name}' not found in table '{table_name}'"
                }

            # Validate based on strategy
            if strategy == 'TIMESTAMP':
                is_valid = self._is_timestamp_column(column_name.lower(), target_column['data_type'].lower())
                confidence = self._calculate_timestamp_confidence(column_name.lower(), target_column['data_type'].lower())
            elif strategy == 'SEQUENCE':
                is_valid = self._is_sequence_column(column_name.lower(), target_column['data_type'].lower())
                confidence = self._calculate_sequence_confidence(column_name.lower(), target_column['data_type'].lower())
            else:  # CUSTOM
                is_valid = True  # Allow any column for custom strategy
                confidence = 0.5

            sample_values = self._get_sample_values_via_app_context(schema_name, table_only, column_name)

            return {
                'valid': is_valid,
                'confidence': confidence,
                'column_info': target_column,
                'sample_values': sample_values,
                'recommendations': self._get_column_recommendations(target_column, strategy)
            }

        except Exception as e:
            logger.error(f"Error validating watermark column: {e}")
            return {
                'valid': False,
                'error': f"Error validating column: {str(e)}"
            }

    def _get_column_recommendations(self, column_info: Dict[str, Any], strategy: str) -> List[str]:
        """Get recommendations for using this column as watermark"""
        recommendations = []

        if column_info['is_nullable'] == 'YES':
            recommendations.append("Column allows NULL values - ensure proper handling of NULL watermarks")

        if strategy == 'TIMESTAMP':
            if 'timestamp' not in column_info['data_type'].lower():
                recommendations.append("Consider using a proper timestamp column for better performance")

        if strategy == 'SEQUENCE':
            if 'serial' not in column_info['data_type'].lower():
                recommendations.append("Auto-increment columns work best for sequence-based watermarking")

        return recommendations


class WatermarkProcessingService:
    """Service for processing incremental data using watermarks"""

    def __init__(self, user_task: UserTaskDetails):
        self.user_task = user_task
        self.app_context = ApplicationContext(project_id=user_task.project_id)

    def should_execute_incremental(self) -> Tuple[bool, str]:
        """
        Determine if incremental execution should proceed
        Returns (should_execute, reason)
        """
        if not self.user_task.incremental_enabled:
            return True, "incremental_disabled"

        if not self.user_task.watermark_column:
            return True, "no_watermark_column"

        # Check if new data exists since last watermark
        has_new_data, new_count = self._check_for_new_data()

        if not has_new_data:
            return False, f"no_new_data_since_watermark"

        return True, f"new_data_available_count_{new_count}"

    def execute_incremental_run(self, environment_id: str) -> Dict[str, Any]:
        """
        Execute incremental data processing using watermarks
        """
        start_time = timezone.now()

        try:
            # Get current watermark value
            current_watermark = self._get_current_watermark_value()

            # Modify the transformation to include watermark filtering
            self._apply_watermark_filter(current_watermark)

            # Execute the transformation
            self.app_context.execute_visitran_run_command(
                current_model=self.user_task.task_name,
                environment_id=environment_id
            )

            # Get new watermark value after processing
            new_watermark = self._get_new_watermark_value()

            # Update task watermark
            self._update_task_watermark(new_watermark)

            # Record watermark history
            records_processed = self._count_processed_records(current_watermark, new_watermark)
            self._record_watermark_history(
                watermark_value=new_watermark,
                execution_time=start_time,
                records_processed=records_processed,
                execution_duration=(timezone.now() - start_time).total_seconds()
            )

            return {
                'status': 'success',
                'execution_type': 'incremental',
                'previous_watermark': current_watermark,
                'new_watermark': new_watermark,
                'records_processed': records_processed,
                'execution_duration_seconds': (timezone.now() - start_time).total_seconds()
            }

        except Exception as e:
            logger.error(f"Incremental execution failed for task {self.user_task.id}: {e}")

            # Fallback to full execution on incremental failure
            self.app_context.execute_visitran_run_command(
                current_model=self.user_task.task_name,
                environment_id=environment_id
            )

            return {
                'status': 'success',
                'execution_type': 'full_fallback',
                'error': str(e),
                'execution_duration_seconds': (timezone.now() - start_time).total_seconds()
            }

    def _check_for_new_data(self) -> Tuple[bool, int]:
        """Check if new data exists since last watermark"""
        if not self.user_task.last_watermark_value:
            return True, 0  # First run, assume data exists

        try:
            # Get source table from transformation config
            source_table = self._get_source_table_name()
            watermark_column = self.user_task.watermark_column
            last_watermark = self.user_task.last_watermark_value

            with connection.cursor() as cursor:
                if self.user_task.watermark_strategy == 'TIMESTAMP':
                    cursor.execute(
                        f"SELECT COUNT(*) FROM {source_table} WHERE {watermark_column} > %s",
                        [last_watermark]
                    )
                else:  # SEQUENCE
                    cursor.execute(
                        f"SELECT COUNT(*) FROM {source_table} WHERE {watermark_column} > %s",
                        [int(last_watermark)]
                    )

                count = cursor.fetchone()[0]
                return count > 0, count

        except Exception as e:
            logger.warning(f"Could not check for new data: {e}")
            return True, 0  # Assume data exists on error

    def _get_current_watermark_value(self) -> Optional[str]:
        """Get the current watermark value for filtering"""
        return self.user_task.last_watermark_value

    def _apply_watermark_filter(self, watermark_value: Optional[str]):
        """Apply watermark filtering to the transformation"""
        if not watermark_value or not self.user_task.watermark_column:
            return

        # This would modify the transformation configuration to add WHERE clause
        # Implementation depends on how Visitran handles transformation configs
        # For now, this is a placeholder for the filtering logic
        logger.info(f"Applying watermark filter: {self.user_task.watermark_column} > {watermark_value}")

    def _get_new_watermark_value(self) -> str:
        """Get the new watermark value after processing"""
        try:
            source_table = self._get_source_table_name()
            watermark_column = self.user_task.watermark_column

            with connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT MAX({watermark_column}) FROM {source_table}"
                )
                result = cursor.fetchone()[0]
                return str(result) if result else ""

        except Exception as e:
            logger.error(f"Could not get new watermark value: {e}")
            return ""

    def _get_source_table_name(self) -> str:
        """Extract source table name from transformation configuration"""
        # This would parse the transformation config to get source table
        # Placeholder implementation
        return "source_table"

    def _count_processed_records(self, old_watermark: Optional[str], new_watermark: str) -> int:
        """Count records processed in this incremental run"""
        if not old_watermark:
            return 0

        try:
            source_table = self._get_source_table_name()
            watermark_column = self.user_task.watermark_column

            with connection.cursor() as cursor:
                if self.user_task.watermark_strategy == 'TIMESTAMP':
                    cursor.execute(
                        f"SELECT COUNT(*) FROM {source_table} "
                        f"WHERE {watermark_column} > %s AND {watermark_column} <= %s",
                        [old_watermark, new_watermark]
                    )
                else:  # SEQUENCE
                    cursor.execute(
                        f"SELECT COUNT(*) FROM {source_table} "
                        f"WHERE {watermark_column} > %s AND {watermark_column} <= %s",
                        [int(old_watermark), int(new_watermark)]
                    )

                return cursor.fetchone()[0]

        except Exception as e:
            logger.warning(f"Could not count processed records: {e}")
            return 0

    def _update_task_watermark(self, new_watermark: str):
        """Update the task's watermark value"""
        self.user_task.last_watermark_value = new_watermark
        self.user_task.save(update_fields=['last_watermark_value'])

    def _record_watermark_history(self, watermark_value: str, execution_time: datetime,
                                records_processed: int, execution_duration: float):
        """Record watermark execution in history"""
        if WatermarkHistory is None:
            return
        WatermarkHistory.objects.create(
            user_task=self.user_task,
            watermark_value=watermark_value,
            execution_time=execution_time,
            records_processed=records_processed,
            execution_duration_seconds=execution_duration,
            strategy_used=self.user_task.watermark_strategy,
            organization_id=self.user_task.organization_id,
            metadata={
                'watermark_column': self.user_task.watermark_column,
                'incremental_enabled': self.user_task.incremental_enabled
            }
        )
