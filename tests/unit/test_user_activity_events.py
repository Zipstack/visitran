"""Tests for user-facing activity log events (U001–U020).

Each event is tested for:
- audience() returns "user"
- code() returns the correct U-code
- message() renders correctly with given fields
- title() / subtitle() produce expected UI text
- event_status() returns the correct status string
- level_tag() returns the expected EventLevel
"""

import pytest

from visitran.events.base_types import EventLevel, UserLevel
from visitran.events.types import (
    ConnectionCreated,
    ConnectionDeletedEvt,
    ConnectionDeleteFailedEvt,
    ConnectionTested,
    EnvironmentCreated,
    EnvironmentDeleted,
    FileDeleted,
    FileRenamed,
    JobCreated,
    JobDeleted,
    JobTriggered,
    JobUpdated,
    ModelConfigured,
    ModelCreated,
    ModelRunFailed,
    ModelRunStarted,
    ModelRunSucceeded,
    SeedCompleted,
    TransformationApplied,
    TransformationDeleted,
)


@pytest.mark.unit
@pytest.mark.minimal_core
class TestUserLevelBase:
    """Verify the UserLevel base class contract."""

    def test_audience_is_user(self):
        """All UserLevel subclasses must return audience='user'."""
        event = ModelCreated(model_name="test")
        assert event.audience() == "user"

    def test_default_level_is_info(self):
        event = ModelCreated(model_name="test")
        assert event.level_tag() == EventLevel.INFO

    def test_default_event_status_is_success(self):
        event = ModelCreated(model_name="test")
        assert event.event_status() == "success"

    def test_default_subtitle_is_empty(self):
        """UserLevel.subtitle() defaults to empty string unless overridden."""
        event = JobUpdated(job_name="nightly")
        assert event.subtitle() == ""


# ── P1: Core model operations ──────────────────────────────────────────


@pytest.mark.unit
@pytest.mark.minimal_core
class TestModelRunStarted:
    @pytest.fixture()
    def event(self):
        return ModelRunStarted(
            model_name="orders",
            source_table="raw.orders",
            destination_table="analytics.orders",
            materialization="table",
        )

    def test_is_user_level(self, event):
        assert isinstance(event, UserLevel)
        assert event.audience() == "user"

    def test_code(self, event):
        assert event.code() == "U001"

    def test_message(self, event):
        msg = event.message()
        assert '"orders"' in msg
        assert "analytics.orders" in msg
        assert "as table" in msg
        assert "raw.orders" in msg

    def test_message_without_materialization(self):
        event = ModelRunStarted(
            model_name="orders",
            source_table="raw.orders",
            destination_table="analytics.orders",
            materialization="",
        )
        assert "as " not in event.message()

    def test_title(self, event):
        assert event.title() == 'Building model "orders"'

    def test_subtitle(self, event):
        sub = event.subtitle()
        assert "TABLE" in sub
        assert "raw.orders → analytics.orders" in sub

    def test_event_status(self, event):
        assert event.event_status() == "running"


@pytest.mark.unit
@pytest.mark.minimal_core
class TestModelRunSucceeded:
    @pytest.fixture()
    def event(self):
        return ModelRunSucceeded(model_name="orders", duration_seconds=1.234)

    def test_code(self, event):
        assert event.code() == "U002"

    def test_message(self, event):
        msg = event.message()
        assert '"orders"' in msg
        assert "successfully" in msg
        assert "1.2s" in msg

    def test_message_without_duration(self):
        event = ModelRunSucceeded(model_name="orders", duration_seconds=0.0)
        msg = event.message()
        assert "successfully" in msg
        assert " in " not in msg

    def test_title(self, event):
        assert event.title() == 'Model "orders" built successfully'

    def test_subtitle_with_duration(self, event):
        assert "1.2s" in event.subtitle()

    def test_subtitle_without_duration(self):
        event = ModelRunSucceeded(model_name="orders", duration_seconds=0.0)
        assert event.subtitle() == ""

    def test_event_status(self, event):
        assert event.event_status() == "success"


@pytest.mark.unit
@pytest.mark.minimal_core
class TestModelRunFailed:
    @pytest.fixture()
    def event(self):
        return ModelRunFailed(model_name="orders", error="division by zero")

    def test_code(self, event):
        assert event.code() == "U003"

    def test_message(self, event):
        msg = event.message()
        assert '"orders"' in msg
        assert "failed" in msg
        assert "division by zero" in msg

    def test_message_truncates_long_error(self):
        long_error = "x" * 200
        event = ModelRunFailed(model_name="orders", error=long_error)
        msg = event.message()
        assert len(msg) < 200
        assert msg.endswith("…")

    def test_title(self, event):
        assert event.title() == 'Model "orders" failed'

    def test_subtitle_truncates_long_error(self):
        long_error = "x" * 200
        event = ModelRunFailed(model_name="orders", error=long_error)
        sub = event.subtitle()
        assert len(sub) <= 151  # 150 chars + ellipsis
        assert sub.endswith("…")

    def test_event_status(self, event):
        assert event.event_status() == "error"

    def test_level_is_error(self, event):
        assert event.level_tag() == EventLevel.ERROR


@pytest.mark.unit
@pytest.mark.minimal_core
class TestTransformationApplied:
    @pytest.fixture()
    def event(self):
        return TransformationApplied(model_name="orders", transformation_type="sort")

    def test_code(self, event):
        assert event.code() == "U004"

    def test_message(self, event):
        assert event.message() == 'Applied sort transformation on "orders"'

    def test_title(self, event):
        assert event.title() == 'Applied sort transformation on "orders"'

    def test_audience(self, event):
        assert event.audience() == "user"


@pytest.mark.unit
@pytest.mark.minimal_core
class TestTransformationDeleted:
    @pytest.fixture()
    def event(self):
        return TransformationDeleted(model_name="orders", transformation_type="filter")

    def test_code(self, event):
        assert event.code() == "U005"

    def test_message(self, event):
        assert event.message() == 'Removed filter transformation from "orders"'

    def test_title(self, event):
        assert event.title() == 'Removed filter transformation from "orders"'


@pytest.mark.unit
@pytest.mark.minimal_core
class TestModelConfigured:
    @pytest.fixture()
    def event(self):
        return ModelConfigured(
            model_name="orders", source="raw_db.public", destination="analytics.dw"
        )

    def test_code(self, event):
        assert event.code() == "U006"

    def test_message(self, event):
        msg = event.message()
        assert '"orders"' in msg
        assert "raw_db.public" in msg
        assert "analytics.dw" in msg

    def test_title(self, event):
        assert event.title() == 'Configured model "orders"'

    def test_subtitle(self, event):
        assert event.subtitle() == "raw_db.public → analytics.dw"


@pytest.mark.unit
@pytest.mark.minimal_core
class TestSeedCompleted:
    def test_code(self):
        event = SeedCompleted(seed_name="countries", schema_name="public", status="Success")
        assert event.code() == "U007"

    def test_message_success(self):
        event = SeedCompleted(seed_name="countries", schema_name="public", status="Success")
        assert event.message() == 'Seed "countries" loaded into "public"'

    def test_message_failure(self):
        event = SeedCompleted(seed_name="countries", schema_name="public", status="Failure")
        assert event.message() == 'Seed "countries" failed in "public"'

    def test_title_success(self):
        event = SeedCompleted(seed_name="countries", schema_name="public", status="Success")
        assert event.title() == 'Seed "countries" loaded successfully'

    def test_title_failure(self):
        event = SeedCompleted(seed_name="countries", schema_name="public", status="Failure")
        assert event.title() == 'Seed "countries" failed'

    def test_subtitle(self):
        event = SeedCompleted(seed_name="countries", schema_name="public", status="Success")
        assert event.subtitle() == "Schema: public"

    def test_event_status_success(self):
        event = SeedCompleted(seed_name="countries", schema_name="public", status="Success")
        assert event.event_status() == "success"

    def test_event_status_failure(self):
        event = SeedCompleted(seed_name="countries", schema_name="public", status="Failure")
        assert event.event_status() == "error"


# ── P2: Job scheduler ──────────────────────────────────────────────────


@pytest.mark.unit
@pytest.mark.minimal_core
class TestJobCreated:
    @pytest.fixture()
    def event(self):
        return JobCreated(job_name="nightly_sync", environment_name="production")

    def test_code(self, event):
        assert event.code() == "U008"

    def test_message(self, event):
        assert event.message() == 'Job "nightly_sync" created for environment "production"'

    def test_title(self, event):
        assert event.title() == 'Job "nightly_sync" created'

    def test_subtitle(self, event):
        assert event.subtitle() == "Environment: production"


@pytest.mark.unit
@pytest.mark.minimal_core
class TestJobUpdated:
    @pytest.fixture()
    def event(self):
        return JobUpdated(job_name="nightly_sync")

    def test_code(self, event):
        assert event.code() == "U009"

    def test_message(self, event):
        assert event.message() == 'Job "nightly_sync" updated'

    def test_title(self, event):
        assert event.title() == 'Job "nightly_sync" updated'


@pytest.mark.unit
@pytest.mark.minimal_core
class TestJobDeleted:
    @pytest.fixture()
    def event(self):
        return JobDeleted(job_name="nightly_sync")

    def test_code(self, event):
        assert event.code() == "U010"

    def test_message(self, event):
        assert event.message() == 'Job "nightly_sync" deleted'

    def test_event_status(self, event):
        assert event.event_status() == "warning"


@pytest.mark.unit
@pytest.mark.minimal_core
class TestJobTriggered:
    def test_code(self):
        event = JobTriggered(job_name="nightly_sync", scope="job")
        assert event.code() == "U011"

    def test_message_full_job(self):
        event = JobTriggered(job_name="nightly_sync", scope="job")
        assert "all models" in event.message()

    def test_message_single_model(self):
        event = JobTriggered(job_name="nightly_sync", scope="orders")
        assert "model orders" in event.message()

    def test_title(self):
        event = JobTriggered(job_name="nightly_sync", scope="job")
        assert event.title() == 'Job "nightly_sync" triggered'

    def test_subtitle_full_job(self):
        event = JobTriggered(job_name="nightly_sync", scope="job")
        sub = event.subtitle()
        assert "All models" in sub
        assert "Manual trigger" in sub

    def test_subtitle_single_model(self):
        event = JobTriggered(job_name="nightly_sync", scope="orders")
        sub = event.subtitle()
        assert "Model: orders" in sub

    def test_event_status(self):
        event = JobTriggered(job_name="nightly_sync", scope="job")
        assert event.event_status() == "running"


# ── P3: Model/file CRUD ────────────────────────────────────────────────


@pytest.mark.unit
@pytest.mark.minimal_core
class TestModelCreated:
    @pytest.fixture()
    def event(self):
        return ModelCreated(model_name="orders")

    def test_code(self, event):
        assert event.code() == "U012"

    def test_message(self, event):
        assert event.message() == 'Model "orders" created'

    def test_title(self, event):
        assert event.title() == 'Model "orders" created'


@pytest.mark.unit
@pytest.mark.minimal_core
class TestFileDeleted:
    @pytest.fixture()
    def event(self):
        return FileDeleted(file_names="orders.sql, users.sql")

    def test_code(self, event):
        assert event.code() == "U013"

    def test_message(self, event):
        assert event.message() == "Deleted: orders.sql, users.sql"

    def test_event_status(self, event):
        assert event.event_status() == "warning"


@pytest.mark.unit
@pytest.mark.minimal_core
class TestFileRenamed:
    @pytest.fixture()
    def event(self):
        return FileRenamed(old_name="orders_v1", new_name="orders_v2")

    def test_code(self, event):
        assert event.code() == "U014"

    def test_message(self, event):
        assert event.message() == 'Renamed "orders_v1" → "orders_v2"'

    def test_title(self, event):
        assert event.title() == "File renamed"

    def test_subtitle(self, event):
        assert event.subtitle() == '"orders_v1" → "orders_v2"'


# ── P4: Connections ─────────────────────────────────────────────────────


@pytest.mark.unit
@pytest.mark.minimal_core
class TestConnectionCreated:
    @pytest.fixture()
    def event(self):
        return ConnectionCreated(connection_name="prod_pg", datasource="postgres")

    def test_code(self, event):
        assert event.code() == "U015"

    def test_message(self, event):
        assert event.message() == "Connection created (postgres)"

    def test_title(self, event):
        assert event.title() == "Connection created"

    def test_subtitle(self, event):
        assert event.subtitle() == "Datasource: postgres"


@pytest.mark.unit
@pytest.mark.minimal_core
class TestConnectionTested:
    def test_code(self):
        event = ConnectionTested(datasource="postgres", result="success")
        assert event.code() == "U016"

    def test_message(self):
        event = ConnectionTested(datasource="postgres", result="success")
        assert event.message() == "Connection test: success (postgres)"

    def test_title_success(self):
        event = ConnectionTested(datasource="postgres", result="success")
        assert event.title() == "Connection test passed"

    def test_title_failure(self):
        event = ConnectionTested(datasource="postgres", result="failed")
        assert event.title() == "Connection test failed"

    def test_subtitle(self):
        event = ConnectionTested(datasource="snowflake", result="success")
        assert event.subtitle() == "Datasource: snowflake"


@pytest.mark.unit
@pytest.mark.minimal_core
class TestConnectionDeletedEvt:
    @pytest.fixture()
    def event(self):
        return ConnectionDeletedEvt(connection_name="prod_pg")

    def test_code(self, event):
        assert event.code() == "U017"

    def test_message(self, event):
        assert event.message() == 'Connection "prod_pg" deleted'

    def test_event_status(self, event):
        assert event.event_status() == "warning"


@pytest.mark.unit
@pytest.mark.minimal_core
class TestConnectionDeleteFailedEvt:
    @pytest.fixture()
    def event(self):
        return ConnectionDeleteFailedEvt(
            connection_name="prod_pg",
            reason="Connection is used by job nightly_sync",
        )

    def test_code(self, event):
        assert event.code() == "U020"

    def test_message(self, event):
        msg = event.message()
        assert "prod_pg" in msg
        assert "Connection is used by job nightly_sync" in msg

    def test_message_truncates_long_reason(self):
        long_reason = "x" * 200
        event = ConnectionDeleteFailedEvt(connection_name="prod_pg", reason=long_reason)
        msg = event.message()
        assert msg.endswith("…")

    def test_title(self, event):
        assert event.title() == 'Failed to delete connection "prod_pg"'

    def test_subtitle_truncates_long_reason(self):
        long_reason = "x" * 200
        event = ConnectionDeleteFailedEvt(connection_name="prod_pg", reason=long_reason)
        sub = event.subtitle()
        assert len(sub) <= 151
        assert sub.endswith("…")

    def test_event_status(self, event):
        assert event.event_status() == "error"

    def test_level_is_error(self, event):
        assert event.level_tag() == EventLevel.ERROR


# ── P5: Environments ───────────────────────────────────────────────────


@pytest.mark.unit
@pytest.mark.minimal_core
class TestEnvironmentCreated:
    @pytest.fixture()
    def event(self):
        return EnvironmentCreated(environment_name="staging")

    def test_code(self, event):
        assert event.code() == "U018"

    def test_message(self, event):
        assert event.message() == 'Environment "staging" created'

    def test_title(self, event):
        assert event.title() == 'Environment "staging" created'


@pytest.mark.unit
@pytest.mark.minimal_core
class TestEnvironmentDeleted:
    @pytest.fixture()
    def event(self):
        return EnvironmentDeleted(environment_name="staging")

    def test_code(self, event):
        assert event.code() == "U019"

    def test_message(self, event):
        assert event.message() == 'Environment "staging" deleted'

    def test_event_status(self, event):
        assert event.event_status() == "warning"


# ── Cross-cutting: all events share the UserLevel contract ─────────────


ALL_USER_EVENTS = [
    ModelRunStarted(model_name="m", source_table="s", destination_table="d", materialization="table"),
    ModelRunSucceeded(model_name="m", duration_seconds=1.0),
    ModelRunFailed(model_name="m", error="err"),
    TransformationApplied(model_name="m", transformation_type="sort"),
    TransformationDeleted(model_name="m", transformation_type="sort"),
    ModelConfigured(model_name="m", source="s", destination="d"),
    SeedCompleted(seed_name="s", schema_name="sc", status="Success"),
    JobCreated(job_name="j", environment_name="e"),
    JobUpdated(job_name="j"),
    JobDeleted(job_name="j"),
    JobTriggered(job_name="j", scope="job"),
    ModelCreated(model_name="m"),
    FileDeleted(file_names="f.sql"),
    FileRenamed(old_name="a", new_name="b"),
    ConnectionCreated(connection_name="c", datasource="postgres"),
    ConnectionTested(datasource="postgres", result="success"),
    ConnectionDeletedEvt(connection_name="c"),
    ConnectionDeleteFailedEvt(connection_name="c", reason="in use"),
    EnvironmentCreated(environment_name="e"),
    EnvironmentDeleted(environment_name="e"),
]


@pytest.mark.unit
@pytest.mark.minimal_core
class TestAllUserEventsContract:
    """Every user event must satisfy the UserLevel contract."""

    @pytest.mark.parametrize("event", ALL_USER_EVENTS, ids=lambda e: type(e).__name__)
    def test_is_user_level_instance(self, event):
        assert isinstance(event, UserLevel)

    @pytest.mark.parametrize("event", ALL_USER_EVENTS, ids=lambda e: type(e).__name__)
    def test_audience_is_user(self, event):
        assert event.audience() == "user"

    @pytest.mark.parametrize("event", ALL_USER_EVENTS, ids=lambda e: type(e).__name__)
    def test_code_starts_with_u(self, event):
        assert event.code().startswith("U")

    @pytest.mark.parametrize("event", ALL_USER_EVENTS, ids=lambda e: type(e).__name__)
    def test_message_is_non_empty_string(self, event):
        msg = event.message()
        assert isinstance(msg, str)
        assert len(msg) > 0

    @pytest.mark.parametrize("event", ALL_USER_EVENTS, ids=lambda e: type(e).__name__)
    def test_title_is_non_empty_string(self, event):
        title = event.title()
        assert isinstance(title, str)
        assert len(title) > 0

    @pytest.mark.parametrize("event", ALL_USER_EVENTS, ids=lambda e: type(e).__name__)
    def test_subtitle_is_string(self, event):
        sub = event.subtitle()
        assert isinstance(sub, str)

    @pytest.mark.parametrize("event", ALL_USER_EVENTS, ids=lambda e: type(e).__name__)
    def test_event_status_is_valid(self, event):
        valid_statuses = {"running", "success", "error", "warning", "info"}
        assert event.event_status() in valid_statuses

    @pytest.mark.parametrize("event", ALL_USER_EVENTS, ids=lambda e: type(e).__name__)
    def test_level_tag_is_event_level(self, event):
        assert isinstance(event.level_tag(), EventLevel)
