import datetime
from dataclasses import dataclass
from enum import Enum

from visitran.events import functions
from visitran.events.types import EndSummaryReportCounts, SeedReport, SnapshotReport, SummaryReport


class ExecStatus(Enum):
    Success = "SUCCESS"
    Error = "ERROR"
    Fail = "FAIL"
    Warn = "WARN"
    Skipped = "SKIPPED"
    Run = "RUN"
    OK = "OK"
    START = "START"
    COMPLETED = "COMPLETED"


@dataclass
class BaseResult:
    node_name: str
    status: str
    info_message: str
    failures: bool
    ending_time: datetime.datetime
    sequence_num: int
    end_status: str
    rows_affected: int | None = None
    rows_inserted: int | None = None
    rows_updated: int | None = None
    rows_deleted: int | None = None
    materialization: str = ""
    duration_ms: int | None = None


@dataclass
class SeedResult:
    seed_path: str
    schema_name: str
    status: str


@dataclass
class SnapshotResult:
    source_table: str
    status: str
    unique_key: str


BASE_RESULT: list[BaseResult] = []
SEED_RESULT: list[SeedResult] = []
SNAPSHOT_RESULT: list[SnapshotResult] = []


def parse_and_fire_seed_report() -> None:
    for iterate_result in SEED_RESULT:
        functions.fire_event(
            SeedReport(
                seed_path=iterate_result.seed_path,
                schema_name=iterate_result.schema_name,
                status=iterate_result.status,
            )
        )


def parse_and_fire_snapshot_report() -> None:
    for iterate_result in SNAPSHOT_RESULT:
        functions.fire_event(
            SnapshotReport(
                unique_key=iterate_result.unique_key,
                source_table=iterate_result.source_table,
                status=iterate_result.status,
            )
        )


def parse_and_fire_reports() -> None:
    total_count = len(BASE_RESULT)
    pass_count: int = 0
    warn_count: int = 0
    error_count: int = 0
    skip_count: int = 0
    for iterate_result in BASE_RESULT:
        functions.fire_event(
            SummaryReport(
                node_name=iterate_result.node_name,
                sequence_num=iterate_result.sequence_num,
                total_count=total_count,
                end_status=iterate_result.end_status,
                status=iterate_result.status,
            )
        )
        if iterate_result.end_status == ExecStatus.OK.value:
            pass_count += 1
        elif iterate_result.end_status == ExecStatus.Warn.value:
            warn_count += 1
        elif iterate_result.end_status == ExecStatus.Fail.value:
            error_count += 1
        elif iterate_result.end_status == ExecStatus.Skipped.value:
            skip_count += 1

    functions.fire_event(
        EndSummaryReportCounts(
            pass_count=pass_count,
            warn_count=warn_count,
            error_count=error_count,
            skip_count=skip_count,
        )
    )
