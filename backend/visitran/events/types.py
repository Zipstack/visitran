from __future__ import annotations

from dataclasses import dataclass

import visitran.events.proto_types as proto_type
from visitran.events.base_types import DebugLevel, ErrorLevel, InfoLevel, WarnLevel

#
# | Code |     Description     |
# |:----:|:-------------------:|
# | A    | Pre-project loading |
# | E    | DB adapters          |
# | I    | Project parsing     |
# | M    | Deps generation     |
# | Q    | Node processing     |
# | W    | Node testing        |
# | Y    | Post processing     |
# | Z    | Misc                |
# | T    | Test only           |
#

DOT_CONSTANTS = "...................................."


def format_adapter_message(name: str, base_msg: str, args: list[str]) -> str:
    # only apply formatting if there are arguments to format.
    # specific to adapters
    msg = base_msg if len(args) == 0 else base_msg.format(*args)
    return f"{name} adapter: {msg}"


@dataclass
class MainReportVersion(DebugLevel, proto_type.MainReportVersion):
    def code(self) -> str:
        return "A001"

    def message(self) -> str:
        return "Running with Visitran.."


@dataclass
class StarterProjectPath(DebugLevel, proto_type.StarterProjectPath):
    def code(self) -> str:
        return "A017"

    def message(self) -> str:
        return f"Starter project path: {self.dir}"


@dataclass
class ConfigFolderDirectory(InfoLevel, proto_type.ConfigFolderDirectory):
    def code(self) -> str:
        return "A018"

    def message(self) -> str:
        return f"Creating core configuration folder at {self.dir}"


@dataclass
class SettingUpProfile(InfoLevel, proto_type.SettingUpProfile):
    def code(self) -> str:
        return "A023"

    def message(self) -> str:
        return "Setting up your profile."


@dataclass
class InvalidProfileTemplateYAML(InfoLevel, proto_type.InvalidProfileTemplateYAML):
    def code(self) -> str:
        return "A024"

    def message(self) -> str:
        return "Invalid profile_template.yml in project."


@dataclass
class ProjectFileAlreadyExists(InfoLevel, proto_type.ProjectFileAlreadyExists):
    def code(self) -> str:
        return "A025"

    def message(self) -> str:
        return f"A project file called {self.name} already exists here."


@dataclass
class ProjectCreated(InfoLevel, proto_type.ProjectCreated):
    def code(self) -> str:
        return "A026"

    def message(self) -> str:
        return f"""
Your new core project "{self.project_name}" was created!

Happy modeling!
"""


@dataclass
class SeedEventIntiated(InfoLevel, proto_type.SeedEventIntiated):
    def code(self) -> str:
        return "A027"

    def message(self) -> str:
        return f"""Initiating Seed Task at "{self.seed_path}" """


@dataclass
class SeedFilesFound(DebugLevel, proto_type.SeedFilesFound):
    def code(self) -> str:
        return "A028"

    def message(self) -> str:
        formatted_string = (
            f"Seed path: {self.sf_path}\nDatabase Type: {self.database_type}\nSchema Name: {self.schema_name}\n"
        )
        return f"""Executing Seed File : {formatted_string} """


@dataclass
class SnapshotEventIntiated(InfoLevel, proto_type.SnapshotEventIntiated):
    def code(self) -> str:
        return "A029"

    def message(self) -> str:
        return f"""Initiating Snapshot Task at "{self.snp_path}" """


@dataclass
class SnapshotFilesFound(DebugLevel, proto_type.SnapshotFilesFound):
    def code(self) -> str:
        return "A030"

    def message(self) -> str:
        return f"""Snapshot file found in path : "{self.snp_path}" """


@dataclass
class IncludesExcludesList(DebugLevel, proto_type.IncludesExcludesList):
    def code(self) -> str:
        return "A031"

    def message(self) -> str:
        return f"""Included : "{self.includes}", Excluded : "{self.excludes}" """


@dataclass
class IncludeExcludeError(ErrorLevel, proto_type.IncludeExcludeError):
    def code(self) -> str:
        return "A032"

    def message(self) -> str:
        return f"""Included : "{self.includes}", Excluded : "{self.excludes}" """


# =======================================================
# E - DB Adapter
# =======================================================


@dataclass
class AdapterEventDebug(DebugLevel, proto_type.AdapterEventDebug):
    def code(self) -> str:
        return "E001"

    def message(self) -> str:
        return format_adapter_message(self.name, self.base_msg, self.args)


@dataclass
class AdapterEventInfo(InfoLevel, proto_type.AdapterEventInfo):
    def code(self) -> str:
        return "E002"

    def message(self) -> str:
        return format_adapter_message(self.name, self.base_msg, self.args)


@dataclass
class AdapterEventWarning(WarnLevel, proto_type.AdapterEventWarning):
    def code(self) -> str:
        return "E003"

    def message(self) -> str:
        return format_adapter_message(self.name, self.base_msg, self.args)


@dataclass
class AdapterEventError(ErrorLevel, proto_type.AdapterEventError):
    def code(self) -> str:
        return "E004"

    def message(self) -> str:
        return format_adapter_message(self.name, self.base_msg, self.args)


@dataclass
class AdapterType(InfoLevel, proto_type.AdapterType):
    def code(self) -> str:
        return "E005"

    def message(self) -> str:
        return f"database name: {self.database_name}"


# =======================================================
# I - Project parsing
# =======================================================


@dataclass
class SqlExecutionCompleted(InfoLevel, proto_type.SqlExecutionCompleted):
    def code(self) -> str:
        return "I001"

    def message(self) -> str:
        return "SQL Statements Execution Successful For Adaptors"


@dataclass
class IntiatingRunExecution(InfoLevel, proto_type.IntiatingRunExecution):
    def code(self) -> str:
        return "I002"

    def message(self) -> str:
        return f"""Initating Run in the Model path "{self.model_path}" """


@dataclass
class FoundModels(DebugLevel, proto_type.FoundModels):
    def code(self) -> str:
        return "I003"

    def message(self) -> str:
        return f"""Python model file found in path "{self.mf}" """


@dataclass
class ImportModelsFailed(ErrorLevel, proto_type.ImportModelsFailed):
    def code(self) -> str:
        return "I004"

    def message(self) -> str:
        return f"""Import of Dependency Module "{self.file_name}" Failed."""


@dataclass
class ProcessingModel(InfoLevel, proto_type.ProcessingModel):
    def code(self) -> str:
        return "I005"

    def message(self) -> str:
        return f"""Processing Model : "{self.cls}". """


@dataclass
class ModelAlreadyProcessed(InfoLevel, proto_type.ModelAlreadyProcessed):
    def code(self) -> str:
        return "I006"

    def message(self) -> str:
        return f"""Model Already Processed. Ignoring : "{self.cls}". """


@dataclass
class ProcessingDAG(DebugLevel, proto_type.ProcessingDAG):
    def code(self) -> str:
        return "I007"

    def message(self) -> str:
        return f"""Processing DAG for: "{self.dest_table}". """


@dataclass
class FoundBaseClass(DebugLevel, proto_type.FoundBaseClass):
    def code(self) -> str:
        return "I008"

    def message(self) -> str:
        return f"""Found base classes: "{self.bases}". """


@dataclass
class ObjectForClassNotFound(ErrorLevel, proto_type.ObjectForClassNotFound):
    def code(self) -> str:
        return "I009"

    def message(self) -> str:
        return f"""Could not find Object in objects list: "{self.values}" \
        for base: "{self.base}" """


@dataclass
class ConfigurationFileNotFound(ErrorLevel, proto_type.ConfigurationFileNotFound):
    def code(self) -> str:
        return "I010"

    def message(self) -> str:
        return f"""{self.file} yaml file not found, please run from a core project directory."""


@dataclass
class ExecutingModelNode(InfoLevel, proto_type.ExecutingModelNode):
    def code(self) -> str:
        return "I010"

    def message(self) -> str:
        formatted_string = (
            f"Database: {self.database}\n"
            f"Database Type: {self.database_type}\n"
            f"Destination Schema Name: {self.destination_schema_name}\n"
            f"Destination Table Exists: {self.destination_table_exists}\n"
            f"Destination Table Name: {self.destination_table_name}\n"
            f"Destination Table Object: {self.destination_table_obj}\n"
            f"Materialization: {self.materialization}\n"
            f"Select Statement: {self.select_statement}\n"
            f"Source Schema Name: {self.source_schema_name}\n"
            f"Source Table Name: {self.source_table_name}\n"
            f"Source Table Object: {self.source_table_obj}\n"
        )
        return f"""Executing Model Node: {formatted_string} """


@dataclass
class NodeExecutionError(ErrorLevel, proto_type.NodeExecutionError):
    def code(self) -> str:
        return "I011"

    def message(self) -> str:
        return f"""Error executing node: "{self.dest_table}". Trace : "{self.err}" """


@dataclass
class ExecutingTests(InfoLevel, proto_type.ExecutingTests):
    def code(self) -> str:
        return "I012"

    def message(self) -> str:
        formatted_string = (
            f"Test Name: {self.test_name}\n"
            f"Test Path: {self.test_path}\n"
            f"Database: {self.database}\n"
            f"Database Type: {self.database_type}\n"
            f"Connection String: {self.connection_string}\n"
            f"Destination Schema Name: {self.destination_schema_name}\n"
            f"Destination Table Name: {self.destination_table_name}\n"
            f"Materialization: {self.materialization}\n"
            f"Source Schema Name: {self.source_schema_name}\n"
            f"Source Table Name: {self.source_table_name}\n"
        )
        return f"""Executing Test: {formatted_string} """


@dataclass
class FoundTestModels(DebugLevel, proto_type.FoundTestModels):
    def code(self) -> str:
        return "I013"

    def message(self) -> str:
        return f"""Python Test model file found in path "{self.test_path}" """


@dataclass
class TestExecutionCompleted(InfoLevel, proto_type.TestExecutionCompleted):
    def code(self) -> str:
        return "I014"

    def message(self) -> str:
        return "Test Execution Completed Successfully."


@dataclass
class TestExecutionFailed(ErrorLevel, proto_type.TestExecutionFailed):
    def code(self) -> str:
        return "I015"

    def message(self) -> str:
        return f"Test execution failed: {self.err}"


@dataclass
class ExecutingSnapshotNode(InfoLevel, proto_type.ExecutingSnapshotNode):
    def code(self) -> str:
        return "I016"

    def message(self) -> str:
        formatted_string = (
            f"Database: {self.database}\n"
            f"Database Type: {self.database_type}\n"
            f"Source Table Name: {self.source_table_name}\n"
            f"Source Schema Name: {self.source_schema_name}\n"
            f"Snapshot Table Name: {self.snapshot_table_name}\n"
            f"Snapshot Schema Name: {self.snapshot_schema_name}\n"
            f"Unique Key: {self.unique_key}\n"
            f"Strategy: {self.strategy}\n"
            f"Updated At: {self.updated_at}\n"
            f"Check Columns: {self.check_cols}\n"
            f"Invalidate Hard Deletes: {self.invalidate_hard_deletes}\n"
        )
        return f"""Executing Snapshot node: {formatted_string}"""


# =======================================================
# Z - Misc
# =======================================================


@dataclass
class Formatting(InfoLevel, proto_type.Formatting):
    def code(self) -> str:
        return "Z017"

    def message(self) -> str:
        return self.msg


@dataclass
class SeedExecutionCompleted(InfoLevel, proto_type.SeedExecutionCompleted):
    def code(self) -> str:
        return "Z002"

    def message(self) -> str:
        return "Seed Execution Completed!"


@dataclass
class SortedDAGNodes(DebugLevel, proto_type.SortedDAGNodes):
    def code(self) -> str:
        return "Z003"

    def message(self) -> str:
        return f"""Sorted DAG nodes: {self.sorted_dag_nodes}" """


@dataclass
class FoundModelClass(DebugLevel, proto_type.FoundModelClass):
    def code(self) -> str:
        return "Z005"

    def message(self) -> str:
        return f"""Found Class: {self.cls_str}" """


@dataclass
class FoundModelSubClass(DebugLevel, proto_type.FoundModelSubClass):
    def code(self) -> str:
        return "Z006"

    def message(self) -> str:
        return f"""Found VisitranModel SubClass: {self.cls_str}" """


@dataclass
class MaterializationType(DebugLevel, proto_type.MaterializationType):
    def code(self) -> str:
        return "Z007"

    def message(self) -> str:
        return f"""Materialization Type: "{self.materialization}" """


@dataclass
class SnapshotExecutionCompleted(InfoLevel, proto_type.SnapshotExecutionCompleted):
    def code(self) -> str:
        return "Z008"

    def message(self) -> str:
        return "Snapshot Execution Completed!"


@dataclass
class UsingCachedObject(InfoLevel, proto_type.UsingCachedObject):
    def code(self) -> str:
        return "Z009"

    def message(self) -> str:
        return f"""Using Cached Object of {self.objname}: "{self.is_cached}" """


@dataclass
class ReadConnectionDetails(InfoLevel, proto_type.ReadConnectionDetails):
    def code(self) -> str:
        return "Z010"

    def message(self) -> str:
        return f"""Connection Details for {self.dbname}: {self.details} """


@dataclass
class ExecutingQuery(InfoLevel, proto_type.ExecutingQuery):
    def code(self) -> str:
        return "Z011"

    def message(self) -> str:
        return f"""Executing Query: {self.query} """


@dataclass
class SetExpiration(InfoLevel, proto_type.SetExpiration):
    def code(self) -> str:
        return "Z012"

    def message(self) -> str:
        return f"""Setting Expiration for {self.schema_name}: {self.table_name} \
expire in {self.expiration} hour"""


@dataclass
class GetTableObject(InfoLevel, proto_type.GetTableObject):
    def code(self) -> str:
        return "Z013"

    def message(self) -> str:
        return f"""Getting Table Object for {self.schema_name}: {self.table_name} """


@dataclass
class GetTableColumns(InfoLevel, proto_type.GetTableColumns):
    def code(self) -> str:
        return "Z014"

    def message(self) -> str:
        return f"""Getting Table Coloumns for {self.schema_name} - {self.table_name}:\
 {self.columns} """


@dataclass
class TableExists(InfoLevel, proto_type.TableExists):
    def code(self) -> str:
        return "Z015"

    def message(self) -> str:
        return f"""Table Exists for {self.schema_name}: {self.table_name}:\
 {self.exists} """


@dataclass
class ListAllTables(InfoLevel, proto_type.ListAllTables):
    def code(self) -> str:
        return "Z016"

    def message(self) -> str:
        return f"""List of all tables in {self.schema_name}: {self.tables} """


@dataclass
class GetRowCount(InfoLevel, proto_type.GetRowCount):
    def code(self) -> str:
        return "Z017"

    def message(self) -> str:
        return f"""Getting row count for {self.schema_name}: {self.table_name} \
row count: {self.row_count}"""


@dataclass
class GetTableRecords(InfoLevel, proto_type.GetTableRecords):
    def code(self) -> str:
        return "Z018"

    def message(self) -> str:
        return f"""Getting records for {self.schema_name}: {self.table_name}"""


@dataclass
class InsertInToTable(InfoLevel, proto_type.InsertInToTable):
    def code(self) -> str:
        return "Z019"

    def message(self) -> str:
        return f"""Inserting records in to {self.schema_name}: {self.table_name}"""


@dataclass
class MergeInToTable(InfoLevel, proto_type.MergeInToTable):
    def code(self) -> str:
        return "Z020"

    def message(self) -> str:
        return f"""Merging records in to {self.schema_name}: {self.table_name}:\
 temp_table_name:{self.temp_table_name}"""


@dataclass
class ExecuteEphemeral(InfoLevel, proto_type.ExecuteEphemeral):
    def code(self) -> str:
        return "Z021"

    def message(self) -> str:
        return f"""Executing Ephemeral Query on: {self.schema_name}.{self.table_name}"""


@dataclass
class ExecuteTable(InfoLevel, proto_type.ExecuteTable):
    def code(self) -> str:
        return "Z022"

    def message(self) -> str:
        return f"""Executing Table Query on: {self.schema_name}.{self.table_name}"""


@dataclass
class ExecuteView(InfoLevel, proto_type.ExecuteView):
    def code(self) -> str:
        return "Z023"

    def message(self) -> str:
        return f"""Executing View Query on: {self.schema_name}.{self.table_name}"""


@dataclass
class ExecuteIncrementalCreate(InfoLevel, proto_type.ExecuteIncrementalCreate):
    def code(self) -> str:
        return "Z024"

    def message(self) -> str:
        return f"""Executing create Query on: {self.schema_name}.{self.table_name}"""


@dataclass
class ExecuteIncrementalUpdate(InfoLevel, proto_type.ExecuteIncrementalUpdate):
    def code(self) -> str:
        return "Z025"

    def message(self) -> str:
        return f"""Executing update Query on: {self.schema_name}.{self.table_name}"""


@dataclass
class DoesNotExistError(InfoLevel, proto_type.DoesNotExistError):
    def code(self) -> str:
        return "Z026"

    def message(self) -> str:
        return f"""{self.object_name} does not exist"""


@dataclass
class NotSupportedError(InfoLevel, proto_type.NotSupportedError):
    def code(self) -> str:
        return "Z027"

    def message(self) -> str:
        return f"""{self.action} is not supported in {self.connector} connector"""


@dataclass
class EmptyFileError(InfoLevel, proto_type.EmptyFileError):
    def code(self) -> str:
        return "Z028"

    def message(self) -> str:
        return f"""The file {self.file} is empty"""


@dataclass
class HyphenDebugger(DebugLevel, proto_type.HyphenDebugger):
    def code(self) -> str:
        return "Z029"

    def message(self) -> str:
        return "--------"


@dataclass
class ListOfInstalledAdapters(DebugLevel, proto_type.ListOfInstalledAdapters):
    def code(self) -> str:
        return "Z030"

    def message(self) -> str:
        return f"""list of installed adapters: {self.db_list}"""


@dataclass
class SkipExecution(InfoLevel, proto_type.SkipExecution):
    def code(self) -> str:
        return "Z031"

    def message(self) -> str:
        return f"""Skipping execution of {self.filename}"""


@dataclass
class SummaryReport(InfoLevel, proto_type.SummaryReport):
    def code(self) -> str:
        return "Z032"

    def message(self) -> str:
        return f"""{self.sequence_num} of {self.total_count} {self.status} \
            {self.node_name} {DOT_CONSTANTS} [ {self.end_status} ]"""


@dataclass
class SeedReport(InfoLevel, proto_type.SeedReport):
    def code(self) -> str:
        return "Z033"

    def message(self) -> str:
        return f"""Seed in {self.seed_path} schema : {self.schema_name} {DOT_CONSTANTS} [ {self.status} ]"""


@dataclass
class SnapshotReport(InfoLevel, proto_type.SnapshotReport):
    def code(self) -> str:
        return "Z034"

    def message(self) -> str:
        return f"""Snapshot {self.unique_key} of {self.source_table} {DOT_CONSTANTS} [ {self.status} ]"""


@dataclass
class EndSummaryReportCounts(InfoLevel, proto_type.EndSummaryReportCounts):
    def code(self) -> str:
        return "Z035"

    def message(self) -> str:
        return f"""DONE PASS={self.pass_count} WARN={self.warn_count}\
              ERROR={self.error_count} SKIP={self.skip_count} TOTAL={self.total_count}"""


@dataclass
class ProjectAlreadyExistsInProfile(ErrorLevel, proto_type.ProjectAlreadyExistsInProfile):
    def code(self) -> str:
        return "Z036"

    def message(self) -> str:
        return f"""project name: {self.project_name} already \
            exists in profile:{self.profile_name}"""


@dataclass
class ProjectAlreadyExistsOrNoPermission(ErrorLevel, proto_type.ProjectAlreadyExistsOrNoPermission):
    def code(self) -> str:
        return "Z037"

    def message(self) -> str:
        return f"""project name: {self.project_name} already \
            exists in path:{self.project_path}, check for permissions too"""


@dataclass
class SeedExecutionError(ErrorLevel, proto_type.SeedExecutionError):
    def code(self) -> str:
        return "Z038"

    def message(self) -> str:
        return f"Seed Execution failed for file: {self.file_name} with error: {self.err}"


@dataclass
class BulkExecuteError(ErrorLevel, proto_type.BulkExecuteError):
    def code(self) -> str:
        return "Z039"

    def message(self) -> str:
        return f"Transaction failed for query: {self.query} with error: {self.err}"


@dataclass
class InitiateScheduling(InfoLevel, proto_type.InitiateScheduling):

    def code(self) -> str:
        return "S040"

    def message(self) -> str:
        return f"Scheduling new task of type {self.task_type}. task details: {self.cron_data}"


@dataclass
class CronJobScheduled(InfoLevel, proto_type.CronJobScheduled):
    def code(self) -> str:
        return "S041"

    def message(self) -> str:
        return f"{self.task_type} Task Scheduled, ID: {self.task_id}. task details: {self.cron_data}"


@dataclass
class UpdateCronJob(InfoLevel, proto_type.UpdateCronJob):
    def code(self) -> str:
        return "S042"

    def message(self) -> str:
        return f"{self.task_type} Task updated, task_ID: {self.task_id}. task details: {self.cron_data}"


@dataclass
class ListScheduledJobs(InfoLevel, proto_type.ListScheduledJobs):
    def code(self) -> str:
        return "S043"

    def message(self) -> str:
        return f"Listing scheduled task {self.tasks}"


@dataclass
class DeleteScheduledJob(InfoLevel, proto_type.DeleteScheduledJob):
    def code(self) -> str:
        return "S044"

    def message(self) -> str:
        return f"Scheduled task got deleted successfully. task_id: {self.task_id}"


@dataclass
class FailedDeleteScheduledJob(ErrorLevel, proto_type.FailedDeleteScheduledJob):
    def code(self) -> str:
        return "S045"

    def message(self) -> str:
        return f"Failed to delete Scheduled task. task_id: {self.task_id}"


@dataclass
class FailedScheduleJob(ErrorLevel, proto_type.FailedScheduleJob):
    def code(self) -> str:
        return "S046"

    def message(self) -> str:
        return f"Failed to Scheduled the job for project id {self.project_id}"


@dataclass
class UpdateFailedCronJob(ErrorLevel, proto_type.UpdateFailedCronJob):
    def code(self) -> str:
        return "S047"

    def message(self) -> str:
        return f"{self.task_type} Task update failed, task_ID: {self.task_id}. task details: {self.cron_data}"
