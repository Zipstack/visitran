from visitran.constants import BaseConstant


class ErrorCodeConstants(BaseConstant):
    """Error messages used in exceptions !!!"""

    # Generic fallback error if the issue cause is unknown.
    DEFAULT_ERROR_MSG = (
        "**Unexpected Error**\nSomething went wrong. Please try again or contact support if the issue persists."
    )

    # Used when required fields for a snapshot are missing or invalid.
    INVALID_SNAPSHOT_FIELDS = '**Invalid Snapshot Configuration**\nThe snapshot configuration is invalid. The following fields are missing or invalid: "{invalid_fields}".\nReview your snapshot settings and ensure all required fields are correctly specified.'

    # Used when column names in a snapshot don't exist in the source table.
    INVALID_SNAPSHOT_COLUMNS = '**Invalid Snapshot Columns**\nThe specified columns - "{invalid_columns}" are invalid for snapshot - "{table_name}" in schema - "{schema_name}".\nPlease verify the column names against the table schema.'

    # When a specified model file is missing or not discoverable.
    MODEL_NOT_FOUND = '**Model Not Found**\nThe module "{module_name}" is not found in the configured path.\nEnsure the model exists and the path is correct.'

    # Raised during an exception while importing a model's Python file.
    MODEL_IMPORT_ERROR = '**Model Import Error**\nWhile importing the module "{model_name}", an error occurred: "{error_message}".\nCheck the module path or syntax in the model file.'

    # When a referenced table doesn't exist in the provided schema.
    TABLE_NOT_FOUND = '**Table Not Found**\nThe table "{table_name}" is not found in schema "{schema_name}".\nVerify that the table exists and is accessible.'

    # When uploaded CSV headers don't match required format.
    INVALID_CSV_HEADERS = '**Invalid CSV Headers**\nThe CSV file: "{file_name}" has invalid headers.\nCouldn\'t validate or convert the column name: "{column_name}". Try again with valid headers or rename to proper database-friendly column names.'

    # Raised when a SQL query fails during execution.
    QUERY_EXECUTION_FAILURE = '**Query Execution Failure**\nAn unexpected error occurred while executing the queries in connector: "{error}".\nPlease check your query or the connection configuration.'

    # When a Python module fails to load or import.
    UNABLE_TO_IMPORT = '**Import Error**\nThe module "{module_name}" could not be imported due to: "{error}".\nMake sure the file exists and is free of syntax errors.'

    # When a transformation fails during model execution.
    MODEL_EXECUTION_FAILED = '**Model Execution Failed**\nAn error occurred while executing the model - "{model_name}" with error: "{error_message}".\nCheck the transformation logic or input data.'

    # Raised when the SQL generated for transformation is invalid.
    SQL_TRANSFORMATION_QUERY = '**SQL Transformation Error**\nThe query generated for transformation - "{query_statements}" failed with error: "{error_message}".\nReview the SQL syntax or the referenced columns and tables.'

    # Used when expected objects are not found in the class registry.
    OBJECT_FOR_CLASS_NOT_FOUND = '**Object Not Found**\nCould not find object in list: "{values}" for base: "{base}".\nEnsure the object name is correct and exists in the source list.'

    # Conflict between include and exclude model lists.
    MODEL_INCLUDED_IS_EXCLUDED = '**Conflicting Model Inclusion**\nNode included by "{includes}" is also listed in the excluded list "{excludes}".\nPlease resolve the conflict between includes and excludes.'

    # When a relative path is used instead of absolute for Python imports.
    PYTHON_RELATIVE_PATH_ERROR = '**Invalid Python Path**\nThe path for python file - "{file_name}" is invalid.\nUse the full path instead of a relative path.'

    # When Postgres backend isn't installed but needed.
    POSTGRES_PACKAGE_MISSING = "**Missing Postgres Adapter**\nPlease install postgres adapter using `pip install ibis-framework[postgres]`.\nThis is required to connect with Postgres databases."

    # Trying to create a project with a name that already exists.
    PROJECT_NAME_ALREADY_EXISTS = '**Duplicate Project Name**\nProject name: "{project_name}" already exists in profile: "{profile_name}".\nChoose a different project name.'

    # When DB or connector connection fails.
    CONNECTION_FAILED_ERROR = '**Connection Failed**\nUnable to establish a connection with "{db_type}". Error: "{error_message}".\nCheck the server status or verify your connection details.'

    # When project creation fails due to invalid path or permissions.
    CANNOT_CREATE_PROJECT = '**Project Creation Failed**\nCannot create "{project_name}" in "{project_path}".\nVerify the path and permissions.'

    # When loading or parsing of a seed file fails.
    SEED_FILE_EXECUTION_ERROR = '**Seed File Error**\nThe file "{file_name}" failed due to: "{error_message}".\nCheck the contents and format of the seed file.'

    RUN_SEED_FILE_FAILED_ERROR = '**Seed Run Error**\nThe seed run for file "{file_name}" failed due to "{error_message}".\nPlease contact support if issue persists'

    # When expected column is not found during transformation.
    COLUMN_NOT_EXIST = '**Column Not Found**\nThe column "{column_name}" is missing during transformation - "{transformation_name}" in model - "{model_name}".\nEnsure the column exists in the source data.'

    # Used when a synthesis column is missing from model logic.
    SYNTHESIS_COLUMN_NOT_EXIST = '**Synthesis Column Missing**\nThe column "{column_name}" is missing during transformation - "{transformation_name}" in model - "{model_name}".\nCheck if the column was created or referenced properly.'

    # Generic failure for transformation execution.
    TRANSFORMATION_FAILED = '**Transformation Failed**\nThe transformation "{transformation_name}" in model - "{model_name}" failed due to "{error_message}".\nReview the transformation logic and input data.'

    SCHEMA_CREATION_PERMISSION_DENIED = (
        "**Permission Denied**\n"
        'Schema "{schema_name}" could not be created due to missing permissions.\n'
        "Check database access or create the schema manually."
    )

    SCHEMA_ALREADY_EXIST = '**Duplicate Schema**\n Schema "{schema_name}" already exists. Use a different name.'

    SCHEMA_CREATION_FAILED = (
        "**Schema Creation Failed**\n"
        'Unable to create schema "{schema_name}" — {error_message}.\n'
        "Try again or create the schema manually if the issue persists."
    )
    INVALID_CONNECTION_URL = (
        "**Invalid Connection URL**\n" "The connection URL is invalid. Please provide a valid connection string."
    )

    MISSING_REQUIRED_CONNECTION_FIELDS = (
        "**Missing Required Fields**\n"
        'The given connection is missing mandatory field(s): - "{missing_fields}"'
        "Please make sure all mandatory fields are filled"
    )

    # Direct execution errors
    IBIS_BUILD_ERROR = (
        "**Ibis Build Error**\n"
        "{message}\n"
        "Review the transformation SQL or column references."
    )

    DAG_EXECUTION_ERROR = (
        "**DAG Execution Error**\n"
        'Error executing model "{model_name}": {message}\n'
        "Check the model definition and its dependencies."
    )
