from visitran.constants import BaseConstant


class BackendSuccessMessages(BaseConstant):
    """Success messages for API responses"""

    # Project Sharing Success Messages
    PROJECT_SHARED_WITH_ORG = (
        "**Shared with Organization!**\n"
        "Project is now shared with the entire organization as {role}."
    )
    PROJECT_SHARED_WITH_USERS = (
        "**Project Shared!**\n"
        "Successfully shared with {success_count} user(s)."
    )
    PROJECT_USER_ROLE_UPDATED = (
        "**Role Updated!**\n"
        "User role has been updated to {role}."
    )
    PROJECT_USER_ACCESS_REVOKED = (
        "**Access Revoked!**\n"
        "User access has been removed from this project."
    )
    PROJECT_ORG_SHARE_REMOVED = (
        "**Organization Sharing Removed!**\n"
        "This project is no longer shared with the entire organization."
    )

    # AI Context Rules Success Messages
    AI_CONTEXT_RULES_PERSONAL_UPDATED = (
        "**Personal Rules Updated!**\n"
        "Your personal AI context rules have been saved successfully and will apply to all future conversations."
    )
    
    AI_CONTEXT_RULES_PROJECT_UPDATED = (
        "**Project Rules Updated!**\n"
        "Project AI context rules have been saved successfully and are now shared with all team members."
    )
    
    AI_CONTEXT_RULES_PERSONAL_RETRIEVED = (
        "**Personal Rules Retrieved!**\n"
        "Your personal AI context rules have been loaded successfully."
    )
    
    AI_CONTEXT_RULES_PROJECT_RETRIEVED = (
        "**Project Rules Retrieved!**\n"
        "Project AI context rules have been loaded successfully."
    )


class BackendErrorMessages(BaseConstant):
    """Error messages used in exceptions !!!"""

    DEFAULT_ERROR_MSG = "**Application Error!**\nOops! Something went wrong. Please check and try again."

    CORE_EXCEPTIONS = '**Action Failed!**\nUnable to perform action: "{error_message}". Please try again.'

    PROJECT_NOT_EXISTS = (
        '**Project Not Found!**\nThe project with ID "{project_id}" doesn\'t exist. '
        "Check permissions or try another project."
    )

    PROJECT_NAME_RESERVED = (
        "**Reserved Name!**\nProject name '{project_name}' is reserved and cannot be used.\n"
        "Please choose a different name."
    )

    PROJECT_ALREADY_EXISTS = (
        '**Project Exists!**\nProject "{project_name}" already created at {created_at}.\n'
        "Choose a unique name or delete the existing one."
    )

    CONNECTION_ALREADY_EXISTS = (
        '**Connection Exists!**\nConnection "{connection_name}" already created at {created_at}.\n'
        "Choose a unique name or delete the existing one."
    )

    CONNECTION_NOT_EXISTS = (
        '**Connection Not Accessible!**\nConnection "{connection_id}" is not reachable. '
        "Verify the connection ID and retry."
    )

    CONNECTION_DEPENDENCY = (
        "### **Connection Dependency!**"
        '\nConnection "{connection_name}" with id - "{connection_id}" is used in active projects i.e.,'
        '**"{affected_projects}"**. \n'
        "\nPlease delete the projects or **ask for a feature to modify the connections in projects** and retry."
    )

    MODEL_ALREADY_EXISTS = (
        '**Model Exists!**\nModel "{model_name}" already created at {created_at}. '
        "Choose a unique name or delete the existing one."
    )

    MODEL_NOT_EXISTS = (
        '**Model Not Accessible!**\nThe model "{model_name}" cannot be accessed. '
        "Verify the model name and try again."
    )

    CSV_RENAME_FAILED = (
        "**Seed Rename Failed**\n"
        'Seed file "{csv_name}" failed to rename\n'
        "Try to upload renamed seed file if issue still persist!"
    )
    CSV_FILE_ALREADY_EXISTS = (
        '**CSV File Exists!**\nCSV file "{csv_name}", is already created at {created_at}. \n'
        "Use a unique name or delete the existing file."
    )

    CSV_FILE_NOT_UPLOADED = '**CSV UPLOAD ERROR!**\nCSV file "{csv_name}" was not uploaded due to\n {reason}.\n Try re-uploading the CSV again.'

    CSV_FILE_NOT_EXISTS = '**CSV File Missing!**\nCSV file "{csv_name}" not found. \nVerify the name and retry.'

    INVALID_USER = "**Access Denied!**\nYou're not authorized to access this resource. Contact admin if required."

    BACKUP_MODEL_NOT_EXIST = '**Backup Missing!**\nNo backup found for "{model_name}". Restoration is not possible.'

    ENVIRONMENT_NOT_EXIST = (
        '**Environment Not Accessible!**\nEnvironment "{environment_id}" isn\'t accessible. '
        "Verify the ID and try again."
    )

    ENVIRONMENT_ALREADY_EXIST = (
        '**Environment Exists!**\nEnvironment "{env_name}" already created at {created_at}. '
        "Use a unique name or delete the existing one."
    )

    SAMPLE_PROJECT_CONNECTION_FAILED = "**Connection Failed!**\nDatabase connection failed for the sample project.\n Check settings and retry."
    SAMPLE_PROJECT_LIMIT_EXCEED = (
        "**Limit Exceed**\n"
        "{project_base_name} : Sample Project creation failed due to limit exceed\n"
        "Current Sample project: {sample_project_count}\n"
        "Limit: {sample_project_limit}"
    )
    RESOURCE_PERMISSION_DENIED = (
        "**Resource Permission Denied**\n"
        "Requested resource have limited permission for current user or user's role\n"
        "Please contact organization's admin to change suitable role."
    )

    TABLE_CONTENT_ISSUE = (
        "**Data Fetch Failed!**\n"
        'Unable to fetch the records from the table: "{table_name}",\n '
        "The error message - {reason}"
    )

    SOURCE_TABLE_NOT_EXIST = (
        "### **Source Table Missing!**\n"
        "The database does not contain the specified source table configuration.\n"
        "Model: **{model_name}**\n"
        "Schema: **{schema_name}**\n"
        "Table: **{table_name}**\n\n"
        "Please verify the schema name and table name, and try again."
    )

    INVALID_SOURCE_TABLE = (
        "**Source Table Name Invalid!**\n"
        'The source table name: "{table_name}" configured in the model is invalid.\n'
        "Verify the name and retry."
    )

    INVALID_DESTINATION_TABLE = (
        "**Destination Table Name Invalid!**\n"
        'The destination table name: "{table_name}" configured in the model is invalid.\n'
        "Verify the name and retry."
    )

    DESTINATION_TABLE_ALREADY_EXIST = (
        "### **Table Conflict!**\n"
        'The destination table **"{schema_name}".'
        '"{table_name}"** specified in the model **"{current_model_name}"** '
        'is already being used in another model **"{conflicting_model_name}"**.\n\n'
        "ℹ️ This conflict will cause one model to overwrite the transformations of the other.\n\n"
        'Please update the destination table name in **"{current_model_name}"** or **"{conflicting_model_name}"** to resolve the conflict.'
    )

    JOIN_TABLE_NOT_EXIST = (
        '**Join Table Missing!**\nTable "{table_name}" not found for join in model "{model_name}". \n'
        "Update join configuration and retry."
    )

    MERGE_TABLE_NOT_EXIST = (
        '**Merge Table Missing!**\nTable "{table_name}" not found for merge in model "{model_name}". \n '
        "Update merge configuration and retry."
    )

    COLUMN_DEPENDENCY = (
        "### **Column Dependency Conflict !**\n\n"
        "In the **{model_name}** model,\n\n"
        "While updating **{transformation_name}** transformation, the **\"{affected_columns}\"** column is affected and "
        "it is currently being used in the **\"{affected_transformation}\"** transformation.\n\n"
        "**Action Required:** Please validate your current **{transformation_name}** transformation and retry."
    )

    MULTIPLE_COLUMN_DEPENDENCY = (
        "### **Multiple Columns Dependency Conflict !**\n\n"
        "In the **{model_name}** model,\n\n"
        "While updating **{transformation_name}** transformation, **{affected_columns_count}** column(s) are affected:\n"
        "**{affected_columns}**\n\n"
        "These columns are currently being used in the following transformations:\n\n"
        "{dependency_details}\n\n"
        "**Action Required:** Please review the dependent transformations above and "
        "update your **{transformation_name}** transformation accordingly before proceeding."
    )

    TRANSFORMATION_CONFLICT = (
        "### **Transformation Conflict !**\n"
        "From **{model_name}** model, Transformation cannot be applied after a **{transformation_name}** "
        "transformation.\n"
        "Once pivot is performed, rows and columns can be transposed, "
        "which prevents the **{affected_transformation}** transformation.\n"
        "Columns **{affected_columns}** are used in **{affected_transformation}**\n\n"
        "To proceed with **{affected_transformation}** transformation, please create a child model or remove the "
        "columns used in **{affected_transformation}** transformation."
    )

    MODEL_DEPENDENCY = (
        '**Model Deletion Blocked!**\nCannot delete "{model_name}" as it\'s referenced in: {child_models}. \n '
        "Remove references first."
    )

    MODEL_TABLE_DEPENDENCY = (
        "### **Model Table Dependency Conflict !**\n\n"
        "In the **{model_name}** model,\n\n"
        "The configured model table **\"{table_name}\"** is currently referenced in the following child model(s):\n"
        "**{child_models}**\n\n"
        "**Action Required:** Remove or update these references in the child models before changing or removing "
        "the model."
    )

    PROJECT_DEPENDENCY = (
        '**Project Deletion Blocked**\n'
        'The project ”{project_name}” cannot be deleted while {job_count} associated jobs exist.\n'
        'Please delete related jobs before attempting to delete the project'
    )

    INVALID_MATERIALIZATION = (
        '**Invalid Materialization Type!**\nThe materialization type in the model is: "{materialization}" \n '
        "Choose from: {supported_materializations} and retry"
    )

    REFERENCE_MODEL_NOT_FOUND = (
        "**Reference Model Missing!**\nThe reference models used - {missing_references} are not found. \n "
        "Update reference and retry."
    )

    INVALID_MODEL_CONFIGURATION_DATA = (
        "**Invalid Model Configuration!**\n"
        "The model configuration payload is invalid. \n"
        "Reason - {failure_reason} \n"
        "Please review and try again."
    )

    INVALID_MODEL_REFERENCE_DATA = "**Invalid Status Reference Type,{failure_reason}."

    CIRCULAR_DEPENDENCY_IN_REFERENCES = '**Circular Reference!**\nCircular dependency detected in "{model_name}". Path: {traversed_path}.'

    CHAT_NOT_FOUND = '**Chat Not Found!**\nChat with ID "{chat_id}" doesn\'t exist. Verify the ID and retry.'

    CHAT_MESSAGE_NOT_FOUND = (
        '**Chat Message Missing!**\nMessage ID "{chat_message_id}" not found in chat "{chat_name}" \n '
        '(Chat ID: "{chat_id}"). Verify and retry.'
    )

    INVALID_CHAT_PROMPT = "**Invalid Prompt!**\nThe chat prompt is invalid. Double-check and retry."

    FEEDBACK_SUBMISSION_FAILED = (
        "**Feedback Error!**\nCouldn't save feedback for message ID \"{chat_message_id}\". Please try again."
    )
    
    ORGANIZATION_REQUIRED = "**Organization Required!**\nOrganization ID is required for this operation."
    
    INVALID_FEEDBACK_FORMAT = (
        "**Invalid Feedback!**\nFeedback format is invalid. Use 'P' for positive, 'N' for negative, or '0' for neutral."
    )
    
    FEEDBACK_RETRIEVAL_FAILED = (
        "**Feedback Retrieval Failed!**\nUnable to retrieve feedback for message ID \"{chat_message_id}\". Please try again."
    )
    
    INVALID_CHAT_MESSAGE_STATUS = (
        '**Invalid Status!**\nStatus "{invalid_status}" is invalid. Valid statuses: {valid_status}.'
    )


    INVALID_SQL_QUERY = "**SQL Query Error**\nThe SQL query is invalid. Please review the syntax and try again."

    PROHIBITED_QUERY = (
        '**Prohibited SQL Query**\nThe query attempted to perform "{prohibited_action}", which is not allowed.\n'
        'The following actions are prohibited: "{prohibited_actions}"'
    )

    SQL_EXTRACTION_FAILED = "**SQL Query Extraction Failed**\nUnable to extract SQL query from the provided text."
    
    LLM_SERVER_FAILURE = (
        "**AI Server Error!**\n"
        "Failed while answering your prompt \n "
        'The error message - "{error_message}"'
    )

    AIRaisedException = (
        "{error_message}"
    )

    SCHEDULE_JOB_FAILURE = (
        "**Scheduled Job Error!**\n"
        "Failed to scheduled job \n "
        "The error message - {error_message}"
    )

    SCHEMA_MISSING_IN_SEED_UPLOAD = (
        "**Missing Schema**\n Schema is missing while uploading seed file."
    )

    CSV_DOWNLOAD_FAILED = (
        "**CSV Download Failed!**\n"
        'Unable to download CSV data from table: "{table_name}".\n'
        "Error details: {reason}\n"
        "Please try again or contact support if the issue persists."
    )

    SCHEMA_NOT_FOUND = '**Schema Not Found**\nSchema is not found in project {project_name}".\nVerify that the schema exists in project, environment or connecction.'

    # AI Context Rules Error Messages
    AI_CONTEXT_RULES_FETCH_FAILED = (
        "**Context Rules Fetch Failed!**\n"
        "Unable to retrieve AI context rules. Please try again or contact support if the issue persists."
    )
    
    AI_CONTEXT_RULES_UPDATE_FAILED = (
        "**Context Rules Update Failed!**\n"
        "Failed to save AI context rules. Please verify your input and try again."
    )
    
    AI_CONTEXT_RULES_INVALID_PROJECT = (
        '**Invalid Project!**\nProject with ID "{project_id}" not found or you don\'t have access to it. '
        "Verify the project ID and your permissions."
    )
    
    AI_CONTEXT_RULES_PERMISSION_DENIED = (
        "**Permission Denied!**\n"
        "You don't have permission to modify AI context rules for this project. "
        "Contact your project administrator for access."
    )
    
    AI_CONTEXT_RULES_INVALID_INPUT = (
        "**Invalid Input!**\n"
        "The context rules format is invalid. Please check your input and try again."
    )

    # Project Connection Error Messages
    PROJECT_CONNECTION_GET_FAILED = (
        "**Connection Retrieval Failed!**\n"
        "Unable to retrieve connection details for project \"{project_id}\".\n"
        "Error: {error_message}"
    )

    PROJECT_CONNECTION_UPDATE_FAILED = (
        "**Connection Update Failed!**\n"
        "Unable to update connection details for project \"{project_id}\".\n"
        "Error: {error_message}"
    )

    PROJECT_CONNECTION_TEST_FAILED = (
        "**Connection Test Failed!**\n"
        "Unable to test connection for project \"{project_id}\".\n"
        "Error: {error_message}"
    )

    PROJECT_CONNECTION_MISSING_FIELD = (
        "**Missing Required Field!**\n"
        "Required field \"{field_name}\" is missing from the request.\n"
        "Please provide all required fields and try again."
    )

    PROJECT_CONNECTION_INVALID_DATA = (
        "**Invalid Connection Data!**\n"
        "The provided connection data is invalid.\n"
        "Error: {error_message}"
    )

    # Project Sharing Error Messages
    PROJECT_SHARE_PERMISSION_DENIED = (
        "**Permission Denied!**\n"
        "You need Admin or Owner role on this project to manage sharing."
    )
    PROJECT_SHARE_INVALID_PAYLOAD = (
        "**Invalid Request!**\n"
        "Please provide either 'shares' (for users) or 'share_with_org' (for organization)."
    )
    PROJECT_SHARE_NOT_FOUND = (
        "**Share Not Found!**\n"
        "No sharing permission found for this user on this project."
    )
    PROJECT_SHARE_CANNOT_REVOKE = (
        "**Cannot Revoke!**\n"
        "Unable to revoke access. The user may be the project owner or not have direct access."
    )
    PROJECT_SHARE_ORG_NOT_SHARED = (
        "**Not Shared with Organization!**\n"
        "This project is not currently shared with the organization."
    )
    PROJECT_SHARE_USER_NOT_FOUND = (
        "**User Not Found!**\n"
        'User with ID "{user_id}" was not found.'
    )
    PROJECT_SHARE_USER_NOT_IN_ORG = (
        "**User Not in Organization!**\n"
        "The target user does not belong to the same organization as this project."
    )
    PROJECT_SHARE_CANNOT_SHARE_OWNER = (
        "**Cannot Share with Owner!**\n"
        "The project owner already has full access and cannot be added as a shared user."
    )

    # Version Control Error Messages
    VERSION_NOT_FOUND = (
        '**Version Not Found!**\nVersion {version_number} was not found in this project.'
    )
    COMMIT_FAILED = (
        '**Commit Failed!**\nFailed to create version for model "{model_name}". Please try again.'
    )
    VERSION_CONFLICT = (
        '**Version Conflict!**\nExpected version {expected_version} but current is {current_version}. '
        'Another user may have committed. Please refresh and retry.'
    )
    DUPLICATE_CONTENT_COMMIT = (
        '**No Changes!**\nContent is identical to version {existing_version}. Nothing to commit.'
    )
    CONCURRENT_MODIFICATION = (
        '**Concurrent Edit!**\nModel "{model_name}" was modified by another session. '
        'Please refresh and retry.'
    )
    NO_CHANGES_TO_COMMIT = (
        '**No Changes!**\nNo changes detected since the last version. Nothing to commit.'
    )
    GIT_CONNECTION_FAILED = (
        '**Git Connection Failed!**\n{error_message}'
    )
    GIT_PUSH_FAILED = (
        '**Git Push Failed!**\nFailed to push "{model_name}" to git.\n{error_message}'
    )
    GIT_RATE_LIMIT = (
        '**Rate Limited!**\nGitHub API rate limit exceeded. Please wait and try again.'
    )
    GIT_TOKEN_EXPIRED = (
        '**Token Expired!**\nGit authentication token has expired. Please update your credentials.'
    )
    UNSUPPORTED_GIT_PROVIDER = (
        '**Unsupported Provider!**\nGit provider for URL "{repo_url}" is not supported. '
        'Currently only GitHub is supported.'
    )
    GIT_CONFIG_NOT_FOUND = (
        '**Git Config Not Found!**\nNo git configuration found for project "{project_id}".'
    )
    GIT_CONFIG_ALREADY_EXISTS = (
        '**Git Config Exists!**\nProject "{project_id}" already has an active git configuration.'
    )

    # Token Balance Error Messages
    INSUFFICIENT_TOKEN_BALANCE = (
        "**Insufficient Token Balance!**\n"
        "You need **{tokens_required}** tokens for this operation, but only have **{tokens_available}** tokens remaining.\n\n"
        "Please purchase more tokens to continue using AI features."
    )

