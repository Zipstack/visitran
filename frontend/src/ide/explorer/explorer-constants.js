import {
  FileAddOutlined,
  PlayCircleOutlined,
  DeleteOutlined,
  EditOutlined,
  FolderOpenOutlined,
  DatabaseOutlined,
} from "@ant-design/icons";

import {
  Folder,
  FolderTest,
  FolderModel,
  FolderPython,
  Python,
  Csv,
  FolderDB,
  FolderDocs,
  FolderConfig,
  Yaml,
  Readme,
  Document,
  NoCode,
  Lineage,
  LinearScale,
} from "../../base/icons";

const CONTEXT_MENU_LIST_MAPPING = {
  ROOT_DATABASE: [getMenuItem("Add database", "database_add")],
  FULL_CODE: [
    getMenuItem("New folder", "fc_new_folder"),
    getMenuItem("New model", "fc_new_model"),
  ],
  ROOT_TEST: [getMenuItem("Add test file", "test_new_file")],
  ROOT_DOCUMENTATION: [
    getMenuItem("Add markdown file", "documentation_new_file"),
  ],

  DATABASE: [getMenuItem("Delete", "database_delete")],
  TABLE: [getMenuItem("Show table structure", "table_structure_get")],

  NO_CODE_FOLDER: [
    getMenuItem("New model", "nc_new_model"),
    getMenuItem("Rename folder", "nc_rename_folder"),
    getMenuItem("Delete folder", "nc_delete_folder"),
  ],
  FULL_CODE_FOLDER: [
    getMenuItem("New model", "fc_new_model"),
    getMenuItem("Rename folder", "fc_rename_folder"),
    getMenuItem("Delete folder", "fc_delete_folder"),
  ],
  FULL_CODE_MODEL: [getMenuItem("Delete", "fc_delete_model")],

  SNAPSHOT: [getMenuItem("Delete", "snapshot_delete")],

  SEED_FOLDER: [
    getMenuItem("Add .csv file", "add_csv"),
    getMenuItem("Run seed", "seed_run"),
    getMenuItem("Rename folder", "seed_rename_folder"),
    getMenuItem("Delete folder", "seed_delete_folder"),
  ],
  // Right clicke Dropdowns
  NO_CODE: [
    // getMenuItem("New folder", "nc_new_folder"),
    getClickMenuItem("New model", "nc_new_model", <FileAddOutlined />),
  ],
  ROOT_SEED: [
    getClickMenuItem("Run seed", "seed_run", <PlayCircleOutlined />),
    getClickMenuItem("Add .csv file", "add_csv", <FileAddOutlined />),
  ],
  SEED_CSV_FILE: [
    getClickMenuItem("Run CSV", "seed_run_csv", <PlayCircleOutlined />),
    getClickMenuItem("Rename", "seed_rename_csv", <EditOutlined />),
    getClickMenuItem(
      <span className="red">Delete</span>,
      "seed_delete_csv",
      <DeleteOutlined className="red" />
    ),
  ],
  NO_CODE_MODEL: [
    // getMenuItem("New derived model", "nc_new_model"),
    getClickMenuItem("Run", "nc_run_model", <PlayCircleOutlined />),
    getClickMenuItem("Rename", "nc_rename_model", <EditOutlined />),
    getClickMenuItem(
      <span className="red">Delete</span>,
      "nc_delete_model",
      <DeleteOutlined className="red" />
    ),
  ],
};

const CONTEXT_MENU_KEY_VS_TYPE_MAPPING = {
  database_add: "DATABASE",
  nc_new_folder: "NO_CODE_FOLDER",
  nc_new_model: "NO_CODE_MODEL",
  fc_new_folder: "FULL_CODE_FOLDER",
  fc_new_model: "FULL_CODE_MODEL",
  seed_new_folder: "SEED_FOLDER",
  add_csv: "SEED_CSV_FILE",
  test_new_file: "TEST_FILE",
  documentation_new_file: "DOCUMENTATION_FILE",
  csv: "SEED_CSV_FILE1",
};

const EXPLORER_TAB_ITEMS = [
  { id: 1, label: "Models", icon: <FolderOpenOutlined />, key: "1" },
  { id: 2, label: "Database", icon: <DatabaseOutlined />, key: "2" },
  { id: 3, label: "Data Flow", icon: <LinearScale />, key: "3" },
];

const TYPE_VS_CONTEXT_MENU_KEY_MAPPING = {
  DATABASE: "database_add",
  NO_CODE_FOLDER: "nc_new_folder",
  NO_CODE_MODEL: "nc_new_model",
  FULL_CODE_FOLDER: "fc_new_folder",
  FULL_CODE_MODEL: "fc_new_model",
  SEED_FOLDER: "seed_new_folder",
  SEED_CSV_FILE: "add_csv",
  TEST_FILE: "test_new_file",
  DOCUMENTATION_FILE: "documentation_new_file",
  SEED_CSV_FILE1: "csv",
};

const TYPE_VS_ICON_MAPPING = {
  ROOT_DATABASE: <FolderDB />,
  ROOT_MODEL: <Folder />,
  ROOT_SNAPSHOT: <Folder />,
  ROOT_SEED: <Folder />,
  SEED_FOLDER: <Folder />,
  ROOT_TEST: <FolderTest />,
  ROOT_DOCUMENTATION: <FolderDocs />,
  ROOT_LOG_FOLDER: <FolderDocs />,
  ROOT_CONFIGURATION: <FolderConfig />,
  ROOT_README_FILE: <Readme />,
  ROOT_YAML_FILE: <Yaml />,
  NO_CODE: <FolderModel />,
  NO_CODE_FOLDER: <FolderModel />,
  NO_CODE_MODEL: <NoCode />,
  FULL_CODE: <FolderPython />,
  FULL_CODE_FOLDER: <FolderPython />,
  FULL_CODE_MODEL: <Python />,
  SEED_CSV_FILE: <Csv />,
  SEED_PYTHON_FILE: <Python />,
  YAML_FILE: <Yaml />,
  LINEAGE: <Lineage />,
};

const CONTEXT_MENU_KEY_VS_FILE_TYPE_MAPPING = {
  add_csv: ".csv, text/csv",
};

const TAB_VIEW_TYPES = [
  "ROOT_README_FILE",
  "ROOT_YAML_FILE",
  "NO_CODE_MODEL",
  "FULL_CODE_MODEL",
  "SEED_CSV_FILE",
  "SEED_PYTHON_FILE",
  "YAML_FILE",
  "ROOT_DB",
  "LINEAGE",
  "SQL_FLOW",
];

function getIconByType(type) {
  return TYPE_VS_ICON_MAPPING[type] || <Document />;
}

function getAllowedFileByKey(key) {
  return CONTEXT_MENU_KEY_VS_FILE_TYPE_MAPPING[key] || "";
}

function getMenuItem(label, key, children, type) {
  return {
    label,
    key,
    children, // for sub menu
    type, // for grouping
  };
}
function getClickMenuItem(label, key, icon) {
  return {
    label,
    key,
    icon,
  };
}

export {
  CONTEXT_MENU_LIST_MAPPING,
  CONTEXT_MENU_KEY_VS_TYPE_MAPPING,
  TYPE_VS_CONTEXT_MENU_KEY_MAPPING,
  TAB_VIEW_TYPES,
  EXPLORER_TAB_ITEMS,
  getIconByType,
  getAllowedFileByKey,
};
