import React, {
  useState,
  useCallback,
  useRef,
  useEffect,
  useMemo,
} from "react";
import { createPortal } from "react-dom";
import PropTypes from "prop-types";
import {
  Tree,
  Input,
  Menu,
  Upload,
  message,
  Button,
  Tooltip,
  Typography,
  Modal,
  Tabs,
  Dropdown,
  Badge,
  theme,
  Checkbox,
  Popover,
} from "antd";
import {
  CaretDownOutlined,
  ReloadOutlined,
  FileAddOutlined,
  PlayCircleOutlined,
  InboxOutlined,
  DownCircleOutlined,
  ExclamationCircleFilled,
  CloseCircleFilled,
  CheckCircleFilled,
  DeleteOutlined,
  SettingOutlined,
  CloseOutlined,
} from "@ant-design/icons";
import * as Icons from "@ant-design/icons";
import debounce from "lodash/debounce";
import isEmpty from "lodash/isEmpty";
import Cookies from "js-cookie";

import { useProjectStore } from "../../store/project-store.js";
import { orgStore } from "../../store/org-store.js";
import { ErrorBoundary } from "../../widgets/error_boundary";
import { ResizerComponent } from "../../widgets/resizer";
import { LazyLoadComponent } from "../../widgets/lazy_loader";
import { explorerService } from "./explorer-service.js";
import { DbConfigPreview } from "./db_config/db-config-preview.jsx";
import {
  CONTEXT_MENU_LIST_MAPPING,
  CONTEXT_MENU_KEY_VS_TYPE_MAPPING,
  TYPE_VS_CONTEXT_MENU_KEY_MAPPING,
  TAB_VIEW_TYPES,
  getIconByType,
  getAllowedFileByKey,
  EXPLORER_TAB_ITEMS,
} from "./explorer-constants.js";
import "../ide-layout.css";
import { useNotificationService } from "../../service/notification-service.js";
import { SpinnerLoader } from "../../widgets/spinner_loader/index.js";
import { useRefreshModelsStore } from "../../store/refresh-models-store.js";
import { useExplorerStore } from "../../store/explorer-store.js";
import { LinearScale } from "../../base/icons";

// Static sort options for model explorer
const MODEL_SORT_OPTIONS = [
  { label: "Dependency Chain", key: "dep_chain" },
  { label: "Execution Order", key: "exec_order" },
  { label: "A \u2192 Z", key: "alpha_asc" },
  { label: "Z \u2192 A", key: "alpha_desc" },
];

// Only backgroundColor varies per status — layout handled by .model-status-dot CSS class

const getModelRunStatus = (runStatus, failureReason, lastRunAt, token) => {
  if (runStatus === "RUNNING") {
    return (
      <Tooltip title="Running">
        <span
          className="model-status-dot"
          style={{ backgroundColor: token.colorInfo }}
        />
      </Tooltip>
    );
  }
  if (runStatus === "FAILED") {
    const popoverContent = (
      <div style={{ maxWidth: 400, maxHeight: 300, overflow: "auto" }}>
        {failureReason && (
          <pre
            style={{
              margin: 0,
              fontSize: "12px",
              whiteSpace: "pre-wrap",
              wordBreak: "break-word",
            }}
          >
            {failureReason}
          </pre>
        )}
        {lastRunAt && (
          <div
            style={{
              marginTop: 8,
              fontSize: "11px",
              color: token.colorTextSecondary,
            }}
          >
            Last run: {new Date(lastRunAt).toLocaleString()}
          </div>
        )}
      </div>
    );
    return (
      <Popover
        title="Execution Failed"
        content={popoverContent}
        trigger="hover"
        placement="right"
      >
        <span
          className="model-status-dot"
          style={{ backgroundColor: token.colorError }}
        />
      </Popover>
    );
  }
  if (runStatus === "SUCCESS") {
    const tooltipTitle = (
      <div>
        <div>Success</div>
        {lastRunAt && (
          <div style={{ marginTop: 4, fontSize: "11px" }}>
            Last run: {new Date(lastRunAt).toLocaleString()}
          </div>
        )}
      </div>
    );
    return (
      <Tooltip title={tooltipTitle}>
        <span
          className="model-status-dot"
          style={{ backgroundColor: token.colorSuccess }}
        />
      </Tooltip>
    );
  }
  return null;
};

const IdeExplorer = ({
  currentNode,
  onSelect = () => {},
  onDelete = () => {},
  openNewModalPopup,
  setNewModalPopup = () => {},
  activeTab,
  setActiveTab,
}) => {
  const { Search } = Input;
  const csrfToken = Cookies.get("csrftoken");
  const { selectedOrgId } = orgStore();
  const { DirectoryTree } = Tree;
  const [messageApi, messageContextHolder] = message.useMessage();
  const {
    projectDetails,
    makeActiveTab,
    setOpenedTabs,
    previewTimeTravel,
    projectName,
    projectId,
  } = useProjectStore();
  const currentSchema = useProjectStore((state) => state.currentSchema);
  const setCurrentSchema = useProjectStore((state) => state.setCurrentSchema);
  const setSchemaList = useProjectStore((state) => state.setSchemaList);
  const setExplorerData = useExplorerStore((state) => state.setExplorerData);
  const setDbExplorerData = useExplorerStore(
    (state) => state.setDbExplorerData
  );
  const clearExplorerData = useExplorerStore(
    (state) => state.clearExplorerData
  );

  // Reset currentSchema on unmount to prevent stale data
  useEffect(() => {
    return () => {
      setCurrentSchema("");
    };
  }, [setCurrentSchema]);

  const expService = explorerService();
  const [tree, setTree] = useState([]);
  const [expandedKeys, setExpandedKeys] = useState([]);
  const rawTreeDataRef = useRef(null); // Store raw tree data for re-transformation
  const [autoExpandParent, setAutoExpandParent] = useState(true);
  const [rightClickedItem, setRightClickedItem] = useState();
  const [selectedKey, setSelectedKey] = useState(currentNode);
  const [openNameModal, setOpenNameModal] = useState(false);
  const [newSchemaName, setNewSchemaName] = useState("");
  const [isSchemaModalOpen, setIsSchemaModalOpen] = useState(false);
  const [schemaMenu, setSchemaMenu] = useState(null);
  const [dbExplorer, setDBExplorer] = useState([]);
  const [activeMenu, setActiveMenu] = useState("");
  const [dbLoading, setDbLoading] = useState(false);
  const [checked, setChecked] = useState(true);
  const [openDeleteModal, setOpenDeleteModal] = useState(false);
  const [cachedLists, setCachedLists] = useState({ 1: [], 2: [] });
  const [loading, setLoading] = useState(false);
  const [seedRunning, setSeedRunning] = useState(false);
  const [, setSelectedSeedKeys] = useState([]);
  const [, setSeedDeleteMode] = useState(false);
  const seedRunningRef = useRef(false);
  const seedDeleteModeRef = useRef(false);
  const selectedSeedKeysRef = useRef([]);
  const [, setSelectedModelKeys] = useState([]);
  const [, setModelDeleteMode] = useState(false);
  const modelDeleteModeRef = useRef(false);
  const selectedModelKeysRef = useRef([]);
  const [modelsRunning, setModelsRunning] = useState(new Set()); // Track multiple running models
  const modelsRunningRef = useRef(new Set()); // Ref for click handlers
  const targetElementRef = useRef();
  const uploadRef = useRef();
  const contextMenuRef = useRef("");
  const resizerRef = useRef();
  const [isModalVisible, setIsModalVisible] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [fileList, setFileList] = useState([]);
  const [modelSortBy, setModelSortBy] = useState("dep_chain");
  const modelSortByRef = useRef("dep_chain");
  const MAX_FILE_SIZE_MB = 50;
  const MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024;
  const { refreshModels, setRefreshModels } = useRefreshModelsStore();
  const { notify } = useNotificationService();
  const { setRenamedModel } = useProjectStore();
  const { token } = theme.useToken();

  // Update ref whenever state changes
  useEffect(() => {
    seedRunningRef.current = seedRunning;
  }, [seedRunning]);

  // Update modelsRunning ref whenever state changes
  useEffect(() => {
    modelsRunningRef.current = modelsRunning;
  }, [modelsRunning]);

  // Re-transform tree when modelsRunning changes to update loading indicators
  useEffect(() => {
    if (rawTreeDataRef.current) {
      // Create a fresh copy and re-transform
      const freshTreeData = JSON.parse(JSON.stringify(rawTreeDataRef.current));
      transformTree(freshTreeData);
      setTreeData(freshTreeData);
    }
  }, [modelsRunning]);

  const generateList = (data, dataList = []) => {
    for (const element of data) {
      const { key } = element;
      dataList.push({
        key,
        title: key,
      });
      if (element.children) {
        generateList(element.children, dataList);
      }
    }
    return dataList;
  };

  useEffect(() => {
    getDbEXplorer(projectId);
    getSchemas(); // Add schema refresh on initial load
  }, [projectId]);

  useEffect(() => {
    if (refreshModels) {
      onRefreshDebounce(projectId);
    }
  }, [refreshModels]);

  // Helper function to count total tables across all schemas
  // Database -> Schemas -> Tables (we count tables, which are children of schemas)
  const countTotalTables = (schemas) => {
    if (!schemas || schemas.length === 0) return 0;
    return schemas.reduce((count, schema) => {
      return count + (schema.children?.length || 0);
    }, 0);
  };

  // Sort no_code models based on sort option
  const sortModels = (models, sortBy) => {
    if (sortBy === "alpha_asc") {
      return [...models].sort((a, b) => a.title.localeCompare(b.title));
    }
    if (sortBy === "alpha_desc") {
      return [...models].sort((a, b) => b.title.localeCompare(a.title));
    }
    if (sortBy === "dep_chain") {
      const modelSet = new Set(models.map((m) => m.title));
      const childrenOf = {};
      models.forEach((m) => {
        (m.references || []).forEach((ref) => {
          if (modelSet.has(ref)) {
            if (!childrenOf[ref]) childrenOf[ref] = [];
            childrenOf[ref].push(m.title);
          }
        });
      });
      const modelByName = {};
      models.forEach((m) => {
        modelByName[m.title] = m;
      });
      const visited = new Set();
      const result = [];
      const addChain = (name) => {
        if (visited.has(name) || !modelByName[name]) return;
        visited.add(name);
        result.push(modelByName[name]);
        (childrenOf[name] || []).forEach(addChain);
      };
      models.forEach((m) => {
        const refs = (m.references || []).filter((r) => modelSet.has(r));
        if (refs.length === 0) addChain(m.title);
      });
      models.forEach((m) => {
        if (!visited.has(m.title)) result.push(m);
      });
      return result;
    }
    // "exec_order" and default: keep original backend order (topological/execution)
    return [...models];
  };

  const applyModelDecorations = (models, sortBy) => {
    // Only apply parent-child indent in Dependency Chain mode
    if (sortBy !== "dep_chain") {
      models.forEach((m) => {
        delete m._isChild;
      });
      return;
    }
    // Build set of model names to distinguish from external table references
    const modelNames = new Set(models.map((m) => m.title));
    models.forEach((m) => {
      const refs = (m.references || []).filter((r) => modelNames.has(r));
      if (refs.length > 0) {
        m._isChild = true;
      } else {
        delete m._isChild;
      }
    });
  };

  // handleModelSort and modelSortMenu are defined after setTreeData (line ~816)
  // to avoid temporal dead zone — see sortMenuRef below for the Dropdown binding

  // Function to map string icons from API to actual icon components
  // depth: 0 = root (Database), 1 = schema, 2 = table, 3 = column
  const mapIconsToTreeData = (data, depth = 0) => {
    return data.map((node) => {
      const hasChildren = node.children && node.children.length > 0;
      let titleWithBadge = node.title;

      // For the root database node (depth 0), show total tables count across all schemas
      if (depth === 0 && hasChildren) {
        const totalTables = countTotalTables(node.children);
        if (totalTables > 0) {
          titleWithBadge = (
            <span>
              {node.title}
              <Badge
                count={totalTables}
                size="small"
                style={{
                  marginLeft: 8,
                  backgroundColor: token.colorWarning,
                  color: "#000",
                  fontSize: "10px",
                }}
                overflowCount={999}
              />
            </span>
          );
        }
      }
      // For schema nodes (depth 1), show their tables count
      else if (depth === 1 && hasChildren) {
        const tablesCount = node.children.length;
        if (tablesCount > 0) {
          titleWithBadge = (
            <span>
              {node.title}
              <Badge
                count={tablesCount}
                size="small"
                style={{
                  marginLeft: 8,
                  backgroundColor: token.colorWarning,
                  color: "#000",
                  fontSize: "10px",
                }}
                overflowCount={999}
              />
            </span>
          );
        }
      }
      // For tables (depth 2) and columns (depth 3+), no badge

      return {
        ...node,
        title: titleWithBadge,
        icon:
          node.icon && Icons[node.icon]
            ? React.createElement(Icons[node.icon])
            : null,
        children: node.children
          ? mapIconsToTreeData(node.children, depth + 1)
          : undefined,
      };
    });
  };

  useEffect(
    function updateActiveNode() {
      setSelectedKey(currentNode);

      scrollToSelectedNode();

      if (!currentNode?.includes("/")) {
        // if there is no `/` character in currentNode(key) then it is root node
        return;
      }
      // to expand the collapsed folder items when Editor tab changes
      const keysToExpand = getKeysToExpand();

      const mergedExpandedKeys = [...expandedKeys, ...keysToExpand];
      setExpandedKeys([...new Set(mergedExpandedKeys)]);
    },
    [currentNode]
  );
  const getAllParentKeys = (treeData) => {
    const keys = [];
    const traverse = (nodes) => {
      nodes.forEach((node) => {
        if (node.children) {
          keys.push(node.key);
          traverse(node.children);
        }
      });
    };
    traverse(treeData);
    return keys;
  };

  function getSchemas(projectIdParam) {
    const id = projectIdParam || projectId;
    if (!id) return;
    expService
      .getAllSchema(id)
      .then((res) => {
        const data = res?.data?.schema_names || [];
        const defaultSchema = res?.data?.default_project_schema || "";

        // Always include defaultSchema in the list if it exists
        const allSchemas = [
          ...new Set([...data, defaultSchema].filter(Boolean)),
        ];

        // If we have a default schema, always select it
        if (defaultSchema) {
          setCurrentSchema(defaultSchema);
        } else {
          setCurrentSchema("");
        }

        // Store plain schema list in shared store
        setSchemaList(allSchemas);

        const items = allSchemas.map((el) => ({
          label: el,
          key: el,
        }));

        items.push({
          label: <Typography.Text className="flex">+ New</Typography.Text>,
          key: "add-new-schema",
        });

        setSchemaMenu(items);
      })
      .catch((error) => {
        console.error(error);
        notify({ error });
      });
  }

  const seedIconFunctions = (type, rightClickedItem) => {
    if (type === "run") {
      const fileName = null;
      if (rightClickedItem) {
        runSeed(fileName);
      }
    } else if (type === "add") {
      openFileUpload({ ...rightClickedItem, contextMenuKey: "add_csv" });
      closeContextMenu();
    }
  };
  const seedRunFile = (type, fileName) => {
    if (type === "run") {
      runSeed(fileName);
    }
  };

  const ncFolder = (rightClickedItem) => {
    setOpenNameModal({ ...rightClickedItem, contextMenuKey: "nc_new_model" });
  };
  const treeNoCodeModeTitleIcon = (data) => {
    data.map((item) => {
      if (item.title === "no_code") {
        item.children = item.children || [];
        // Note: sort and decorations are applied in getExplorer/rebuildTree/handleModelSort
        // BEFORE transformTree, so _isChild flag is set when className is assigned
        // Clean up stale selected model keys
        const currentModelKeys = item.children.map((c) => c.key);
        const filtered = selectedModelKeysRef.current.filter((k) =>
          currentModelKeys.includes(k)
        );
        if (filtered.length !== selectedModelKeysRef.current.length) {
          selectedModelKeysRef.current = filtered;
          setSelectedModelKeys(filtered);
        }
        item.title = (
          <Typography.Text disabled={previewTimeTravel}>
            no_code
            <Typography className="float_right flex">
              {modelDeleteModeRef.current ? (
                <>
                  {item.children.length > 0 && (
                    <Checkbox
                      checked={
                        selectedModelKeysRef.current.length > 0 &&
                        selectedModelKeysRef.current.length ===
                          item.children.length
                      }
                      indeterminate={
                        selectedModelKeysRef.current.length > 0 &&
                        selectedModelKeysRef.current.length <
                          item.children.length
                      }
                      onChange={(e) =>
                        handleSelectAllToggle(
                          e,
                          item.children,
                          selectedModelKeysRef,
                          setSelectedModelKeys
                        )
                      }
                      onClick={(e) => e.stopPropagation()}
                      style={{ marginRight: 4 }}
                    />
                  )}
                  <Tooltip
                    placement="top"
                    title={`Delete (${selectedModelKeysRef.current.length})`}
                  >
                    <Typography.Text
                      className="ml-10 icon-highlight"
                      onClick={() => {
                        handleIconFunctions("delete_selected_model", item);
                      }}
                      disabled={
                        previewTimeTravel ||
                        selectedModelKeysRef.current.length === 0
                      }
                    >
                      <DeleteOutlined />
                    </Typography.Text>
                  </Tooltip>
                  <Tooltip placement="top" title="Cancel">
                    <Typography.Text
                      className="ml-10 icon-highlight"
                      onClick={(e) => {
                        e.stopPropagation();
                        modelDeleteModeRef.current = false;
                        selectedModelKeysRef.current = [];
                        setModelDeleteMode(false);
                        setSelectedModelKeys([]);
                        setActiveMenu("");
                        rebuildTree();
                      }}
                    >
                      <CloseOutlined />
                    </Typography.Text>
                  </Tooltip>
                </>
              ) : (
                <>
                  <Tooltip placement="top" title="Add New Model">
                    <Typography.Text
                      onClick={() => {
                        handleIconFunctions("nc_add_model", item);
                      }}
                      className={`icon-highlight ${
                        activeMenu === "nc_add_model"
                          ? "icon-highlight-active"
                          : ""
                      }`}
                      disabled={previewTimeTravel}
                    >
                      <FileAddOutlined />
                    </Typography.Text>
                  </Tooltip>
                  <Tooltip placement="top" title="Delete All">
                    <Typography.Text
                      disabled={previewTimeTravel || !item.children?.length}
                      className={`ml-10 icon-highlight ${
                        activeMenu === "nc_delete_model"
                          ? "icon-highlight-active"
                          : ""
                      }`}
                      onClick={() => {
                        handleIconFunctions("nc_delete_model", item);
                      }}
                    >
                      <DeleteOutlined />
                    </Typography.Text>
                  </Tooltip>
                  <Dropdown
                    menu={modelSortMenu}
                    trigger={["click"]}
                    placement="bottomRight"
                  >
                    <Tooltip placement="top" title="Sort Models">
                      <Typography.Text className="ml-10 icon-highlight">
                        <Icons.SortAscendingOutlined />
                      </Typography.Text>
                    </Tooltip>
                  </Dropdown>
                </>
              )}
            </Typography>
          </Typography.Text>
        );
        // Add checkboxes and run status to model children
        item.children = item.children.map((child) => {
          const statusBadge = getModelRunStatus(
            child.run_status,
            child.failure_reason,
            child.last_run_at,
            token
          );
          const wrappedIcon = statusBadge ? (
            <span className="model-icon-badge-wrapper">
              {child.icon}
              {statusBadge}
            </span>
          ) : (
            child.icon
          );
          return {
            ...child,
            icon: wrappedIcon,
            title: (
              <Typography.Text type="span" disabled={previewTimeTravel}>
                {modelDeleteModeRef.current && (
                  <Checkbox
                    checked={selectedModelKeysRef.current.includes(child.key)}
                    onChange={() =>
                      handleItemSelectToggle(
                        child.key,
                        selectedModelKeysRef,
                        setSelectedModelKeys
                      )
                    }
                    onClick={(e) => e.stopPropagation()}
                    style={{ marginRight: 6 }}
                  />
                )}
                {child.title}
              </Typography.Text>
            ),
          };
        });
      }
      return item;
    });
  };

  const handleSelectAllToggle = (e, children, keysRef, setKeys) => {
    e.stopPropagation();
    if (keysRef.current?.length === children?.length) {
      keysRef.current = [];
      setKeys([]);
    } else {
      const allKeys = children?.map((c) => c.key) || [];
      keysRef.current = allKeys;
      setKeys(allKeys);
    }
    rebuildTree();
  };

  const handleItemSelectToggle = (itemKey, keysRef, setKeys) => {
    const prev = keysRef.current || [];
    const next = prev.includes(itemKey)
      ? prev.filter((k) => k !== itemKey)
      : [...prev, itemKey];
    keysRef.current = next;
    setKeys(next);
    rebuildTree();
  };

  const handleSeedIconClick = (event, title) => {
    event.stopPropagation();
    if (!previewTimeTravel && !seedRunningRef.current) {
      seedRunFile("run", title);
    }
  };

  const handleModelRun = (modelName) => {
    // Check if this specific model is already running
    if (previewTimeTravel || modelsRunningRef.current.has(modelName)) {
      if (modelsRunningRef.current.has(modelName)) {
        messageApi.warning({
          key: `model-already-running-${modelName}`,
          content: `Model "${modelName}" is already running`,
        });
      }
      return;
    }

    // Add model to running set
    setModelsRunning((prev) => new Set([...prev, modelName]));

    // Show loading message
    messageApi.loading({
      key: `model-run-${modelName}`,
      content: `Running model "${modelName}"...`,
      duration: 0, // Don't auto-close
    });

    expService
      .runModel(projectId, modelName)
      .then(() => {
        messageApi.destroy(`model-run-${modelName}`);
        notify({
          type: "success",
          message: "Model Run Successful",
          description: `Model "${modelName}" executed successfully.`,
        });
        getExplorer(projectId);
        setRefreshModels(true);
      })
      .catch((error) => {
        messageApi.destroy(`model-run-${modelName}`);
        notify({ error });
        getExplorer(projectId);
        setRefreshModels(true);
      })
      .finally(() => {
        // Remove model from running set
        setModelsRunning((prev) => {
          const next = new Set(prev);
          next.delete(modelName);
          return next;
        });
      });
  };

  const handleModalCancel = () => {
    // Check if there are any files currently uploading
    const hasUploadingFiles = fileList.some(
      (file) => file.status === "uploading"
    );

    // Also check the global uploading state
    if (hasUploadingFiles || uploading) {
      // Don't allow closing if files are still uploading
      messageApi.warning({
        content:
          "Please wait for all files to finish uploading before closing the modal.",
        duration: 3,
      });
      return;
    }

    setIsModalVisible(false);
    setFileList([]);
    setActiveMenu("");
  };

  // Handler to run seed and close modal
  const handleRunSeedAndClose = () => {
    // Check if there are any files currently uploading
    const hasUploadingFiles = fileList.some(
      (file) => file.status === "uploading"
    );

    if (hasUploadingFiles || uploading) {
      messageApi.warning({
        content:
          "Please wait for all files to finish uploading before running seed.",
        duration: 3,
      });
      return;
    }

    // Close the modal first
    setIsModalVisible(false);
    setFileList([]);
    setActiveMenu("");

    // Trigger seed execution (runs all seeds)
    runSeed();
  };

  const getSeedStatus = (status) => {
    if (status === "uploaded") {
      return (
        <Tooltip title="Yet to Seed." key="Yet to Seed.">
          <ExclamationCircleFilled style={{ color: "yellow" }} />
        </Tooltip>
      );
    } else if (status === "Failed") {
      return (
        <Tooltip title="Failed" key="Failed">
          <CloseCircleFilled style={{ color: "red" }} />
        </Tooltip>
      );
    } else {
      return (
        <Tooltip title="Success" key="Success">
          <CheckCircleFilled style={{ color: "green" }} />
        </Tooltip>
      );
    }
  };

  const handleIconFunctions = (type, param) => {
    if (type === "run_seed") {
      if (
        !seedRunningRef.current &&
        !previewTimeTravel &&
        currentSchema &&
        param.children.length
      ) {
        seedIconFunctions("run", param);
        setActiveMenu("run_seed");
      }
    } else if (type === "add_seed") {
      seedIconFunctions("add", param);
      setActiveMenu("add_seed");
    } else if (type === "delete_seed" && param?.children?.length) {
      const allKeys = param.children.map((c) => c.key);
      seedDeleteModeRef.current = true;
      selectedSeedKeysRef.current = allKeys;
      setSeedDeleteMode(true);
      setSelectedSeedKeys(allKeys);
      setActiveMenu("delete_seed");
      rebuildTree();
    } else if (type === "nc_delete_model" && param?.children?.length) {
      const allKeys = param.children.map((c) => c.key);
      modelDeleteModeRef.current = true;
      selectedModelKeysRef.current = allKeys;
      setModelDeleteMode(true);
      setSelectedModelKeys(allKeys);
      setActiveMenu("nc_delete_model");
      rebuildTree();
    } else if (type === "nc_add_model") {
      ncFolder(param);
      setActiveMenu("nc_add_model");
    } else if (type === "delete_selected_seed") {
      setOpenDeleteModal({
        contextMenuKey: "seed_delete_csv",
        key: selectedSeedKeysRef.current,
        type: "multiple",
      });
      setActiveMenu("delete_selected_seed");
    } else if (type === "delete_selected_model") {
      setOpenDeleteModal({
        contextMenuKey: "nc_delete_model",
        key: selectedModelKeysRef.current,
        type: "multiple",
      });
      setActiveMenu("delete_selected_model");
    }
  };

  // Helper to count no_code models in the models folder
  const countNoCodeModels = (modelsChildren) => {
    if (!modelsChildren) return 0;
    const noCodeFolder = modelsChildren.find(
      (item) => item.title === "no_code"
    );
    return noCodeFolder?.children?.length || 0;
  };

  const setTreeData = (treeData, updateExpanded = true) => {
    const resp = treeData.map((el) => {
      if (el.title === "seeds") {
        el.children = el.children || [];
        const seedCount = el.children.length;
        el.title = (
          <Typography.Text type="span" disabled={previewTimeTravel}>
            seeds
            {seedCount > 0 && (
              <Badge
                count={seedCount}
                size="small"
                style={{
                  marginLeft: 8,
                  backgroundColor: token.colorWarning,
                  color: "#000",
                  fontSize: "10px",
                }}
              />
            )}
            <Typography className="float_right flex">
              {seedDeleteModeRef.current ? (
                <>
                  {el.children.length > 0 && (
                    <Checkbox
                      checked={
                        selectedSeedKeysRef.current.length > 0 &&
                        selectedSeedKeysRef.current.length ===
                          el.children.length
                      }
                      indeterminate={
                        selectedSeedKeysRef.current.length > 0 &&
                        selectedSeedKeysRef.current.length < el.children.length
                      }
                      onChange={(e) =>
                        handleSelectAllToggle(
                          e,
                          el.children,
                          selectedSeedKeysRef,
                          setSelectedSeedKeys
                        )
                      }
                      onClick={(e) => e.stopPropagation()}
                      style={{ marginRight: 4 }}
                    />
                  )}
                  <Tooltip
                    placement="top"
                    title={`Delete (${selectedSeedKeysRef.current.length})`}
                  >
                    <Typography.Text
                      className="ml-10 icon-highlight"
                      onClick={() => {
                        handleIconFunctions("delete_selected_seed", el);
                      }}
                      disabled={
                        previewTimeTravel ||
                        selectedSeedKeysRef.current.length === 0
                      }
                    >
                      <DeleteOutlined />
                    </Typography.Text>
                  </Tooltip>
                  <Tooltip placement="top" title="Cancel">
                    <Typography.Text
                      className="ml-10 icon-highlight"
                      onClick={(e) => {
                        e.stopPropagation();
                        seedDeleteModeRef.current = false;
                        selectedSeedKeysRef.current = [];
                        setSeedDeleteMode(false);
                        setSelectedSeedKeys([]);
                        setActiveMenu("");
                        rebuildTree();
                      }}
                    >
                      <CloseOutlined />
                    </Typography.Text>
                  </Tooltip>
                </>
              ) : (
                <>
                  <Dropdown
                    menu={{
                      items: (schemaMenu || []).map((el) => ({
                        ...el,
                        label:
                          el.key === "add-new-schema" ? (
                            el.label
                          ) : (
                            <div className="flex" style={{ minWidth: "150px" }}>
                              <div className="w20px">
                                {currentSchema === el.key && (
                                  <CheckCircleFilled className="check-icon-green" />
                                )}
                              </div>
                              <Typography.Text className="flex-1">
                                {el.label}
                              </Typography.Text>
                            </div>
                          ),
                      })),
                      onClick: ({ key }) => handleSchemaChange(key),
                    }}
                    trigger={["click"]}
                    placement="bottomRight"
                    overlayClassName="schema-select-dropdown"
                  >
                    <Tooltip placement="top" title="Set Schema">
                      <Typography.Text className="ml-10 icon-highlight">
                        <SettingOutlined />
                      </Typography.Text>
                    </Tooltip>
                  </Dropdown>

                  <Tooltip
                    placement="top"
                    title={
                      !currentSchema
                        ? "Run Seed Disabled - Please select a schema"
                        : previewTimeTravel
                        ? "Run Seed Disabled - Time travel mode active"
                        : (schemaMenu || []).length <= 1
                        ? "Run Seed Disabled - No schemas available"
                        : "Run Seed"
                    }
                  >
                    <Typography.Text
                      className={`ml-10 icon-highlight ${
                        activeMenu === "run_seed" ? "icon-highlight-active" : ""
                      }`}
                      onClick={() => {
                        if (
                          !previewTimeTravel &&
                          currentSchema &&
                          el.children.length
                        ) {
                          handleIconFunctions("run_seed", el);
                        }
                      }}
                      disabled={
                        previewTimeTravel ||
                        !currentSchema ||
                        (schemaMenu || []).length <= 1
                      }
                    >
                      <PlayCircleOutlined />
                    </Typography.Text>
                  </Tooltip>

                  <Tooltip placement="top" title="Add Seed File">
                    <Typography.Text
                      className={`ml-10 icon-highlight ${
                        activeMenu === "add_seed" ? "icon-highlight-active" : ""
                      }`}
                      onClick={() => {
                        handleIconFunctions("add_seed", el);
                      }}
                      disabled={previewTimeTravel}
                    >
                      <FileAddOutlined />
                    </Typography.Text>
                  </Tooltip>

                  <Tooltip placement="top" title="Delete All">
                    <Typography.Text
                      disabled={previewTimeTravel || !el.children.length}
                      className={`ml-10 icon-highlight ${
                        activeMenu === "delete_seed"
                          ? "icon-highlight-active"
                          : ""
                      }`}
                      onClick={() => {
                        handleIconFunctions("delete_seed", el);
                      }}
                    >
                      <DeleteOutlined />
                    </Typography.Text>
                  </Tooltip>
                </>
              )}
            </Typography>
          </Typography.Text>
        );
        // Clean up stale selected seed keys
        const currentSeedKeys = el.children.map((c) => c.key);
        const filtered = selectedSeedKeysRef.current.filter((k) =>
          currentSeedKeys.includes(k)
        );
        if (filtered.length !== selectedSeedKeysRef.current.length) {
          selectedSeedKeysRef.current = filtered;
          setSelectedSeedKeys(filtered);
        }
        el.children = el.children.map((child) => {
          return {
            ...child,
            title: (
              <Typography.Text
                type="span"
                disabled={previewTimeTravel || seedRunningRef.current}
              >
                {seedDeleteModeRef.current && (
                  <Checkbox
                    checked={selectedSeedKeysRef.current.includes(child.key)}
                    onChange={() =>
                      handleItemSelectToggle(
                        child.key,
                        selectedSeedKeysRef,
                        setSelectedSeedKeys
                      )
                    }
                    onClick={(e) => e.stopPropagation()}
                    style={{ marginRight: 6 }}
                  />
                )}
                {child.title}
                <Typography className="float_right">
                  {getSeedStatus(child.status)}
                  <Tooltip placement="top" title={`Run ${child.title}`}>
                    <Typography.Text>
                      <PlayCircleOutlined
                        onClick={(event) => {
                          event.stopPropagation();
                          if (
                            !previewTimeTravel &&
                            currentSchema &&
                            (schemaMenu || []).length > 1
                          ) {
                            handleSeedIconClick(event, child.title);
                          }
                        }}
                        className={`seed-icon ${
                          previewTimeTravel ||
                          seedRunningRef.current ||
                          !currentSchema ||
                          (schemaMenu || []).length <= 1
                            ? "seed-icon-disabled"
                            : ""
                        }`}
                      />
                    </Typography.Text>
                  </Tooltip>
                </Typography>
              </Typography.Text>
            ),
          };
        });
      } else if (el.title === "models") {
        const modelCount = countNoCodeModels(el.children);
        el.title = (
          <Typography.Text type="span" disabled={previewTimeTravel}>
            models
            {modelCount > 0 && (
              <Badge
                count={modelCount}
                size="small"
                style={{
                  marginLeft: 8,
                  backgroundColor: token.colorWarning,
                  color: "#000",
                  fontSize: "10px",
                }}
              />
            )}
          </Typography.Text>
        );
        treeNoCodeModeTitleIcon(el.children);
      }
      return el;
    });
    setTree(resp);
    if (updateExpanded) {
      // Set all parent keys as expanded keys
      const allParentKeys = getAllParentKeys(resp);
      setExpandedKeys(allParentKeys);
    }
  };

  const rebuildTree = () => {
    if (rawTreeDataRef.current.length > 0) {
      const freshData = JSON.parse(JSON.stringify(rawTreeDataRef.current));
      // Apply sort and decorations BEFORE transformTree so _isChild is set
      freshData.forEach((node) => {
        if (node.title === "models" && node.children) {
          node.children.forEach((child) => {
            if (child.title === "no_code" && child.children) {
              child.children = sortModels(
                child.children,
                modelSortByRef.current
              );
              applyModelDecorations(child.children, modelSortByRef.current);
            }
          });
        }
      });
      transformTree(freshData);
      setTreeData(freshData, false);
    }
  };

  const handleModelSort = useCallback((sortBy) => {
    modelSortByRef.current = sortBy;
    setModelSortBy(sortBy);
  }, []);

  // Rebuild tree when sort mode changes so the dropdown checkmark updates
  useEffect(() => {
    if (rawTreeDataRef.current && rawTreeDataRef.current.length > 0) {
      const freshData = JSON.parse(JSON.stringify(rawTreeDataRef.current));
      freshData.forEach((node) => {
        if (node.title === "models" && node.children) {
          node.children.forEach((child) => {
            if (child.title === "no_code" && child.children) {
              child.children = sortModels(child.children, modelSortBy);
              applyModelDecorations(child.children, modelSortBy);
            }
          });
        }
      });
      transformTree(freshData);
      setTreeData(freshData, false);
    }
    // eslint-disable-next-line
  }, [modelSortBy]);

  const modelSortMenu = useMemo(
    () => ({
      items: MODEL_SORT_OPTIONS.map((opt) => ({
        key: opt.key,
        label: (
          <div
            style={{
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              gap: 12,
              minWidth: 160,
              fontWeight: modelSortBy === opt.key ? 600 : "normal",
            }}
          >
            <span>{opt.label}</span>
            {modelSortBy === opt.key && (
              <CheckCircleFilled style={{ color: "#1677ff", fontSize: 12 }} />
            )}
          </div>
        ),
      })),
      onClick: ({ key }) => handleModelSort(key),
    }),
    [modelSortBy, handleModelSort]
  );

  // schemaMenu starts as null; becomes an array (possibly empty) after
  // getSchemas resolves. Gating on truthiness skips the redundant mount-time
  // fetch while still firing for projects whose schema list is legitimately
  // empty.
  useEffect(() => {
    if (schemaMenu) {
      getExplorer(projectId);
    }
  }, [schemaMenu, currentSchema]);

  // Clear shared explorer data on project switch so other consumers
  // (e.g. chat autocomplete) don't momentarily read the previous project's tree.
  // Ref-gated so the clear does NOT fire on initial mount / remount within the
  // same project — only when projectId actually changes.
  const prevProjectIdRef = useRef(projectId);
  useEffect(() => {
    if (prevProjectIdRef.current !== projectId) {
      clearExplorerData();
      prevProjectIdRef.current = projectId;
    }
  }, [projectId, clearExplorerData]);

  function getExplorer(projectId) {
    if (!projectId) return;
    setLoading(true);
    expService
      .getExplorer(projectId)
      .then((res) => {
        const treeData = res.data.children;
        rawTreeDataRef.current = JSON.parse(JSON.stringify(treeData));
        // Publish the raw (pre-mutation) shape to the shared store so
        // consumers like chat-ai/Body.jsx get the untransformed children.
        setExplorerData(rawTreeDataRef.current);
        // Apply sort and decorations to no_code models BEFORE transformTree
        // so that _isChild flag is set when className is assigned
        treeData.forEach((node) => {
          if (node.title === "models" && node.children) {
            node.children.forEach((child) => {
              if (child.title === "no_code" && child.children) {
                child.children = sortModels(
                  child.children,
                  modelSortByRef.current
                );
                applyModelDecorations(child.children, modelSortByRef.current);
              }
            });
          }
        });
        transformTree(treeData);
        setTreeData(treeData);

        setCachedLists((prev) => ({
          ...prev,
          1: generateList(treeData), // Correct key
        }));
      })
      .catch((error) => {
        console.error(error);
        notify({ error });
      })
      .finally(() => {
        setLoading(false);
      });
  }

  function getDbEXplorer(projectId, hardReload = false) {
    if (!projectId) return;
    setDbLoading(true);
    expService
      .getDbExplorer(projectId, hardReload)
      .then((res) => {
        const treeData = res.data;
        const mappedData = mapIconsToTreeData([treeData]);
        setDBExplorer(mappedData);
        setDbExplorerData(treeData);
        setCachedLists((prev) => ({
          ...prev,
          2: generateList([treeData]), // Correct key
        }));
      })
      .catch((error) => {
        console.error(error);
        notify({ error });
      })
      .finally(() => {
        setDbLoading(false);
      });
  }

  useEffect(() => {
    getExplorer(projectId);
  }, [previewTimeTravel]);

  // Listen for refreshExplorer custom event (triggered by AI chat delete models)
  useEffect(() => {
    const handleRefreshExplorer = () => {
      getExplorer(projectId);
    };

    window.addEventListener("refreshExplorer", handleRefreshExplorer);
    return () => {
      window.removeEventListener("refreshExplorer", handleRefreshExplorer);
    };
  }, [projectId]);

  // Listen for triggerDeleteAllModels event (triggered by AI chat)
  useEffect(() => {
    const handleTriggerDeleteAllModels = () => {
      // Find the no_code folder in the tree
      const modelsFolder = tree.find(
        (item) => item.title === "models" || item.key?.endsWith("/models")
      );
      const noCodeFolder = modelsFolder?.children?.find(
        (item) => item.title === "no_code" || item.key?.includes("/no_code")
      );

      if (noCodeFolder && noCodeFolder.children?.length > 0) {
        // Trigger the existing delete modal
        handleIconFunctions("nc_delete_model", noCodeFolder);
      }
    };

    window.addEventListener(
      "triggerDeleteAllModels",
      handleTriggerDeleteAllModels
    );
    return () => {
      window.removeEventListener(
        "triggerDeleteAllModels",
        handleTriggerDeleteAllModels
      );
    };
  }, [tree]);

  // Listen for triggerDeleteSpecificModels event (triggered by AI chat for specific model deletion)
  useEffect(() => {
    const handleTriggerDeleteSpecificModels = (event) => {
      const modelNames = event.detail?.models || [];
      if (modelNames.length === 0) return;

      // Find the no_code folder in the tree
      const modelsFolder = tree.find(
        (item) => item.title === "models" || item.key?.endsWith("/models")
      );
      const noCodeFolder = modelsFolder?.children?.find(
        (item) => item.title === "no_code" || item.key?.includes("/no_code")
      );

      if (!noCodeFolder?.children?.length) return;

      // Find the specific model nodes by their names
      const matchingModelKeys = noCodeFolder.children
        .filter((model) => {
          // Model title could be the model name or the key could contain it
          const modelName =
            model.title || model.key?.split("/").pop()?.replace(".yaml", "");
          return modelNames.includes(modelName);
        })
        .map((model) => model.key);

      if (matchingModelKeys.length > 0) {
        // Trigger the delete modal with only the specific models
        setOpenDeleteModal({
          contextMenuKey: "nc_delete_model",
          key: matchingModelKeys,
          type: matchingModelKeys.length === 1 ? "single" : "multiple",
        });
        setActiveMenu("nc_delete_model");
      }
    };

    window.addEventListener(
      "triggerDeleteSpecificModels",
      handleTriggerDeleteSpecificModels
    );
    return () => {
      window.removeEventListener(
        "triggerDeleteSpecificModels",
        handleTriggerDeleteSpecificModels
      );
    };
  }, [tree]);

  function scrollToSelectedNode() {
    setTimeout(function scrollToView() {
      // antd tree scrollTo() method is not working here
      const treeElement = document.getElementById("explorerTree");
      const selectedNodeElement = treeElement?.getElementsByClassName(
        "ant-tree-node-selected"
      )[0];
      selectedNodeElement?.scrollIntoView({
        block: "nearest",
        behavior: "smooth",
      });
    }, 20);
  }

  function getKeysToExpand() {
    // currentNode(key) usually contains path(directory) of the node(file). eg. "rootFolder/childFolder/node"
    const splittedKeys = currentNode.split("/");
    splittedKeys.pop(); // removing leaf node since not needed

    // joining back to form original keys eg. ["rootFolder", "rootFolder/childFolder"]
    const originalKeys = splittedKeys.map((_, index) => {
      return splittedKeys.slice(0, index + 1).join("/");
    });
    return originalKeys;
  }

  const onSearchDebounce = useCallback(
    debounce(({ target: { value } }) => {
      onSearch(value);
    }, 600),
    [tree, activeTab, dbExplorer]
  );

  const onRefreshDebounce = useCallback(
    debounce(
      (projectId, dbHardReload = false) => {
        getExplorer(projectId);
        getDbEXplorer(projectId, dbHardReload);
        getSchemas(projectId);
      },
      1000,
      { leading: true, trailing: false }
    ),
    []
  );

  function onExpand(newExpandedKeys) {
    setExpandedKeys(newExpandedKeys);
    setAutoExpandParent(false);
  }

  function getParentNode(key, treeData = activeTab === 1 ? tree : dbExplorer) {
    let parentNode;
    for (const element of treeData) {
      if (element.children) {
        if (element.children.some((item) => item.key === key)) {
          parentNode = element;
        } else {
          const findParentNode = getParentNode(key, element.children);
          if (findParentNode) parentNode = findParentNode;
        }
      }
    }
    return parentNode;
  }

  function onSearch(value) {
    if (!value?.trim()) {
      setExpandedKeys([]);
      setAutoExpandParent(false);
      return;
    }

    const resp = cachedLists[activeTab];
    const newExpandedKeys = resp
      .map((item) => {
        if (item.title.indexOf(value) > -1) {
          return getParentNode(item.key)?.key;
        }
        return null;
      })
      .filter((item, i, self) => item && self.indexOf(item) === i);
    setExpandedKeys(newExpandedKeys);
    setAutoExpandParent(true);
  }

  const onNodeSelect = debounce((selectedKeys, event) => {
    const projTabs = projectDetails[projectId] || [];
    const focused = projTabs.focussedTab;
    const opened = projTabs.openedTabs || [];

    const selectedKey = selectedKeys?.[0];

    // Allow execution if selected key is not the same as the focused key
    const isSameAsFocused = focused?.key === selectedKey;
    const isEmptyTabs = opened.length === 0;

    if (!isEmptyTabs & isSameAsFocused) {
      return; // Exit early
    }

    // Proceed with the selection logic
    setSelectedKey(selectedKey);
    if (TAB_VIEW_TYPES.includes(event.node.type)) {
      onSelect({ key: selectedKey, ...event });
    }
  }, 20);

  function onLoadData() {
    return new Promise((resolve) => {
      resolve();
    });
  }

  function setContextMenu(item) {
    if (["ROOT_MODEL", "ROOT_CONFIGURATION"].includes(item.node.type)) return;

    // highlighting right clicked tree node
    const targetElement = item.event.target.offsetParent;
    if (targetElement) targetElement.style.background = "var(--border-color-2)";
    targetElementRef.current = targetElement;

    // Extract title string from node — title may be a React element
    // (e.g. <Typography.Text>{checkbox}{name}</Typography.Text>)
    let nodeTitle = item.node.title;
    if (typeof nodeTitle === "object") {
      const children = nodeTitle.props?.children;
      const text = Array.isArray(children)
        ? children.find((c) => typeof c === "string")
        : children;
      nodeTitle = typeof text === "string" ? text.split(".")[0] : "";
    }

    setRightClickedItem({
      type: item.node.type,
      title: nodeTitle,
      key: item.node.key,
      pageX: item.event.clientX,
      pageY: item.event.clientY,
      children: item.node.children,
    });
    document.addEventListener("click", closeContextMenu);
  }

  function getContextMenu() {
    if (!rightClickedItem) return;
    const { type, pageX, pageY } = rightClickedItem;
    let menuItems = getMenuList(type);
    if (type === "ROOT_SEED" && (!tree[1].children.length || !currentSchema)) {
      menuItems = [menuItems[1]];
    }
    menuItems = getValidatedMenuItems(menuItems); // restricting some options based on existing node
    if (!menuItems || isEmpty(menuItems)) {
      closeContextMenu();
      return;
    }

    const { left, top } = calculateContextMenuPosition(
      pageX,
      pageY,
      menuItems?.length
    );
    const positionStyle = { left: `${left}px`, top: `${top}px` };

    return createPortal(
      <div className="contextMenu" style={positionStyle}>
        <Menu mode="vertical" items={menuItems} onClick={onContextMenuSelect} />
      </div>,
      document.body
    );
  }

  function onContextMenuSelect({ key, newName, targetContext }) {
    const targetNode = getTargetNode(
      rightClickedItem?.key || targetContext.key,
      tree
    );
    // `targetNode` and `rightClickedItem` refers to same node
    // changing in `targetNode` will reflect in `tree` but changing in `rightClickedItem` will not.
    if (
      [
        "database_add",
        "nc_new_folder",
        "nc_new_model",
        "fc_new_folder",
        "fc_new_model",
        "seed_new_folder",
        "test_new_file",
        "documentation_new_file",
      ].includes(key)
    ) {
      const isLeaf = [
        "fc_new_model",
        "test_new_file",
        "documentation_new_file",
      ].includes(key);
      if (!newName) {
        setOpenNameModal({ ...rightClickedItem, contextMenuKey: key });
        return;
      }
      createNewFolder(
        newName,
        targetNode,
        CONTEXT_MENU_KEY_VS_TYPE_MAPPING[key],
        isLeaf
      );
      setExpandedKeys([...expandedKeys, targetNode.key]);
    } else if (["add_csv"].includes(key)) {
      openFileUpload({ ...rightClickedItem, contextMenuKey: key });
    } else if (["seed_run"].includes(key)) {
      runSeed();
    } else if (["seed_run_csv"].includes(key)) {
      const fileName = targetNode["key"].substring(
        targetNode["key"].lastIndexOf("/") + 1
      );
      runSeed(fileName);
    } else if (["nc_run_model"].includes(key)) {
      // Extract model name from the key (e.g., "project/models/no_code/model_name")
      const modelName =
        rightClickedItem?.key?.split("/").pop() || rightClickedItem?.title;
      if (modelName) {
        handleModelRun(modelName);
      }
      closeContextMenu();
      return;
    } else if (
      [
        "database_delete",
        "fc_delete_folder",
        "fc_delete_model",
        "nc_delete_folder",
        "nc_delete_model",
        "snapshot_delete",
        "seed_delete_folder",
        "seed_delete_csv",
      ].includes(key)
    ) {
      if (!newName) {
        setOpenDeleteModal({
          ...rightClickedItem,
          contextMenuKey: key,
          key: [rightClickedItem?.key],
          type: "single",
        });
        return;
      }
      deleteFolder(targetNode);
    } else if (
      [
        "nc_rename_folder",
        "fc_rename_folder",
        "seed_rename_folder",
        "seed_rename_csv",
        "nc_rename_model",
      ].includes(key)
    ) {
      if (!newName) {
        setOpenNameModal({ ...rightClickedItem, contextMenuKey: key });
        return;
      }
      targetNode["title"] = newName;
    }
    updateDataList(tree);
    setTree([...tree]);
    document.removeEventListener("click", closeContextMenu);
  }

  function createNewFolder(name, targetNode, type, isLeaf = false) {
    setLoading(false);
    const newObj = {
      key: `${targetNode?.key}/${name}`,
      title: name,
      type,
    };
    if (isLeaf) {
      newObj["isLeaf"] = true;
    }
    newObj["icon"] = getIconByType(type);

    const targetObj = (targetNode["children"] ??= []);
    targetObj.push(newObj);
    setLoading(true);
  }

  function deleteFolder(targetNode) {
    setLoading(true);
    const parentNode = getParentNode(targetNode.key);
    if (parentNode.children) {
      parentNode["children"] = filterByKeys(
        parentNode.children,
        targetNode.key
      );
      isEmpty(parentNode.children) &&
        Reflect.deleteProperty(parentNode, "children");
    }
    setLoading(false);
  }

  function closeContextMenu() {
    if (targetElementRef.current) {
      targetElementRef.current.style.background = "unset";
    }
    setRightClickedItem(undefined);
  }

  function updateDataList(newTree) {
    generateList(newTree);
  }

  function getValidatedMenuItems(menuItems) {
    const { type, children } = rightClickedItem;
    if (["FULL_CODE"].includes(type)) {
      if (children?.length) {
        children.forEach((child) => {
          if (child.type === `${type}_MODEL`) {
            menuItems = filterByKeys(
              menuItems,
              TYPE_VS_CONTEXT_MENU_KEY_MAPPING[`${type}_MODEL`]
            );
          }
        });
      }
    }
    return menuItems;
  }

  function createModel(name, contextMenu) {
    contextMenu = {
      ...contextMenu,
      key: `${projectName}/models/no_code`,
    };
    setNewModalPopup(false);

    setLoading(true);
    name = name.replace(/\s+/g, "");
    messageApi.loading({
      key: "model create",
      content: "Creating model...",
      duration: 0,
    });
    expService
      .createModel(name, contextMenu.key, projectId)
      .then(() => {
        messageApi.success({
          key: "model create",
          content: "Model created",
        });
        const targetNode = getTargetNode(contextMenu.key, tree);
        createNewFolder(
          name,
          targetNode,
          CONTEXT_MENU_KEY_VS_TYPE_MAPPING[contextMenu.contextMenuKey],
          true
        );
        setExpandedKeys([...expandedKeys, targetNode.key]);
        closeNameModal();
        updateDataList(tree);
        setTree([...tree]);
        getExplorer(projectId);
        document.removeEventListener("click", closeContextMenu);
      })
      .catch((error) => {
        messageApi.destroy("model create");

        console.error(error);
        notify({ error });
      })
      .finally(() => {
        setLoading(false);
      });
  }

  function createFolder(name, contextMenu) {
    setLoading(true);
    messageApi.loading({
      key: "folder create",
      content: "Creating folder...",
      duration: 0,
    });
    expService
      .createFolder(name, contextMenu.key, projectId)
      .then(() => {
        messageApi.success({
          key: "folder create",
          content: "Folder created",
        });
        const targetNode = getTargetNode(contextMenu.key, tree);
        createNewFolder(
          name,
          targetNode,
          CONTEXT_MENU_KEY_VS_TYPE_MAPPING[contextMenu.contextMenuKey]
        );
        setExpandedKeys([...expandedKeys, targetNode.key]);
        closeNameModal();
        updateDataList(tree);
        setTree([...tree]);
        document.removeEventListener("click", closeContextMenu);
      })
      .catch((error) => {
        messageApi.destroy("folder create");
        console.error(error);
        notify({ error });
      })
      .finally(() => {
        setLoading(false);
      });
  }

  function rename(name, contextMenu, oldName = null) {
    setLoading(true);
    if (contextMenu.contextMenuKey === "seed_rename_csv") {
      const nameSplit = name.split(".");
      if (nameSplit.length !== 2 || nameSplit[1] !== "csv" || !nameSplit[0]) {
        notify({
          type: "error",
          message: "Failed to rename",
          description: "Unsupported file type. Please upload a '.csv' file.",
        });
        return;
      }
    }
    const path = contextMenu.key.split("/");
    // remove old name from path
    path.pop();
    // append path to new name
    const renamed = `${path.join("/")}/${name}`;
    messageApi.loading({
      key: "rename",
      content: "Renaming...",
      duration: 0,
    });
    expService
      .rename(renamed, contextMenu.key, projectId)
      .then(() => {
        messageApi.success({
          key: "rename",
          content: "Renamed",
        });

        if (oldName) {
          setRenamedModel({
            oldName,
            newName: name,
          });
        }
        const targetNode = getTargetNode(contextMenu.key, tree);
        targetNode["key"] = renamed;
        targetNode["title"] = name;
        const projTabs = projectDetails[projectId];
        const focused = projTabs?.focussedTab;
        const opened = projTabs?.openedTabs;
        if (opened?.length) {
          if (focused.key === contextMenu.key) {
            makeActiveTab({ key: renamed, type: focused.type });
          }
          const openTab = opened.map((el) => {
            const tabName = contextMenu.key.split("/").pop();
            const newname = renamed.split("/").pop();
            if (el.key.split("/").pop() === tabName) {
              return {
                ...el,
                extension: newname,
                label: newname,
                key: renamed,
              };
            } else {
              return el;
            }
          });
          setOpenedTabs(openTab);
        }
        closeNameModal();
        setSelectedKey(`${renamed}`);
        getExplorer(projectId);
        document.removeEventListener("click", closeContextMenu);
      })
      .catch((error) => {
        messageApi.destroy("rename");
        console.error(error);
        notify({ error });
      })
      .finally(() => {
        setLoading(false);
      });
  }

  function openFileUpload(contextMenu) {
    contextMenuRef.current = contextMenu;
    setRightClickedItem(contextMenu);
    setIsModalVisible(true);
    setTimeout(() => {
      if (uploadRef.current) {
        // Assuming you want to trigger the file input click
        const inputElement =
          uploadRef.current.querySelector('input[type="file"]');
        if (inputElement) {
          inputElement.click();
        }
      }
    }, 400);
  }

  function onNameModalDone(contextMenu, name, oldName = null) {
    if (["nc_new_model"].includes(contextMenu.contextMenuKey)) {
      createModel(name.trim(), contextMenu);
      return;
    }
    if (["seed_new_folder"].includes(contextMenu.contextMenuKey)) {
      createFolder(name, contextMenu);
      return;
    }
    if (["nc_rename_model"].includes(contextMenu.contextMenuKey)) {
      rename(name, contextMenu, oldName);
    }
    if (
      ["seed_rename_folder", "seed_rename_csv"].includes(
        contextMenu.contextMenuKey
      )
    ) {
      rename(name + ".csv", contextMenu);
    }
  }

  function isValidFileType(file) {
    const fileType = file.type;
    const isValidFileType =
      fileType && getAllowedFilesTypes().includes(fileType);
    if (!isValidFileType) {
      messageApi.error({
        content: "This file type is not supported",
      });
      return Upload.LIST_IGNORE;
    }
    if (file.size > MAX_FILE_SIZE_BYTES) {
      messageApi.error({
        content: `File size exceeds ${MAX_FILE_SIZE_MB}MB!`,
      });
      return Upload.LIST_IGNORE;
    }
    return isValidFileType;
  }

  function getAllowedFilesTypes(
    menukey = contextMenuRef.current?.contextMenuKey
  ) {
    return getAllowedFileByKey(menukey);
  }

  const onUpload = (info) => {
    const { fileList: updatedFileList } = info;
    setFileList(updatedFileList);

    if (info.file.status === "uploading") {
      setUploading(true);
      messageApi.loading({
        key: "upload",
        content: "Uploading...",
        duration: 0,
      });
    } else if (info.file.status === "done") {
      const targetNode = getTargetNode(contextMenuRef.current?.key, tree);
      createNewFolder(
        info.file.name,
        targetNode,
        CONTEXT_MENU_KEY_VS_TYPE_MAPPING[
          contextMenuRef.current?.contextMenuKey
        ],
        true
      );
      setExpandedKeys([...expandedKeys, contextMenuRef.current?.key]);
      updateDataList(tree);
      setTree([...tree]);

      // Check if all files have finished (done or error)
      const allSettled = updatedFileList.every(
        (f) => f.status === "done" || f.status === "error"
      );
      if (allSettled) {
        const uploadedPath = contextMenuRef.current?.key.split("/")?.at(-1);
        const doneCount = updatedFileList.filter(
          (f) => f.status === "done"
        ).length;
        messageApi.success({
          key: "upload",
          content: `${doneCount} file${
            doneCount > 1 ? "s" : ""
          } uploaded successfully${uploadedPath ? ` on ${uploadedPath}` : ""}`,
        });
        setUploading(false);
        setIsModalVisible(true);
        document.removeEventListener("click", closeContextMenu);
        getExplorer(projectId);
      }
    } else if (info.file.status === "error") {
      messageApi.destroy("upload");
      const errMsg = {
        response: {
          data: {
            error_message: info.file.response?.error_message,
            is_markdown: true,
          },
        },
      };
      notify({
        error: errMsg,
        renderMarkdown: true,
      });

      // Check if all files have finished (done or error)
      const allSettled = updatedFileList.every(
        (f) => f.status === "done" || f.status === "error"
      );
      if (allSettled) {
        setUploading(false);
        getExplorer(projectId);
      }
    }
  };

  const runSeed = useCallback(
    async (fileName, prjId = projectId) => {
      // If already running or missing required data, do nothing
      if (!prjId || !currentSchema || seedRunningRef.current) {
        return;
      }

      try {
        setSeedRunning(true);
        setLoading(true);
        messageApi.loading({
          key: "seed",
          content: "Running seed",
          duration: 0,
        });

        await expService.runSeed(prjId, fileName, currentSchema);

        messageApi.success({
          key: "seed",
          content: "Seed executed successfully",
        });

        await getExplorer(prjId);
      } catch (error) {
        messageApi.destroy("seed");
        console.error(error);
        notify({ error });
      } finally {
        setLoading(false);
        setSeedRunning(false);
      }
    },
    [
      projectId,
      currentSchema,
      seedRunning,
      expService,
      messageApi,
      notify,
      getExplorer,
    ]
  );

  function closeNameModal() {
    setOpenNameModal(false);
    setActiveMenu("");
  }

  function onDeleteModalDone(contextMenu) {
    if (
      ["seed_delete_folder", "seed_delete_csv", "nc_delete_model"].includes(
        contextMenu.contextMenuKey
      )
    ) {
      setLoading(true);
      messageApi.loading({
        key: "delete",
        content: "Deleting...",
        duration: 0,
      });
      expService
        .deleteFolder(
          projectId,
          contextMenu.key,
          contextMenu.contextMenuKey === "nc_delete_model"
            ? {
                type: contextMenu.type,
                checked: checked,
              }
            : undefined
        )
        .then(() => {
          messageApi.success({
            key: "delete",
            content: "Deleted",
          });

          // Handle "delete all" differently - no need to update individual nodes
          if (contextMenu.type === "all") {
            const projTabs = projectDetails[projectId];
            const opened = projTabs?.openedTabs;
            if (opened?.length) {
              const openTab = opened.filter((el) => {
                if (contextMenu.contextMenuKey === "seed_delete_csv") {
                  return el.type !== "SEED_CSV_FILE";
                } else {
                  return el.type !== "NO_CODE_MODEL";
                }
              });
              setOpenedTabs(openTab);
              if (openTab?.length) {
                const { key, type } = openTab[0];
                makeActiveTab({ key, type });
              }
            }
            closeDeleteModal();
            document.removeEventListener("click", closeContextMenu);
            getExplorer(projectId);
          } else if (contextMenu.type === "multiple") {
            const projTabs = projectDetails[projectId];
            const opened = projTabs?.openedTabs;
            if (opened?.length) {
              const openTab = opened.filter(
                (el) => !contextMenu.key?.includes(el.key)
              );
              setOpenedTabs(openTab);
              if (openTab?.length) {
                const { key, type } = openTab[0];
                makeActiveTab({ key, type });
              }
            }
            // Optimistically update refs and raw data, then rebuild tree
            // so the UI shows correct state immediately (no flash)
            rawTreeDataRef.current = rawTreeDataRef.current.map((node) => {
              if (
                contextMenu.contextMenuKey === "seed_delete_csv" &&
                node.title === "seeds"
              ) {
                return {
                  ...node,
                  children: (node.children || []).filter(
                    (child) => !contextMenu.key?.includes(child.key)
                  ),
                };
              }
              if (
                contextMenu.contextMenuKey === "nc_delete_model" &&
                node.title === "models"
              ) {
                return {
                  ...node,
                  children: (node.children || []).map((child) => {
                    if (child.title === "no_code") {
                      return {
                        ...child,
                        children: (child.children || []).filter(
                          (m) => !contextMenu.key?.includes(m.key)
                        ),
                      };
                    }
                    return child;
                  }),
                };
              }
              return node;
            });
            // Reset the appropriate delete mode refs
            if (contextMenu.contextMenuKey === "seed_delete_csv") {
              selectedSeedKeysRef.current = [];
              seedDeleteModeRef.current = false;
              setSelectedSeedKeys([]);
              setSeedDeleteMode(false);
            } else if (contextMenu.contextMenuKey === "nc_delete_model") {
              selectedModelKeysRef.current = [];
              modelDeleteModeRef.current = false;
              setSelectedModelKeys([]);
              setModelDeleteMode(false);
            }
            closeDeleteModal();
            rebuildTree();
            document.removeEventListener("click", closeContextMenu);
            getExplorer(projectId);
          } else {
            // Handle single file/folder deletion
            const targetNode = getTargetNode(contextMenu.key[0], tree);
            deleteFolder(targetNode);
            closeDeleteModal();
            updateDataList(tree);
            setTree([...tree]);
            onDelete(targetNode);
            document.removeEventListener("click", closeContextMenu);
            getExplorer(projectId);
          }
        })
        .catch((error) => {
          messageApi.destroy("delete");
          console.error(error);
          notify({ error });
        })
        .finally(() => {
          setLoading(false);
        });
    }
  }

  function closeDeleteModal() {
    setOpenDeleteModal(false);
    setActiveMenu("");
  }

  useEffect(() => {
    if (openNewModalPopup) {
      setOpenNameModal({ contextMenuKey: "nc_new_model" });
      setNewModalPopup(false);
    }
  }, [openNewModalPopup]);

  const handleAddNewSchema = () => {
    if (!newSchemaName.trim()) {
      notify({
        type: "error",
        message: "Schema name cannot be empty",
        description: "Please enter a valid schema name",
      });
      return;
    }

    setLoading(true);
    expService
      .setProjectSchema(projectId, newSchemaName)
      .then(() => {
        // Immediately update the UI with the new schema
        setSchemaMenu((prevMenu) => {
          const newItems = [...prevMenu];
          // Remove the last item (+ New button)
          newItems.pop();
          // Add the new schema
          newItems.push({
            label: newSchemaName,
            key: newSchemaName,
          });
          // Add back the + New button
          newItems.push({
            label: <Typography.Text className="flex">+ New</Typography.Text>,
            key: "add-new-schema",
          });
          return newItems;
        });

        // Set as current schema
        setCurrentSchema(newSchemaName);

        notify({
          type: "success",
          message: "Schema created successfully",
        });
        setIsSchemaModalOpen(false);
        setNewSchemaName("");

        // Refresh the schema list to ensure everything is in sync
        getSchemas();
      })
      .catch((error) => {
        console.error(error);
        notify({ error });
      })
      .finally(() => {
        setLoading(false);
      });
  };

  const handleSchemaChange = (key) => {
    if (key === "add-new-schema") {
      setIsSchemaModalOpen(true);
      return;
    }

    setLoading(true);
    expService
      .setProjectSchema(projectId, key)
      .then(() => {
        setCurrentSchema(key);
        notify({
          type: "success",
          message: "Schema updated successfully",
        });
      })
      .catch((error) => {
        console.error(error);
        notify({ error });
      })
      .finally(() => {
        setLoading(false);
      });
  };

  return (
    <ErrorBoundary>
      {messageContextHolder}
      <div className="ideResizeExplorer">
        <ResizerComponent ref={resizerRef} limit={1000} height="100%">
          <div id="explorerTree" className="ideExplorer">
            <div className="explorer-tabs">
              <Tabs
                size="small"
                tabPosition="left"
                className="height-100"
                activeKey={activeTab}
                onChange={setActiveTab}
                onTabClick={() => {
                  resizerRef.current?.expand();
                }}
                tabBarExtraContent={{
                  right: (
                    <Tooltip title="Docs" placement="right">
                      <div
                        className="explorer-docs-tab"
                        onClick={() =>
                          window.open(
                            "https://docs.visitran.com/",
                            "_blank",
                            "noopener,noreferrer"
                          )
                        }
                      >
                        <Icons.ReadOutlined />
                      </div>
                    </Tooltip>
                  ),
                }}
                items={EXPLORER_TAB_ITEMS.filter((el) => !el.isExternal).map(
                  (el, index) => {
                    // Data Flow tab (id: 3) gets special handling
                    if (el.id === 3) {
                      return {
                        label: <Tooltip title={el.label}>{el.icon}</Tooltip>,
                        key: el.id,
                        children: (
                          <div className="p-4 flex-direction-column height-100">
                            <div className="sql-flow-explorer-panel">
                              <div className="sql-flow-explorer-content">
                                <LinearScale
                                  style={{
                                    width: 48,
                                    height: 48,
                                    color: "#1890ff",
                                    marginBottom: 16,
                                  }}
                                />
                                <Typography.Title
                                  level={5}
                                  style={{ margin: 0 }}
                                >
                                  Data Flow
                                </Typography.Title>
                                <Typography.Text
                                  type="secondary"
                                  style={{ textAlign: "center", marginTop: 8 }}
                                >
                                  View table relationships and JOIN connections
                                  across all your models
                                </Typography.Text>
                                <Button
                                  type="primary"
                                  icon={
                                    <LinearScale
                                      style={{ width: 14, height: 14 }}
                                    />
                                  }
                                  onClick={() => {
                                    onSelect({
                                      key: "sql-flow",
                                      node: {
                                        title: "Data Flow",
                                        type: "SQL_FLOW",
                                      },
                                    });
                                  }}
                                  style={{ marginTop: 16 }}
                                >
                                  Open Data Flow
                                </Button>
                              </div>
                            </div>
                          </div>
                        ),
                      };
                    }
                    return {
                      label: <Tooltip title={el.label}>{el.icon}</Tooltip>,
                      key: el.id,
                      children: (
                        <div className="p-4 flex-direction-column height-100">
                          <div className="explorerHeader">
                            <Search
                              placeholder="Search"
                              onChange={onSearchDebounce}
                              allowClear
                            />
                            <div className="ml-10">
                              <Tooltip title="Refresh" placement="bottom">
                                <Button
                                  shape="circle"
                                  onClick={() => {
                                    onRefreshDebounce(projectId, true);
                                  }}
                                >
                                  <ReloadOutlined spin={loading} />
                                </Button>
                              </Tooltip>
                            </div>
                          </div>
                          <div className="tree-container">
                            {index === 0 ? (
                              <DirectoryTree
                                size="small"
                                treeData={tree}
                                switcherIcon={<CaretDownOutlined />}
                                selectedKeys={[selectedKey]}
                                expandedKeys={expandedKeys}
                                autoExpandParent={autoExpandParent}
                                onExpand={onExpand}
                                expandAction={false}
                                onSelect={onNodeSelect}
                                loadData={onLoadData}
                                onRightClick={setContextMenu}
                                rootStyle={{
                                  pointerEvents: rightClickedItem
                                    ? "none"
                                    : "auto",
                                }}
                                rootClassName="explorerTree"
                                disabled={previewTimeTravel || loading}
                              />
                            ) : dbLoading ? (
                              <SpinnerLoader />
                            ) : (
                              <DirectoryTree
                                size="small"
                                treeData={dbExplorer}
                                selectedKeys={[selectedKey]}
                                switcherIcon={<CaretDownOutlined />}
                                expandedKeys={expandedKeys}
                                autoExpandParent={autoExpandParent}
                                onSelect={onNodeSelect}
                                onExpand={onExpand}
                                expandAction={false}
                                rootClassName="explorerTree"
                                disabled={previewTimeTravel}
                              />
                            )}
                          </div>
                        </div>
                      ),
                    };
                  }
                )}
              />
            </div>
            <div className="expDbPreview">
              <DbConfigPreview />
            </div>
            {getContextMenu()}
            {openNameModal && (
              <LazyLoadComponent
                component={() => import("./name-modal.jsx")}
                componentName="NameModal"
                name={
                  openNameModal?.contextMenuKey?.includes("rename")
                    ? openNameModal?.title
                    : ""
                }
                contextMenu={openNameModal}
                onDone={onNameModalDone}
                onClose={closeNameModal}
              />
            )}
            {openDeleteModal && (
              <LazyLoadComponent
                component={() => import("./delete-modal.jsx")}
                componentName="DeleteModal"
                contextMenu={openDeleteModal}
                onDelete={onDeleteModalDone}
                onClose={closeDeleteModal}
                loading={loading}
                checked={checked}
                setChecked={setChecked}
              />
            )}
            <div ref={uploadRef} className="upload-container">
              <Modal
                title={
                  <div
                    style={{
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "space-between",
                    }}
                  >
                    <span>Upload Seed File</span>
                    {uploading && (
                      <span style={{ fontSize: "12px", color: "#1890ff" }}>
                        Uploading... Please wait
                      </span>
                    )}
                  </div>
                }
                open={isModalVisible}
                onCancel={handleModalCancel}
                footer={
                  <div
                    style={{
                      textAlign: "right",
                      display: "flex",
                      gap: "8px",
                      justifyContent: "flex-end",
                    }}
                  >
                    <Tooltip
                      placement="top"
                      title={
                        seedRunning
                          ? "Seed is currently running"
                          : uploading ||
                            fileList.some((file) => file.status === "uploading")
                          ? "File upload in progress"
                          : !fileList.some((file) => file.status === "done")
                          ? "Please upload file(s) first"
                          : "Run Seed"
                      }
                    >
                      <Button
                        onClick={handleRunSeedAndClose}
                        disabled={
                          uploading ||
                          seedRunning ||
                          fileList.some(
                            (file) => file.status === "uploading"
                          ) ||
                          !fileList.some((file) => file.status === "done")
                        }
                        type="primary"
                      >
                        Run Seed
                      </Button>
                    </Tooltip>
                    <Button
                      onClick={handleModalCancel}
                      disabled={
                        uploading ||
                        fileList.some((file) => file.status === "uploading")
                      }
                    >
                      Close
                    </Button>
                  </div>
                }
                maskClosable={!uploading}
                closable={!uploading}
              >
                <Upload.Dragger
                  name="file"
                  multiple={true}
                  action={`/api/v1/visitran/${
                    selectedOrgId || "default_org"
                  }/project/${projectId}/explorer/upload`}
                  accept={getAllowedFilesTypes()}
                  beforeUpload={isValidFileType}
                  onChange={onUpload}
                  fileList={fileList}
                  data={(file) => {
                    const path = contextMenuRef.current.key.split("/");
                    path.shift();
                    return {
                      file_name: `${path.join("/")}/${file.name}`,
                      schema_name: currentSchema,
                    };
                  }}
                  headers={{ "X-CSRFToken": csrfToken }}
                  showUploadList={true}
                  disabled={uploading}
                >
                  {(uploading ||
                    fileList.some((file) => file.status === "uploading")) && (
                    <div
                      style={{
                        marginBottom: "16px",
                        padding: "12px",
                        backgroundColor: "#f6ffed",
                        border: "1px solid #b7eb8f",
                        borderRadius: "6px",
                      }}
                    >
                      <div
                        style={{
                          display: "flex",
                          alignItems: "center",
                          justifyContent: "space-between",
                        }}
                      >
                        <span style={{ color: "#52c41a", fontWeight: "500" }}>
                          📤 Uploading files...
                        </span>
                        <span style={{ fontSize: "12px", color: "#666" }}>
                          {
                            fileList.filter(
                              (file) => file.status === "uploading"
                            ).length
                          }{" "}
                          file(s) uploading
                        </span>
                      </div>
                    </div>
                  )}
                  <p className="ant-upload-drag-icon">
                    <InboxOutlined />
                  </p>
                  <p className="ant-upload-text">
                    Click or drag file to this area to upload
                  </p>
                  <p className="ant-upload-hint">
                    Support for a single or bulk upload. Please upload the seed
                    with .csv file.
                  </p>
                  {fileList.length > 0 && (
                    <div className="recent-upload-text">
                      Recent Uploads <DownCircleOutlined />
                    </div>
                  )}
                </Upload.Dragger>
              </Modal>
            </div>
            <Modal
              title="Add New Schema"
              open={isSchemaModalOpen}
              onOk={handleAddNewSchema}
              centered
              onCancel={() => {
                setIsSchemaModalOpen(false);
                setActiveMenu("");
              }}
            >
              <Input
                placeholder="Enter new schema name"
                value={newSchemaName}
                onChange={(e) => setNewSchemaName(e.target.value)}
              />
            </Modal>
          </div>
        </ResizerComponent>
      </div>
    </ErrorBoundary>
  );
};
IdeExplorer.propTypes = {
  currentNode: PropTypes.string,
  onSelect: PropTypes.func,
  onDelete: PropTypes.func,
  openNewModalPopup: PropTypes.bool,
  setNewModalPopup: PropTypes.func,
  activeTab: PropTypes.number,
  setActiveTab: PropTypes.func,
};

function getMenuList(type) {
  return CONTEXT_MENU_LIST_MAPPING[type];
}

function calculateContextMenuPosition(targetX, targetY, count = 1) {
  const clientHeight = window.innerHeight;
  const left = targetX;
  let top = targetY;

  const contextMenuHeight = count * 50; // approx. one menu item height is 50px
  if (top + contextMenuHeight > clientHeight) {
    top = clientHeight - contextMenuHeight;
  }

  return { left, top };
}

function getTargetNode(key, treeData) {
  let targetNode;
  for (const node of treeData) {
    if (node.key === key) {
      targetNode = node;
      break;
    } else if (node.children) {
      const findTargetNode = getTargetNode(key, node.children);
      if (findTargetNode) {
        targetNode = findTargetNode;
        break;
      }
    }
  }
  return targetNode;
}

function filterByKeys(array, keys, keyInArrayToMatch = "key") {
  const key = keyInArrayToMatch;
  if (Array.isArray(keys)) {
    return array.filter((item) => !keys.includes(item[key]));
  } else {
    return array.filter((item) => item[key] !== keys);
  }
}

function transformTree(tree) {
  // transforming tree as per frontend need
  let type = "";
  tree.forEach((node) => {
    type = node.type;

    // apply icon
    node["icon"] = getIconByType(type);
    // change is_folder to isLeaf key and delete is_folder
    delete Object.assign(node, { isLeaf: !node.is_folder }).is_folder;

    // Indent child/reference models (only in Dependency Chain sort)
    if (node._isChild) {
      node.className = "explorer-child-model";
    } else if (node.className === "explorer-child-model") {
      node.className = "";
    }

    if (node.children) {
      transformTree(node.children);
    }
  });
}

IdeExplorer.displayName = "IdeExplorer";
export { IdeExplorer };
