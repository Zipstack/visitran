import {
  Button,
  Card,
  Checkbox,
  Divider,
  Dropdown,
  Empty,
  Input,
  Modal,
  Pagination,
  Radio,
  Select,
  Space,
  Table,
  Tabs,
  Tag,
  theme,
  Tooltip,
  Typography,
} from "antd";
import { useNavigate } from "react-router-dom";
import { useEffect, useMemo, useRef, useState } from "react";
import { Resizable } from "react-resizable";
import Cookies from "js-cookie";
import AnsiToHtml from "ansi-to-html";
import DOMPurify from "dompurify";
import yaml from "js-yaml";
import {
  CalendarOutlined,
  CheckCircleOutlined,
  CheckOutlined,
  ClearOutlined,
  DownloadOutlined,
  EditOutlined,
  DatabaseOutlined,
  InfoCircleOutlined,
  RollbackOutlined,
  FontColorsOutlined,
  NumberOutlined,
  ClockCircleOutlined,
  CloseCircleOutlined,
  CloseOutlined,
  ContainerOutlined,
  CopyOutlined,
  DatabaseFilled,
  ExclamationCircleFilled,
  EyeInvisibleOutlined,
  FilterOutlined,
  LineHeightOutlined,
  LinkOutlined,
  DownOutlined,
  MergeCellsOutlined,
  PlayCircleOutlined,
  PlusSquareOutlined,
  ProfileOutlined,
  RetweetOutlined,
  SwapOutlined,
  UndoOutlined,
} from "@ant-design/icons";
import { debounce, isEmpty, isEqual } from "lodash";
import { useImmer } from "use-immer";
import { produce } from "immer";
import PropTypes from "prop-types";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { Prism as SyntaxHighlighter } from "react-syntax-highlighter";
import { oneDark, vs } from "react-syntax-highlighter/dist/esm/styles/prism";
import { io } from "socket.io-client";
// import ReactDragListView from "react-drag-listview";
import ReactFlow, {
  Controls,
  ControlButton,
  MarkerType,
  Position,
  useEdgesState,
  useNodesState,
} from "reactflow";
import dagre from "dagre";

import { useAxiosPrivate } from "../../../service/axios-service.js";
import { NoCodeToolbar } from "../no-code-toolbar/no-code-toolbar.jsx";
import { NoCodeTopbar } from "../no-code-topbar/no-code-topbar.jsx";
import { ConfigureSourceDestination } from "../no-code-configuration/configure-source-destination.jsx";
import { ConfigureJoins } from "../no-code-configuration/configure-joins.jsx";
import { useProjectStore } from "../../../store/project-store.js";
import { useUserStore } from "../../../store/user-store.js";
import { orgStore } from "../../../store/org-store.js";
import { joinTableColors, THEME } from "../../../common/constants.js";
import {
  addIdToObjects,
  checkPermission,
  getBaseUrl,
  getRelativeTime,
  removeIdFromObjects,
  removeUnwantedKeys,
} from "../../../common/helpers.js";
import { EditorBottomSection } from "../bottom_section/bottom-section.jsx";
import {
  GenAI,
  Lineage,
  Logs,
  Tech,
  Time,
  OpenTab,
} from "../../../base/icons/index.js";
import { SpinnerLoader } from "../../../widgets/spinner_loader/index.js";
import { CopyableCell } from "../../../widgets/copyable-cell/index.js";
import "./no-code-model.css";
import "reactflow/dist/style.css";
import { useNotificationService } from "../../../service/notification-service.js";
import { useJobService } from "../../scheduler/service.js";
import { useTransformIdStore } from "../../../store/transform-id-store.js";
import {
  getCombineColumnsSpec,
  getGroupAndAggregationSpec,
  getJoinSpec,
  getRenameColumnSpec,
  getSynthesizeSpec,
  getWindowSpec,
  getTransformId,
  transformationTypes,
} from "./helper.js";
import { useRefreshModelsStore } from "../../../store/refresh-models-store.js";
import { useLineageTabStore } from "../../../store/lineage-tab-store.js";

import initialSpec from "../../../skeleton/initialSpec.json";

import Papa from "papaparse";

const { Text } = Typography;

const ResizableTitle = (props) => {
  const { onResize, width, ...restProps } = props;

  if (!width) {
    return <th {...restProps} />;
  }

  return (
    <Resizable
      width={width}
      height={0}
      handle={
        <span
          className="react-resizable-handle"
          onClick={(e) => e.stopPropagation()}
        />
      }
      onResize={onResize}
      draggableOpts={{ enableUserSelectHack: false }}
    >
      <th {...restProps} />
    </Resizable>
  );
};

ResizableTitle.propTypes = {
  onResize: PropTypes.func,
  width: PropTypes.number,
};

const LOG_LEVEL_RANK = { debug: 0, info: 1, warn: 2, warning: 2, error: 3 };
const LOG_LEVEL_COLOR = {
  debug: "#8c8c8c",
  info: undefined,
  warn: "#d48806",
  warning: "#d48806",
  error: "#cf1322",
};

function NoCodeModel({ nodeData }) {
  const { token } = theme.useToken();
  const axios = useAxiosPrivate();
  const csrfToken = Cookies.get("csrftoken");
  const sessionId = Cookies.get("sessionid");
  const userDetails = useUserStore((state) => state.userDetails);
  const { selectedOrgId } = orgStore();
  const {
    projectName,
    dbConfigDetails,
    projectDetails = {},
    setPreview,
    previewTimeTravel,
    projectId,
  } = useProjectStore();
  const ansiToHtml = new AnsiToHtml();
  const dsName = dbConfigDetails?.datasource_name;
  const isSchemaExists = dbConfigDetails?.is_schema_exists ?? false;
  const [columns, setColumns] = useState([]);
  const [spec, setSpec] = useImmer(initialSpec);
  const [openFormula, setOpenFormula] = useState(false);
  const [columnDetails, setColumnDetails] = useState({
    columns: {},
    dataTypes: [],
    allColumnsDetails: {},
    sql: "",
    schema_name: "",
  });

  const can_write = checkPermission("DATA_TRANSFORMATION", "can_write");

  const [editingColumn, setEditingColumn] = useState(null);
  const [newColumnName, setNewColumnName] = useState("");
  const [orginalColName, setorginalColName] = useState([]);
  const [isDuplicate, setIsDuplicate] = useState(false);
  const [formulaColumns, setFormulaColumns] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const [isExportingCsv, setIsExportingCsv] = useState(false);
  const [openedModal, setOpenedModal] = useState(null);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [reference, setReference] = useState(initialSpec.reference);
  const [source, setSource] = useImmer(initialSpec.source);
  const [model, setModel] = useImmer(initialSpec.model);
  const [joins, setJoins] = useImmer([]);
  const [joinValidationError, setJoinValidationError] = useState(false);
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(50);
  const [currentData, setCurrentData] = useState([]);
  const [totalCount, setTotalCount] = useState(0);
  const [clientHeight, setClientHeight] = useState(0);
  const [lineageData, setLineageData] = useState();
  const [infoStack, setInfoStack] = useState([]);
  const [nodes, setNodes, onNodesChange] = useNodesState();
  const [edges, setEdges, onEdgesChange] = useEdgesState();
  const [lineageNodeCount, setLineageNodeCount] = useState(0); // Track node count for zoom
  const [lineageLayoutDirection, setLineageLayoutDirection] = useState("LR"); // LR = horizontal, TB = vertical
  const [seqLineageData, setSeqLineageData] = useState();
  const [seqNodes, setSeqNodes, onSeqNodesChange] = useNodesState();
  const [seqEdges, setSeqEdges, onSeqEdgesChange] = useEdgesState();
  const [reactFlowInstance, setReactFlowInstance] = useState(null);
  const [logsInfo, setLogsInfo] = useState([]);
  const [logsLevel, setLogsLevel] = useState(
    () => localStorage.getItem("visitran.logsLevel") || "info"
  );
  const [reveal, setReveal] = useState(false);
  const [seqOrder, setSeqOrder] = useState({});
  const [specRevert, setSpecRevert] = useState(false);
  const [configApply, setConfigApply] = useState(false);
  const [columnWidths, setColumnWidths] = useState({});
  const seqBackup = useRef();
  const prevTabKey = useRef();
  const [transformationErrorFlag, setTransformationErrorFlag] = useState(false);
  const [selectedFormulaCol, setSelectedFormulaCol] = useState({
    column: "",
    formula: "",
  });
  const containerRef = useRef(null);
  const bottomSectionRef = useRef({ height: "200px" });
  const { notify, closeAll } = useNotificationService();
  const { transformIds, setTransformIds } = useTransformIdStore();
  const { refreshModels, setRefreshModels } = useRefreshModelsStore();
  const { renamedModel, setRenamedModel } = useProjectStore();
  const { setPendingLineageTab } = useLineageTabStore();
  const axiosPrivate = useAxiosPrivate();
  const navigate = useNavigate();
  const {
    listDeployCandidates,
    runTaskForModel,
    runTask,
    listRecentRunsForModel,
  } = useJobService();

  const [quickDeployModal, setQuickDeployModal] = useState({
    open: false,
    step: "loading", // loading | empty | confirm | pick
    candidates: [],
    selectedTaskId: null,
    selectedScope: null, // "model" | "job"
    confirmed: false,
    submitting: false,
  });
  const [recentRunsState, setRecentRunsState] = useState({
    loading: false,
    runs: [],
    fetchedFor: null, // model name the current runs are for
  });

  const modelName =
    nodeData?.node?.title ||
    projectDetails[projectId]?.["focussedTab"]?.key?.split("/").pop();

  const dataTypeIcon = {
    String: <FontColorsOutlined />,
    Number: <NumberOutlined />,
    Time: <ClockCircleOutlined />,
    Date: <CalendarOutlined />,
    DateTime: <CalendarOutlined />,
    Boolean: (
      <>
        <CloseCircleOutlined />
        <CheckCircleOutlined />
      </>
    ),
    Float: (
      <>
        <NumberOutlined />.
        <NumberOutlined />
        <NumberOutlined />
      </>
    ),
  };

  const handleToggleLineageLayout = () => {
    const newDirection = lineageLayoutDirection === "LR" ? "TB" : "LR";
    setLineageLayoutDirection(newDirection);

    // Re-layout existing nodes with new direction
    if (lineageData?.nodes && lineageData?.edges) {
      const { nodes: layoutedNodes, edges: layoutedEdges } =
        getLayoutedElements(lineageData.nodes, lineageData.edges, newDirection);
      setNodes(layoutedNodes);
      setEdges(layoutedEdges);
    }
  };

  useEffect(() => {
    setTransformIds(spec);
  }, [spec]);

  // Suppress React Flow development warnings
  useEffect(() => {
    const originalWarn = console.warn;
    console.warn = (...args) => {
      if (
        args[0]?.includes?.("[React Flow]") &&
        args[0]?.includes?.("width and a height")
      ) {
        return; // Suppress this specific warning
      }
      originalWarn.apply(console, args);
    };

    return () => {
      console.warn = originalWarn;
    };
  }, []);

  // Track node count changes (not position changes during drag)
  useEffect(() => {
    const currentLength = nodes?.length || 0;
    if (currentLength !== lineageNodeCount) {
      setLineageNodeCount(currentLength);
    }
  }, [nodes?.length, lineageNodeCount]);

  // Handle zoom adjustment for small node counts in lineage
  // Only triggers when node count changes, not on drag operations
  useEffect(() => {
    if (reactFlowInstance && lineageNodeCount > 0) {
      setTimeout(() => {
        if (lineageNodeCount <= 3) {
          // For 1-3 nodes, set zoom to 70% instead of fitView
          reactFlowInstance.fitView();
          reactFlowInstance.zoomTo(0.7);
        } else {
          // For more than 3 nodes, use normal fitView
          reactFlowInstance.fitView();
        }
      }, 100);
    }
  }, [lineageNodeCount, reactFlowInstance]);

  useEffect(() => {
    setJoins(
      addIdToObjects(
        getJoinSpec(spec?.transform, transformIds?.JOIN)?.tables || []
      )
    );
  }, [spec, transformIds]);

  useEffect(() => {
    // Early return if renamedModel is empty or doesn't have required properties
    if (!renamedModel?.oldName || !renamedModel?.newName) return;

    // Early return if reference doesn't include the old name
    if (!reference?.includes(renamedModel.oldName)) return;

    // Update references
    const updatedRefs = reference.map((ref) =>
      ref === renamedModel.oldName ? renamedModel.newName : ref
    );

    // Update both reference state and spec in one go
    setReference(updatedRefs);
    updateSpec({ ...spec, reference: updatedRefs });
    setRenamedModel({});
  }, [renamedModel]);

  const handleElementClick = debounce((node) => {
    setPreview(true);
    const newSpec = produce(spec, (draft) => {
      draft.transform.preview = {
        preview_at: node,
        preview_enabled: true,
      };
    });
    updateSpec(newSpec);
  }, 500);

  const disablePreview = () => {
    setPreview(false);
    const newSpec = produce(spec, (draft) => {
      draft.transform.preview = {
        preview_at: null,
        preview_enabled: false,
      };
    });
    updateSpec(newSpec);
  };
  const parseLog = (log) =>
    DOMPurify.sanitize(ansiToHtml.toHtml(log), {
      ALLOWED_TAGS: ["span", "br"],
      ALLOWED_ATTR: ["style"],
    });

  const filteredLogs = useMemo(
    () =>
      logsInfo.filter(
        (entry) =>
          (LOG_LEVEL_RANK[entry.level] ?? 1) >= (LOG_LEVEL_RANK[logsLevel] ?? 1)
      ),
    [logsInfo, logsLevel]
  );
  const handleLogsLevelChange = (value) => {
    setLogsLevel(value);
    localStorage.setItem("visitran.logsLevel", value);
  };

  const hideGenAIAndTimeTravelTabs = true;
  const BOTTOM_TABS = [
    {
      label: (
        <div className="flex-align-center">
          <Lineage />
          <Text className="ml-5">Lineage</Text>
        </div>
      ),
      key: "lineage",
      children: (
        <>
          {!lineageData && <SpinnerLoader />}
          {lineageData && !lineageData.nodes && (
            <Text>Error in fetching lineage data</Text>
          )}
          {lineageData?.nodes && (
            <div className="flex-direction-column height-100 overflow-hidden">
              <div className="flex-space-between pad-8">
                <Space>
                  <LineageInfo
                    helpText="&nbsp;Parent / Independent model"
                    color="#00A6ED"
                  />
                  <LineageInfo helpText="&nbsp;Derived model" color="#E8A400" />
                  <LineageInfo
                    helpText="&nbsp;Terminal Nodes"
                    color="#FF889D"
                  />
                </Space>
                <Tooltip title="Open as Tab">
                  <span
                    className="open-tab-icon"
                    onClick={() => {
                      setPendingLineageTab({
                        key: "lineage-tab",
                        title: "Lineage",
                      });
                    }}
                  >
                    <OpenTab
                      style={{ width: 20, height: 20, cursor: "pointer" }}
                    />
                  </span>
                </Tooltip>
              </div>
              <div className="flex-1 overflow-y-auto">
                <ReactFlow
                  nodes={nodes}
                  edges={edges}
                  onNodesChange={onNodesChange}
                  onEdgesChange={onEdgesChange}
                  // panOnDrag={false}
                  // zoomOnScroll={false}set
                  onInit={setReactFlowInstance}
                  snapToGrid={true}
                  onNodeClick={(event, node) => {
                    handleInfoClick(node);
                  }}
                >
                  <div className="react-flow">
                    <Controls position="top-left" orientation="horizontal">
                      <ControlButton
                        onClick={handleToggleLineageLayout}
                        title={
                          lineageLayoutDirection === "LR"
                            ? "Switch to vertical layout"
                            : "Switch to horizontal layout"
                        }
                      >
                        <SwapOutlined
                          rotate={lineageLayoutDirection === "LR" ? 0 : 90}
                        />
                      </ControlButton>
                    </Controls>
                  </div>
                </ReactFlow>
              </div>

              <div className="info-stack-container">
                {infoStack.map((box, index) => {
                  const isDarkTheme = userDetails.currentTheme === THEME.DARK;
                  return (
                    <div
                      key={box.id}
                      className={`info-box stacked ${
                        isDarkTheme ? "dark-theme" : ""
                      }`}
                      style={{
                        zIndex:
                          1000 - index /* Higher index = lower in stack */,
                        borderColor: box.borderColor,
                        borderWidth: "2px",
                        borderStyle: "solid",
                      }}
                      onClick={() => bringToFront(box.id)}
                    >
                      <div className="info-box-header">
                        <div className="info-box-title">
                          {/* Find the node that corresponds to this info box */}
                          {nodes.find((n) => n.id === box.id)?.data
                            ?.typeIcon && (
                            <div
                              className={
                                nodes.find((n) => n.id === box.id)?.data
                                  ?.iconStyleClass || "icon_style"
                              }
                            >
                              {
                                nodes.find((n) => n.id === box.id)?.data
                                  ?.typeIcon
                              }
                            </div>
                          )}
                          <span className="info-box-title-text">
                            {box.title}
                          </span>
                        </div>
                        <Button
                          type="text"
                          size="small"
                          className="info-box-close"
                          icon={<CloseOutlined />}
                          onClick={(e) => {
                            e.stopPropagation(); // Prevent triggering the box click
                            setInfoStack((prev) =>
                              prev.filter((b) => b.id !== box.id)
                            );
                          }}
                          style={{
                            color: isDarkTheme
                              ? "rgba(255, 255, 255, 0.65)"
                              : "rgba(0, 0, 0, 0.45)",
                            transition: "color 0.3s",
                            padding: "4px",
                            display: "flex",
                            alignItems: "center",
                            justifyContent: "center",
                          }}
                        />
                      </div>

                      <div className="info-box-section">
                        <div className="info-box-section-label">
                          Source Table
                        </div>
                        <div className="info-box-section-content">
                          {box.content.sourceTable}
                        </div>
                      </div>

                      <div className="info-box-section">
                        <div className="info-box-section-label">
                          Join Tables
                        </div>
                        <div className="info-box-section-content">
                          {box.content.joinTables || "-"}
                        </div>
                      </div>

                      <div className="info-box-section">
                        <div
                          className="info-box-section-label"
                          style={{
                            display: "flex",
                            justifyContent: "space-between",
                            alignItems: "center",
                          }}
                        >
                          <span>SQL Query</span>
                          <Tooltip title="Copy SQL">
                            <CopyOutlined
                              style={{ cursor: "pointer" }}
                              onClick={(e) => {
                                e.stopPropagation();
                                navigator.clipboard.writeText(
                                  box.content.sqlQuery
                                );
                                notify({
                                  type: "success",
                                  message: "SQL Copied",
                                  description: "SQL query copied to clipboard",
                                });
                              }}
                            />
                          </Tooltip>
                        </div>
                        <div
                          className="info-box-section-content"
                          style={{
                            maxHeight: "200px",
                            overflowY: "auto",
                            borderRadius: "4px",
                            border: isDarkTheme
                              ? "1px solid #303030"
                              : "1px solid #e8e8e8",
                          }}
                        >
                          <SyntaxHighlighter
                            language="sql"
                            style={isDarkTheme ? oneDark : vs}
                            customStyle={{
                              margin: 0,
                              padding: "8px",
                              fontSize: "12px",
                              borderRadius: "4px",
                              backgroundColor: "transparent",
                            }}
                            wrapLines={true}
                            wrapLongLines={true}
                          >
                            {box.content.sqlQuery}
                          </SyntaxHighlighter>
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}
        </>
      ),
    },
    {
      label: (
        <div className="flex-align-center">
          <Time />
          <Text className="ml-5">Time Travel</Text>
        </div>
      ),
      key: "Time Travel",
      disabled: true,
      children: (
        <>
          {!seqLineageData && <SpinnerLoader />}
          {seqLineageData && !seqLineageData.nodes && (
            <Text>Error in fetching lineage data</Text>
          )}
          {seqLineageData?.nodes && (
            <div
              className="lineageSection"
              style={{
                height: `calc(${bottomSectionRef.current.height} - 70px)`,
              }}
            >
              <ReactFlow
                nodes={seqNodes}
                edges={seqEdges}
                onNodesChange={onSeqNodesChange}
                onEdgesChange={onSeqEdgesChange}
              ></ReactFlow>
            </div>
          )}
        </>
      ),
    },
    {
      label: (
        <div className="flex-align-center">
          <GenAI />
          <Text className="ml-5">Gen AI</Text>
        </div>
      ),
      key: "gen_ai",
      children: "Gen AI data",
      disabled: true,
    },
    {
      label: (
        <div className="flex-align-center">
          <Logs />
          <Text className="ml-5">Logs</Text>
        </div>
      ),
      key: "logs",
      children: (
        <>
          <div
            style={{
              display: "flex",
              justifyContent: "flex-end",
              padding: "4px 8px",
              borderBottom: `1px solid ${token.colorBorderSecondary}`,
            }}
          >
            <Select
              size="small"
              value={logsLevel}
              onChange={handleLogsLevelChange}
              style={{ width: 140 }}
              options={[
                { value: "debug", label: "All logs" },
                { value: "info", label: "Info & above" },
                { value: "warn", label: "Warnings & errors" },
                { value: "error", label: "Errors only" },
              ]}
            />
          </div>
          <div
            className="logsSection"
            style={{
              height: `calc(${bottomSectionRef.current.height} - 100px)`,
            }}
          >
            {filteredLogs.map((el, index) => (
              <div
                key={index}
                style={{ color: LOG_LEVEL_COLOR[el.level] }}
                dangerouslySetInnerHTML={{ __html: parseLog(el.message) }}
              />
            ))}
          </div>
        </>
      ),
    },
  ].filter((tab) => {
    if (
      hideGenAIAndTimeTravelTabs &&
      (tab.key === "Time Travel" || tab.key === "gen_ai")
    ) {
      return false;
    }
    return true;
  });

  const handleModalClose = (type, okText) => {
    if (type !== "ok") {
      switch (openedModal) {
        case "sourceDestination":
          setReference(spec?.reference);
          setModel(spec?.model);
          setSource(spec?.source);
          break;
        case "joins": {
          // Reset joins back to the original spec when user cancels
          const originalJoins =
            getJoinSpec(spec?.transform, transformIds?.JOIN)?.tables || [];
          setJoins(addIdToObjects(originalJoins));
          break;
        }
      }
    }
    if (okText === "I Understand") {
      setConfigApply(true);
      setIsModalOpen(false);
      // This is done to avoid glitching of modal when closing
      setTimeout(() => {
        setOpenedModal(null);
      }, 100);
    } else {
      setIsModalOpen(false);
      // This is done to avoid glitching of modal when closing
      setTimeout(() => {
        setOpenedModal(null);
      }, 100);
    }
  };

  const handleSourceDestinationChange = async () => {
    setIsLoading(true);
    const requestOptions = {
      method: "POST",
      url: `/api/v1/visitran/${
        selectedOrgId || "default_org"
      }/project/${projectId}/no_code_model/${nodeData.node.title}/set-model`,
      headers: {
        "X-CSRFToken": csrfToken,
      },
      data: {
        model_name: nodeData.node.title,
        model_config: {
          source,
          model,
        },
        reference_config: reference,
      },
    };

    axiosPrivate(requestOptions)
      .then((res) => {
        updateSpec(res?.data?.model_data);
        seqBackup.current = res?.data?.sequence_orders;
        const linData = res?.data?.sequence_lineage.data;
        const lineageSeqdata = transformLineageData(
          linData,
          "sequence",
          handleElementClick
        );
        setSeqLineageData(linData);
        const { nodes: layoutedNodes, edges: layoutedEdges } =
          getLayoutedElements(lineageSeqdata.nodes, lineageSeqdata.edges);
        setSeqNodes(layoutedNodes);
        setSeqEdges(layoutedEdges);
        runTransformation(res?.data?.model_data);
        setConfigApply(true);
        handleModalClose("ok");
      })
      .catch((error) => {
        notify({ error });
        setIsLoading(false);
        handleModalClose();
      });
  };

  const handleJoinsChange = async () => {
    const joinsRemovedId = removeIdFromObjects(joins);

    let result = {};
    if (joinsRemovedId?.length === 0) {
      const body = {
        step_id: transformIds?.JOIN,
      };
      result = await handleDeleteTransformation(body);
    } else {
      const body = {
        type: "join",
        join: { tables: removeIdFromObjects(joins) },
      };
      result = await saveTransformation(body, transformIds?.JOIN);
    }
    if (result?.status === "success") {
      updateSpec(result?.spec);
      handleModalClose("ok");
    } else {
      handleModalClose();
    }
  };

  const handleOk = async (okText) => {
    if (okText === "Apply") {
      setConfigApply(true);
    }

    switch (openedModal) {
      case "sourceDestination":
        if (spec?.source?.table_name && !isEqual(spec?.source, source)) {
          setOpenedModal("sourceDestinationChangeAlert");
        } else {
          handleSourceDestinationChange();
        }
        break;
      case "joins":
        handleJoinsChange();
        break;
      case "sourceDestinationChangeAlert":
        handleSourceDestinationChange();
        break;
      case "clearTransformsAlert": {
        const result = await handleDeleteTransformation({ clear_all: true });
        if (result?.status === "success") {
          updateSpec(result?.spec);
          setJoins([]);
          handleModalClose("ok");
          setReveal(false);
          handleCancel();
        }
      }
    }
  };

  const handleModalOpen = (value) => {
    setOpenedModal(value);
    setIsModalOpen(true);

    if (value === "joins") {
      handleGetColumns(transformIds?.JOIN, transformationTypes?.JOIN);
    }
  };

  const getLocale = () => {
    let description = null;
    if (!spec?.source?.table_name && !isLoading) {
      description = (
        <div>
          <Button
            type="link"
            onClick={() => handleModalOpen("sourceDestination")}
          >
            Source table not configured
          </Button>
        </div>
      );
    }
    if (isLoading) {
      description = "Loading data";
    }
    if (description) {
      return {
        emptyText: (
          <Empty
            image={Empty.PRESENTED_IMAGE_SIMPLE}
            description={description}
          />
        ),
      };
    }
    return {};
  };

  function getTableName(schema, table) {
    if (dsName && spec?.source?.table_name) {
      return `${dsName}.${isSchemaExists ? [schema, table].join(".") : table}`;
    }
    return null;
  }

  const handleExportCsv = async () => {
    if (!nodeData?.node?.title) {
      notify({
        type: "warning",
        message: "No Model Selected",
        description: "Please select a model to export data",
      });
      return;
    }

    setIsExportingCsv(true);

    try {
      const requestOptions = {
        method: "GET",
        url: `/api/v1/visitran/${
          selectedOrgId || "default_org"
        }/project/${projectId}/no_code_model/${nodeData.node.title}/export_csv`,
      };

      const response = await axios(requestOptions);
      const data = response.data;

      if (!data.content || data.content.length === 0) {
        notify({
          type: "warning",
          message: "No Data Available",
          description: "The table contains no data to export",
        });
        return;
      }

      // Convert data to CSV format using papaparse
      const csvContent = Papa.unparse(data.content, {
        header: true,
        delimiter: ",",
      });

      // Add BOM for Excel compatibility
      const BOM = "\uFEFF";
      const csvWithBOM = BOM + csvContent;

      // Create blob and download
      const blob = new Blob([csvWithBOM], { type: "text/csv;charset=utf-8;" });
      const link = document.createElement("a");
      const url = URL.createObjectURL(blob);

      link.setAttribute("href", url);
      link.setAttribute("download", `${nodeData.node.title}_data.csv`);
      link.style.visibility = "hidden";

      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);

      notify({
        type: "success",
        message: "CSV Export Successful",
        description: `Data exported to '${nodeData.node.title}_data.csv'`,
      });
    } catch (error) {
      console.error("CSV export error:", error);
      notify({ error });
    } finally {
      setIsExportingCsv(false);
    }
  };

  const updateSpec = (newSpec) => {
    setSpec(newSpec);
  };

  async function reloadAndRollbackData(type, showLoader = true) {
    const urlType = {
      rollback: `/api/v1/visitran/${
        selectedOrgId || "default_org"
      }/project/${projectId}/no_code_model/${modelName}/rollback`,
      reload: `/api/v1/visitran/${
        selectedOrgId || "default_org"
      }/project/${projectId}/reload?file_name=${modelName}`,
    };
    const requestOptions = {
      method: "GET",
      url: urlType[type],
    };
    if (showLoader) setIsLoading(true);
    axios(requestOptions)
      .then((res) => {
        let specYaml = res?.data?.yaml;
        if (specYaml !== "") {
          // Add missing keys to transform if not present
          const newSpec = produce(yaml.load(specYaml), (draft) => {
            if (!draft.transform) {
              draft.transform = initialSpec.transform;
            }
            if (!draft.transform_order) {
              draft.transform_order = initialSpec.transform_order;
            }
            if (!draft.presentation.sort) {
              draft.presentation.sort = initialSpec.presentation.sort;
            }
            if (!draft.presentation.hidden_columns) {
              draft.presentation.hidden_columns =
                initialSpec.presentation.hidden_columns;
            }
            if (!draft.reference) {
              draft.reference = initialSpec.reference;
            }
            if (!draft.source) {
              draft.source = initialSpec.source;
            }
            if (!draft.model) {
              draft.model = initialSpec.model;
            }
          });
          updateSpec(newSpec);
          setReference(newSpec?.reference);
          setSource(newSpec?.source);
          setModel(newSpec?.model);

          if (type === "rollback") {
            runTransformation(newSpec);
          }
        } else {
          specYaml = initialSpec;
          updateSpec(specYaml || {});
          setReference(specYaml?.reference || []);
          setSource(specYaml?.source || {});
          setModel(specYaml?.model || {});
          handleModalOpen("sourceDestination");
        }
        seqBackup.current = res?.data?.sequence_orders;
        getLineageData();
      })
      .catch((error) => {
        console.error(error);
        notify({ error });
      })
      .finally(() => {
        if (showLoader) {
          setIsLoading(false);
        }
      });
  }

  // Update sequence, lineage, UI state and run the model.
  const processTransformSuccess = (res) => {
    seqBackup.current = res?.data?.sequence_orders;

    /* lineage graph */
    const linData = res.data.sequence_lineage.data;
    const lineageSeqdata = transformLineageData(
      linData,
      "sequence",
      handleElementClick
    );
    setSeqLineageData(linData);
    const { nodes: layoutedNodes, edges: layoutedEdges } = getLayoutedElements(
      lineageSeqdata.nodes,
      lineageSeqdata.edges
    );
    setSeqNodes(layoutedNodes);
    setSeqEdges(layoutedEdges);

    /* reveal sequence if toggled */
    if (reveal) setSeqOrder(res.data.sequence_orders);

    /* re-run model */
    runTransformation(res?.data?.model_data);

    return { status: "success", spec: res?.data?.model_data };
  };

  // Centralised error handler for both save / delete transforms.
  const processTransformError = (error) => {
    if (error.response?.status === 424) {
      const spec = error.response.data.message_args.spec;
      if (Object.keys(spec).length) getRevertSpec(spec);
    }
    console.error(error);
    notify({ error });
    setTransformationErrorFlag(true);
    setIsLoading(false);
    return { status: "failed" };
  };

  // Execute any transform request (POST / DELETE) with shared flow.
  const executeTransformRequest = async (requestOptions) => {
    setIsLoading(true);
    try {
      const res = await axios(requestOptions);
      return processTransformSuccess(res);
    } catch (error) {
      return processTransformError(error);
    }
  };

  const handleDeleteTransformation = async (body) => {
    body.model_name = nodeData.node.title;
    const requestOptions = {
      method: "DELETE",
      url: `/api/v1/visitran/${
        selectedOrgId || "default_org"
      }/project/${projectId}/no_code_model/${
        nodeData.node.title
      }/delete-transform`,
      headers: { "X-CSRFToken": csrfToken },
      data: body,
    };
    return executeTransformRequest(requestOptions);
  };

  const saveTransformation = async (step_config, step_id) => {
    const body = { model_name: nodeData.node.title, step_config };
    if (step_id) body.step_id = step_id;

    const requestOptions = {
      method: "POST",
      url: `/api/v1/visitran/${
        selectedOrgId || "default_org"
      }/project/${projectId}/no_code_model/${
        nodeData.node.title
      }/set-transform`,
      headers: { "X-CSRFToken": csrfToken },
      data: body,
    };

    return executeTransformRequest(requestOptions);
  };

  const handleSetPresentation = (body) => {
    body.model_name = nodeData.node.title;
    const requestOptions = {
      method: "POST",
      url: `/api/v1/visitran/${
        selectedOrgId || "default_org"
      }/project/${projectId}/no_code_model/${
        nodeData.node.title
      }/set-presentation`,
      headers: {
        "X-CSRFToken": csrfToken,
      },
      data: body,
    };

    return executeTransformRequest(requestOptions);
  };

  const formCol = (isFormulaColumn, isAlias, isWindowColumn) => {
    let columnTitle = "";
    if (isFormulaColumn) {
      columnTitle = "formula-column-title";
    } else if (isWindowColumn) {
      columnTitle = "window-column-title";
    } else if (isAlias) {
      columnTitle = "alias-column";
    }
    return columnTitle;
  };

  const formulaInfoShow = (isAlias, aliasfunctions, column) => {
    if (isAlias) {
      return (
        <span>
          <Tooltip title={aliasfunctions[column]} key={column}>
            <InfoCircleOutlined style={{ marginLeft: "10px" }} />
          </Tooltip>
        </span>
      );
    }
    return null;
  };
  const components = {
    header: {
      cell: ResizableTitle,
    },
  };

  const handleResize =
    (index) =>
    (e, { size }) => {
      setColumnWidths((prev) => ({
        ...prev,
        [index]: size.width,
      }));
    };
  function formatNumber(value) {
    if (typeof value !== "number" || isNaN(value)) {
      return "-";
    }
    return value % 1 === 0 ? value : value.toFixed(2);
  }

  const calculateColumnWidth = (columnName, dataType) => {
    // Base width calculation on column name length
    const nameLength = columnName.length;
    const characterWidth = nameLength * 8; // 8px per character
    let baseWidth = characterWidth + 60; // Add padding for icons

    // Add extra width for specific data types
    if (dataType === "Number" || dataType === "Float") {
      baseWidth = Math.max(baseWidth, 100);
    } else if (dataType === "DateTime" || dataType === "Date") {
      baseWidth = Math.max(baseWidth, 120);
    } else if (dataType === "Boolean") {
      baseWidth = Math.max(baseWidth, 80);
    }

    // Ensure minimum width
    return Math.max(baseWidth, 50);
  };

  const settingNewCols = (
    selectedColumns,
    allColumns,
    dataTypes,
    aggregateCols,
    combineColumnsCols,
    joinedMapped,
    aliasfunctions,
    spec
  ) => {
    const newColumns = selectedColumns.map((column) => {
      const index = allColumns.indexOf(column);
      const dataType = dataTypes[index];
      const formulaCols = {};
      const windowCols = {};

      const synthesizeId = getTransformId(
        spec?.transform,
        transformationTypes.SYNTHESIZE
      );
      const renameColumnId = getTransformId(
        spec?.transform,
        transformationTypes.RENAME_COLUMN
      );
      const windowId = getTransformId(
        spec?.transform,
        transformationTypes.WINDOW
      );

      // Get formula columns from synthesize transform
      const synthesizeSpec = getSynthesizeSpec(spec?.transform, synthesizeId);
      synthesizeSpec.columns.forEach((el) => {
        formulaCols[el.column_name] = el.operation.formula;
      });

      // Get window columns from window transform (separate transform type)
      const windowSpec = getWindowSpec(spec?.transform, windowId);
      windowSpec.columns.forEach((el) => {
        const op = el.operation;
        const aggCol = op.agg_column ? `(${op.agg_column})` : "()";
        const partitions = op.partition_by?.length
          ? ` PARTITION BY ${op.partition_by.join(", ")}`
          : "";
        const orders = op.order_by
          ?.map((o) => `${o.column} ${o.direction}`)
          .join(", ");
        windowCols[el.column_name] = {
          info: `${op.function || ""}${aggCol}${partitions} ORDER BY ${
            orders || ""
          }`,
          data: el,
        };
      });

      const isFormulaColumn = formulaCols[column];
      const isWindowColumn = windowCols[column];
      const isAlias = aggregateCols.includes(column);
      const rename_col = getRenameColumnSpec(
        spec?.transform,
        renameColumnId
      )?.mappings;

      // Find rename entry for the current column
      const renameEntry = rename_col.find((item) => item.new_name === column);
      const originalName = renameEntry
        ? renameEntry.original_name || renameEntry.old_name
        : null;
      const formulaColsIncCol = Object.keys(formulaCols).includes(column);

      const isCombineColumn = combineColumnsCols.includes(column);
      const calculatedWidth = calculateColumnWidth(column, dataType);

      return {
        key: column,
        width: calculatedWidth,
        title: (
          <div
            className={`column-title flex-space-between
             dragHandler
            ${formCol(isFormulaColumn, isAlias, isWindowColumn)}`}
          >
            <div className="flex-align-center" style={{ whiteSpace: "normal" }}>
              {joinedMapped[column] !== undefined && (
                <Typography.Text
                  style={{
                    color: joinTableColors[joinedMapped[column]],
                    marginRight: "2px",
                    marginLeft: "-7px",
                  }}
                >
                  |
                </Typography.Text>
              )}
              <span
                className={`${
                  formulaColsIncCol ||
                  isWindowColumn ||
                  isAlias ||
                  isCombineColumn
                    ? "pointer"
                    : "rename-hover-effect"
                }`}
                onClick={() => {
                  if (formulaColsIncCol) {
                    // Edit formula column
                    setOpenFormula(true);
                    setSelectedFormulaCol({
                      column,
                      formula: formulaCols[column],
                    });
                  } else if (isWindowColumn) {
                    // Edit window column
                    setOpenFormula(true);
                    setSelectedFormulaCol({
                      column,
                      formula: "",
                      type: "WINDOW",
                      windowData: windowCols[column].data,
                    });
                  } else if (!isAlias && !isCombineColumn) {
                    // Rename regular column
                    handleDoubleClick(column);
                  }
                }}
              >
                {column}
              </span>
              {formulaColsIncCol ? (
                <Typography.Text
                  onClick={() => {
                    setOpenFormula(true);
                    setSelectedFormulaCol({
                      column,
                      formula: formulaCols[column],
                    });
                  }}
                  className="ml-10 pointer"
                >
                  <Tooltip title="Edit" key="edit">
                    <EditOutlined />
                  </Tooltip>
                  <Tooltip
                    title={`Formula : ${formulaCols[column]}`}
                    key={column}
                  >
                    <InfoCircleOutlined style={{ marginLeft: "10px" }} />
                  </Tooltip>
                </Typography.Text>
              ) : isWindowColumn ? (
                <Typography.Text
                  onClick={() => {
                    setOpenFormula(true);
                    setSelectedFormulaCol({
                      column,
                      formula: "",
                      type: "WINDOW",
                      windowData: windowCols[column].data,
                    });
                  }}
                  className="ml-10 pointer"
                >
                  <Tooltip title="Edit" key="edit">
                    <EditOutlined />
                  </Tooltip>
                  <Tooltip
                    title={`Window : ${windowCols[column].info}`}
                    key={column}
                  >
                    <InfoCircleOutlined style={{ marginLeft: "10px" }} />
                  </Tooltip>
                </Typography.Text>
              ) : (
                formulaInfoShow(isAlias, aliasfunctions, column)
              )}
            </div>

            <div className="data-type">
              {/* Show tooltip with original name if column is renamed, otherwise hide the icon */}
              {renameEntry && (
                <Tooltip title={originalName} key={`rename-${column}`}>
                  <RetweetOutlined />
                </Tooltip>
              )}
              <Tooltip title={dataType} key={`type-${column}`}>
                {dataTypeIcon[dataType]}
              </Tooltip>
            </div>
          </div>
        ),
        dataIndex: column,
        ellipsis: true,
        render: (text) => {
          // Handle case where text is an object (e.g., {uuid, source_timestamp})
          const displayValue =
            typeof text === "object" && text !== null
              ? JSON.stringify(text)
              : text;

          const formattedValue =
            displayValue === null || displayValue === undefined
              ? ""
              : ["Boolean", "boolean"].includes(dataType)
              ? String(displayValue)
              : dataType === "Number"
              ? formatNumber(displayValue)
              : displayValue;

          const alignClass =
            dataType === "Number"
              ? "flex-justify-right"
              : ["Boolean", "boolean"].includes(dataType)
              ? "flex-justify-center"
              : "";

          return (
            <CopyableCell
              value={formattedValue}
              className={`column-title ${formCol(
                isFormulaColumn,
                isAlias,
                isWindowColumn
              )} ${alignClass}`}
            >
              {formattedValue}
            </CopyableCell>
          );
        },
      };
    });

    return newColumns;
  };

  const getSampleData = (page = 1, limit = 50, yaml = spec) => {
    if (
      nodeData.node.title !==
      projectDetails[projectId]?.["focussedTab"]?.key?.split("/").pop()
    )
      return;

    setIsLoading(true);
    const requestOptions = {
      method: "GET",
      url: `/api/v1/visitran/${
        selectedOrgId || "default_org"
      }/project/${projectId}/no_code_model/${modelName}/content`,
      params: {
        page,
        limit,
      },
    };
    axios(requestOptions)
      .then((res) => {
        const obj = res.data?.column_description;
        seqBackup.current = res?.data?.sequence_orders;
        const allColumns = Object.keys(obj);
        const aliasfunctions = {};
        const joinedMapped = res.data.column_names.joined_tables;
        const aggregateId = getTransformId(
          yaml?.transform,
          transformationTypes.GROUPS_AND_AGGREGATION
        );
        const aggregateSpec = getGroupAndAggregationSpec(
          yaml?.transform,
          aggregateId
        );
        const aggregateCols = aggregateSpec.aggregate_columns.map((el) => {
          // Formula aggregates have 'expression' field, regular aggregates have 'function' and 'column'
          const str = el.expression
            ? `=${el.expression}`
            : `${el.function}(${el.column})`;
          aliasfunctions[el.alias] = str;
          return el.alias;
        });

        const combineColumnsId = getTransformId(
          yaml?.transform,
          transformationTypes.COMBINE_COLUMNS
        );
        const combineColumnsSpec = getCombineColumnsSpec(
          yaml?.transform,
          combineColumnsId
        );
        const combineColumnsCols = combineColumnsSpec?.columns?.map(
          (el) => el?.columnName
        );

        const dataTypes = allColumns.map((column) => obj[column].data_type);
        const dbDataTypes = allColumns.map(
          (column) => obj[column].column_dbtype
        );
        const orginalCol = res?.data?.all_column_description;
        const orginalColList = Object.keys(orginalCol);
        setorginalColName(orginalColList);
        setColumnDetails({
          columns: res?.data?.column_names || {},
          dataTypes: dataTypes,
          allColumnsDetails: res?.data?.all_column_description,
          dbDataTypes: dbDataTypes,
          schema_name: res?.data?.schema_name,
        });
        const selectedColumns = allColumns;
        const newFormulaColumns = res?.data?.formula_columns || [];
        setFormulaColumns(newFormulaColumns);
        const newColumns = settingNewCols(
          selectedColumns,
          allColumns,
          dataTypes,
          aggregateCols,
          combineColumnsCols,
          joinedMapped,
          aliasfunctions,
          yaml
        );
        const records = Array.isArray(res?.data?.content)
          ? res?.data?.content
          : [];
        const newDataSource = records.map((row, index) => {
          // Destructure to handle 'children' property which can conflict with Ant Design Table
          // Ant Design expects children to be an array for tree data, but backend may return
          // a column named 'children' with non-array values, causing flattenData to crash
          const { children, ...rest } = row || {};
          const sanitizedRow = { key: index, ...rest };
          // Only include children if it's actually an array (for tree-structured data)
          if (Array.isArray(children)) {
            sanitizedRow.children = children;
          } else if (children !== undefined && children !== null) {
            // Rename non-array 'children' to '_children' to preserve the data
            sanitizedRow._children = children;
          }
          return sanitizedRow;
        });
        const totalRecords = res?.data?.total || 0;
        setColumns(Array.isArray(newColumns) ? newColumns : []);
        setCurrentData(newDataSource);
        setTotalCount(totalRecords); // Save the total count for pagination
        setCurrentPage(page);
        if (configApply) {
          getLineageData();
        }
      })
      .catch((error) => {
        console.error(error);
        notify({ error });
      })
      .finally(() => {
        setIsLoading(false);
      });
  };

  const handleGetColumns = (transformationId, transformationType) => {
    const params = {
      transformation_type: transformationType,
    };
    if (transformationId) {
      params["transformation_id"] = transformationId;
    }
    const requestOptions = {
      method: "GET",
      url: `/api/v1/visitran/${
        selectedOrgId || "default_org"
      }/project/${projectId}/no_code_model/${nodeData.node.title}/columns`,
      params,
    };

    axiosPrivate(requestOptions)
      .then((res) => {
        const obj = res.data?.column_description;
        const allColumns = Object.keys(obj);
        const dataTypes = allColumns.map((column) => obj[column].data_type);
        const dbDataTypes = allColumns.map(
          (column) => obj[column].column_dbtype
        );
        setColumnDetails((prev) => {
          return {
            columns: res?.data?.column_names,
            dataTypes: dataTypes,
            allColumnsDetails: prev?.allColumnsDetails,
            dbDataTypes: dbDataTypes,
            schema_name: prev?.schema_name,
          };
        });
      })
      .catch((error) => {
        console.error(error);
        notify({ error });
      });
  };

  const getRevertSpec = (spec) => {
    const newSpec = produce(spec, (draft) => {
      if (!draft.transform) {
        draft.transform = initialSpec.transform;
      }
      if (!draft.transform_order) {
        draft.transform_order = initialSpec.transform_order;
      }
      if (!draft.presentation.sort) {
        draft.presentation.sort = initialSpec.presentation.sort;
      }
      if (!draft.presentation.hidden_columns) {
        draft.presentation.hidden_columns =
          initialSpec.presentation.hidden_columns;
      }
      if (!draft.reference) {
        draft.reference = initialSpec.reference;
      }
      if (!draft.source) {
        draft.source = initialSpec.source;
      }
      if (!draft.model) {
        draft.model = initialSpec.model;
      }
    });
    setIsLoading(false);
    updateSpec(newSpec);
    setSpecRevert(true);
  };

  const STATUS_COLOR = {
    SUCCESS: "green",
    FAILURE: "red",
    STARTED: "processing",
    RUNNING: "processing",
    RETRY: "orange",
  };

  const fetchRecentRuns = async () => {
    const name = nodeData?.node?.title;
    if (!name) return;
    if (recentRunsState.fetchedFor === name && !recentRunsState.loading) return;
    setRecentRunsState((prev) => ({ ...prev, loading: true }));
    try {
      const runs = await listRecentRunsForModel(projectId, name, 5);
      setRecentRunsState({ loading: false, runs, fetchedFor: name });
    } catch (error) {
      setRecentRunsState((prev) => ({ ...prev, loading: false }));
      notify({ error });
    }
  };

  const renderRecentRunsPanel = () => (
    <div
      style={{
        width: 320,
        background: token.colorBgElevated,
        borderRadius: token.borderRadiusLG,
        boxShadow: token.boxShadowSecondary,
        padding: "8px 0",
      }}
    >
      <div
        style={{
          padding: "4px 12px 8px",
          fontSize: 12,
          fontWeight: 600,
          color: token.colorTextSecondary,
        }}
      >
        Recent runs
      </div>
      {recentRunsState.loading ? (
        <div style={{ padding: "12px 16px", color: token.colorTextSecondary }}>
          Loading…
        </div>
      ) : recentRunsState.runs.length === 0 ? (
        <div style={{ padding: "12px 16px", color: token.colorTextSecondary }}>
          No runs for this model yet.
        </div>
      ) : (
        recentRunsState.runs.map((run) => {
          const statusLabel = run.status === "FAILURE" ? "FAILED" : run.status;
          return (
            <div
              key={run.run_id}
              style={{
                padding: "8px 12px",
                borderTop: `1px solid ${token.colorBorderSecondary}`,
                display: "flex",
                flexDirection: "column",
                gap: 2,
              }}
            >
              <Space size={8}>
                <Tag color={STATUS_COLOR[run.status] || "default"}>
                  {statusLabel}
                </Tag>
                <Tag color={run.trigger === "manual" ? "blue" : "default"}>
                  {run.trigger === "manual" ? "Manual" : "Scheduled"}
                </Tag>
                <Tag color={run.scope === "model" ? "purple" : "default"}>
                  {run.scope === "model" ? "Single model" : "Full job"}
                </Tag>
              </Space>
              <Typography.Text style={{ fontSize: 12 }}>
                {run.task_name}
                {run.environment_name ? ` · ${run.environment_name}` : ""}
              </Typography.Text>
              <Typography.Text type="secondary" style={{ fontSize: 11 }}>
                {getRelativeTime(run.start_time)}
              </Typography.Text>
            </div>
          );
        })
      )}
      <Divider style={{ margin: "8px 0" }} />
      <div style={{ padding: "0 8px 4px" }}>
        <Button
          type="link"
          block
          onClick={() => navigate("/project/job/history")}
        >
          View full Run History →
        </Button>
      </div>
    </div>
  );

  const openQuickDeploy = async () => {
    const currentModelName = nodeData?.node?.title;
    if (!currentModelName) return;
    setQuickDeployModal({
      open: true,
      step: "loading",
      candidates: [],
      selectedTaskId: null,
      selectedScope: null,
      confirmed: false,
      submitting: false,
    });
    try {
      const candidates = await listDeployCandidates(
        projectId,
        currentModelName
      );
      if (!candidates.length) {
        setQuickDeployModal((prev) => ({
          ...prev,
          step: "empty",
          candidates: [],
        }));
      } else if (candidates.length === 1) {
        setQuickDeployModal((prev) => ({
          ...prev,
          step: "confirm",
          candidates,
          selectedTaskId: candidates[0].user_task_id,
        }));
      } else {
        setQuickDeployModal((prev) => ({
          ...prev,
          step: "pick",
          candidates,
          selectedTaskId: candidates[0].user_task_id,
        }));
      }
    } catch (error) {
      setQuickDeployModal((prev) => ({ ...prev, open: false }));
      notify({ error });
    }
  };

  const closeQuickDeploy = () => {
    setQuickDeployModal((prev) => ({ ...prev, open: false }));
  };

  const confirmQuickDeploy = async () => {
    const currentModelName = nodeData?.node?.title;
    const { selectedTaskId, selectedScope, confirmed } = quickDeployModal;
    if (!currentModelName || !selectedTaskId || !selectedScope || !confirmed) {
      return;
    }
    setQuickDeployModal((prev) => ({ ...prev, submitting: true }));
    try {
      if (selectedScope === "job") {
        await runTask(projectId, selectedTaskId);
      } else {
        await runTaskForModel(projectId, selectedTaskId, currentModelName);
      }
      const selected = quickDeployModal.candidates.find(
        (c) => c.user_task_id === selectedTaskId
      );
      const envName = selected?.environment_name || "the selected environment";
      const jobName = selected?.task_name || "";
      notify({
        type: "success",
        message: "Deploy Triggered",
        description:
          selectedScope === "job"
            ? `Job "${jobName}" is running on "${envName}" (all enabled models). Check Run History for progress.`
            : `"${currentModelName}" is running on "${envName}" via job "${jobName}". Check Run History for progress.`,
      });
      setRefreshModels(true);
      setRecentRunsState((prev) => ({ ...prev, fetchedFor: null }));
      setQuickDeployModal((prev) => ({
        ...prev,
        open: false,
        submitting: false,
      }));
    } catch (error) {
      setQuickDeployModal((prev) => ({ ...prev, submitting: false }));
      notify({ error });
    }
  };

  const goToScheduler = () => {
    setQuickDeployModal((prev) => ({ ...prev, open: false }));
    navigate("/project/job/list");
  };

  const runTransformation = (spec) => {
    setIsLoading(true);
    const specYaml = yaml.dump(removeUnwantedKeys(spec));
    const requestOptions = {
      method: "POST",
      url: `/api/v1/visitran/${
        selectedOrgId || "default_org"
      }/project/${projectId}/execute/run`,
      data: {
        name: projectName,
        file_name: nodeData.node.title,
        file: specYaml,
      },
      headers: {
        "X-CSRFToken": csrfToken,
      },
    };
    axios(requestOptions)
      .then(() => {
        getSampleData(undefined, undefined, spec);
        setRefreshModels(true);
      })
      .catch((error) => {
        const notifKey = notify({
          type: "error",
          renderMarkdown: false,
          message: "Transformation Execution Failed",
          description: (
            <Space direction="vertical">
              <ReactMarkdown remarkPlugins={[remarkGfm]}>
                {error?.response?.data?.error_message ||
                  "One or more transforms failed to run. Check your configurationand retry."}
              </ReactMarkdown>
              {error?.response?.data?.is_rollback && (
                <div>
                  <Space className="width-100 flex-justify-right">
                    <Button
                      type="primary"
                      size="medium"
                      onClick={() => {
                        reloadAndRollbackData("rollback");
                        closeAll(notifKey);
                      }}
                      icon={<RollbackOutlined />}
                    >
                      Rollback
                    </Button>
                  </Space>
                </div>
              )}
            </Space>
          ),
        });
        setTransformationErrorFlag(true);
        setIsLoading(false);
        setRefreshModels(true);
      });
  };

  const getCurrentModified = () => {
    if (openedModal === "sourceDestination") {
      return (
        source.table_name &&
        model.table_name &&
        !(
          isEqual(reference, spec?.reference) &&
          isEqual(source, spec?.source) &&
          isEqual(model, spec?.model)
        ) &&
        !(
          isEqual(source.table_name, model.table_name) &&
          isEqual(source.schema_name, model.schema_name)
        )
      );
    }
    if (openedModal === "joins") {
      return !isEqual(
        removeIdFromObjects(joins),
        getJoinSpec(spec?.transform, transformIds?.JOIN)?.tables || []
      );
    }
    return true;
  };

  const debouncedHandleResize = debounce(() => {
    setClientHeight(containerRef.current?.clientHeight - 200);
  }, 100);

  const runApi = (spec, isTabChanged) => {
    const hasSourceTable = spec?.source?.table_name !== null;
    const isCurrentTabActive =
      nodeData.node.title ===
      projectDetails[projectId]?.["focussedTab"]?.key?.split("/").pop();

    if (hasSourceTable && !configApply && isTabChanged) {
      getSampleData();

      // Only update transformIds for the currently active tab
      if (isCurrentTabActive) {
        setTransformIds(spec);
      }
    } else if ((!isTabChanged && hasSourceTable) || configApply) {
      setConfigApply(false);
    }
  };

  useEffect(() => {
    if (transformationErrorFlag) {
      setTransformationErrorFlag(false);
      setIsLoading(false);
    }
  }, [transformationErrorFlag]);

  useEffect(() => {
    if (refreshModels) {
      const handleRefreshModels = async () => {
        setIsLoading(true);
        await reloadAndRollbackData("reload", false);
        getSampleData();
        setRefreshModels(false);
      };

      handleRefreshModels();
    }
  }, [refreshModels]);

  useEffect(() => {
    const currentTabKey = projectDetails[projectId]?.["focussedTab"]?.key;
    if (spec?.source?.table_name !== null) {
      const isTabChanged = prevTabKey.current !== currentTabKey;
      if (specRevert) {
        setSpecRevert(false);
      }
      runApi(spec, isTabChanged);
      prevTabKey.current = currentTabKey;
    } else {
      setColumns([]);
      setCurrentData([]);
      setCurrentPage(1);
    }
  }, [spec, projectDetails[projectId]?.["focussedTab"]]);

  useEffect(() => {
    if (isEmpty(dbConfigDetails)) return;
    reloadAndRollbackData("reload");
    window.addEventListener("resize", (evt) => {
      if (evt.detail) {
        bottomSectionRef.current = evt.detail;
      }
      debouncedHandleResize();
    });
    return () => {
      window.removeEventListener("resize", debouncedHandleResize);
    };
  }, [dbConfigDetails.datasource_name]);

  useEffect(() => {
    debouncedHandleResize();
  }, []);

  useEffect(() => {
    const startIndex = (currentPage - 1) * pageSize;
    const endIndex = startIndex + pageSize;
    setCurrentData(currentData.slice(startIndex, endIndex));
  }, [currentPage, pageSize]);

  useEffect(() => {
    if (reveal) {
      setSeqOrder(seqBackup.current);
    } else {
      setSeqOrder({});
    }
  }, [reveal]);

  useEffect(() => {
    const body = {
      transports: ["websocket"],
      path: "/api/v1/socket",
      withCredentials: true,
    };
    const newSocket = io(getBaseUrl(), body);

    let socketSessionId = null;
    newSocket.on("connect", () => {
      // Listen for the session ID sent by the server
      newSocket.on("session_id", (data) => {
        socketSessionId = data.session_id;
        // Listen for messages in the specific room (session ID)
        newSocket.on(`logs:${socketSessionId}`, (data) => {
          const message = data?.data?.message;
          const level = (data?.data?.level || "info").toLowerCase();
          if (!message) return;
          const doc = document.getElementsByClassName("logsSection");
          if (doc[0]) {
            setTimeout(() => {
              doc[0].scrollTop = doc[0].scrollHeight;
            }, 800);
          }
          setLogsInfo((old) => [...old, { level, message }]);
        });
      });
    });
    return () => {
      // unsubscribe to the channel to stop listening the socket messages for the logId
      if (socketSessionId) {
        newSocket.off(`logs:${socketSessionId}`);
      }
      newSocket.disconnect();
    };
  }, [sessionId]);

  const modalData = {
    sourceDestination: {
      width: 606,
      title: `${nodeData.node.title} Model Configuration`,
      body: (
        <ConfigureSourceDestination
          modelName={nodeData.node.title}
          spec={spec}
          updateSpec={updateSpec}
          reference={reference}
          setReference={setReference}
          source={source}
          setSource={setSource}
          model={model}
          setModel={setModel}
        />
      ),
    },
    joins: {
      width: 800,
      title: "Joins",
      body: (
        <ConfigureJoins
          modelName={nodeData?.node?.title}
          joins={joins}
          setJoins={setJoins}
          sourceTable={spec?.source?.table_name}
          columnDetails={columnDetails}
          spec={spec}
          onValidationChange={setJoinValidationError}
        />
      ),
    },
    sourceDestinationChangeAlert: {
      title: "Alert",
      body: "Changing the source table will clear the data grid and all transforms.",
    },
    clearTransformsAlert: {
      title: "Alert",
      body: "This will clear all transforms.",
    },
  };

  // const dragProps = {
  //   onDragEnd(fromIndex, toIndex) {
  //     setColumns((prevColumns) => {
  //       const updatedColumns = [...prevColumns];
  //       const item = updatedColumns.splice(fromIndex, 1)[0];
  //       updatedColumns.splice(toIndex, 0, item);
  //       return updatedColumns;
  //     });
  //   },
  //   nodeSelector: "th",
  //   handleSelector: ".dragHandler",
  // };

  const handlePagination = (newPage, newPageSize) => {
    if (currentPage !== newPage || pageSize !== newPageSize) {
      setCurrentPage(newPage);
      setPageSize(newPageSize);
      getSampleData(newPage, newPageSize);
    }
  };

  function transformLineageData(data, type, handleElementClick) {
    const position = { x: 0, y: 0 };
    data["edges"] = data.edges.map((edge) => {
      return {
        ...edge,
        markerEnd: {
          type: MarkerType.ArrowClosed,
        },
      };
    });
    data["nodes"] = data["nodes"].map((node, index) => {
      const el = node.data.label;
      // Store the original text label before replacing it with JSX
      node.data.originalLabel = el;

      // Store the node type icon for use in the info box
      node.data.typeIcon =
        node.type === "input" ? (
          <DatabaseOutlined style={{ color: "var(--black)" }} />
        ) : (
          <Tech style={{ color: "var(--black)" }} />
        );

      // Store the icon style class
      node.data.iconStyleClass =
        node.type === "input"
          ? "icon_style node_color_blue"
          : !node.type
          ? "node_color_yellow icon_style"
          : "icon_style node_color_pink";

      if (type === "lineage") {
        node["data"]["label"] = (
          <>
            {node.type === "input" ? (
              <div className=" icon_style node_color_blue">
                <DatabaseOutlined style={{ color: "var(--black)" }} />
              </div>
            ) : (
              <div
                className={
                  !node.type
                    ? "node_color_yellow icon_style"
                    : "icon_style node_color_pink"
                }
              >
                <Tech style={{ color: "var(--black)" }} />
              </div>
            )}
            <Text style={{ padding: "4px 12px" }}>{node.data.label}</Text>
            <Space
              className="lineage-info-icon"
              onClick={(e) => {
                e.stopPropagation();
                if (handleInfoClick) {
                  handleInfoClick(node);
                }
              }}
            >
              <InfoCircleOutlined
                style={{ color: "white", fontSize: "12px" }}
              />
            </Space>
          </>
        );
      } else {
        node["data"]["label"] = (
          <>
            {node.type === "input" && (
              <Typography.Text
                className="seqlineagewrap"
                onClick={() => handleElementClick("--source-table--")}
              >
                <DatabaseFilled
                  className="fs_25"
                  style={{ color: "var(--black)" }}
                />
                <Typography.Text
                  style={{ color: "var(--black)" }}
                  className="overflow-ellipsis"
                >
                  {node.data.label}
                </Typography.Text>
              </Typography.Text>
            )}
            {node.type === "output" && (
              <Typography.Text
                className="seqlineagewrap"
                onClick={() => handleElementClick("--destination-table--")}
              >
                <DatabaseFilled
                  style={{ color: "var(--black)" }}
                  className="fs_25"
                />
                <Typography.Text
                  style={{ color: "var(--black)" }}
                  className="overflow-ellipsis"
                >
                  {node.data.label}
                </Typography.Text>
              </Typography.Text>
            )}

            {!node.type && (
              <Typography.Text
                className="seqlineagewrap"
                onClick={() => handleElementClick(el)}
              >
                {iconMap[node.data.label]?.icon}
                <div style={{ color: "var(--black)" }}>
                  {iconMap[node.data.label]?.name}
                </div>
              </Typography.Text>
            )}
          </>
        );
      }
      return {
        ...node,
        position,
        style: {
          backgroundColor: getNodeBg(node.type, type, index + 1),
          width: "auto",
          border: "1px solid var(--black)",
          padding: 0,
          display: "grid",
          gridAutoFlow: "column",
          alignItems: "center",
          lineHeight: "1.8",
          borderRadius: "10px",
          cursor: "default",
        },
      };
    });
    return data;
  }

  const handleInfoClick = (node) => {
    // Get node background color
    let borderColor;
    if (node.type === "input") {
      borderColor = "#B0E3F9";
    } else if (node.type === "output") {
      borderColor = "#FFC8D2";
    } else {
      borderColor = "#FFDD8A"; // Default for transformation nodes
    }

    // Check if info box for this node is already open
    const isInfoBoxOpen = infoStack.some((box) => box.id === node.id);

    if (isInfoBoxOpen) {
      // If info box is already open, close it and reset node border
      setInfoStack((prev) => prev.filter((b) => b.id !== node.id));

      // Reset node border to default
      setNodes((nds) =>
        nds.map((n) => {
          // Reset all node borders to default
          return {
            ...n,
            style: {
              ...n.style,
              border: "1px solid var(--black)",
            },
          };
        })
      );
    } else {
      // Get node info to display
      // Extract the plain text label without the info icon
      const plainTextLabel =
        typeof node.data.originalLabel === "string"
          ? node.data.originalLabel
          : node.data.label || "Node";

      // Set loading state while fetching node info

      // Create a temporary info box with loading state
      const tempNodeInfo = {
        id: node.id,
        title: plainTextLabel,
        borderColor: borderColor,
        content: {
          sourceTable: "Loading...",
          joinTables: "Loading...",
          sqlQuery: "Loading...",
        },
      };

      // Show loading state immediately
      setInfoStack([tempNodeInfo]);

      // Fetch node info from API
      const requestOptions = {
        method: "GET",
        url: `/api/v1/visitran/${
          selectedOrgId || "default_org"
        }/project/${projectId}/lineage/${encodeURIComponent(
          plainTextLabel
        )}/info`,
      };

      axios(requestOptions)
        .then(({ data: { data } }) => {
          // Create node info with data from API
          const nodeInfo = {
            id: node.id,
            title: plainTextLabel,
            borderColor: borderColor,
            content: {
              sourceTable: data.source_table_name || "N/A",
              joinTables: Array.isArray(data.joined_table)
                ? data.joined_table.join(", ")
                : data.joined_table || "None",
              sqlQuery: data.sql?.sql || data.sql || "No SQL available",
            },
          };

          // Update info stack with actual data
          setInfoStack([nodeInfo]);
        })
        .catch((error) => {
          console.error("Error fetching node info:", error);
          console.error(error);
          notify({ error });

          // Show error state in info box
          const errorNodeInfo = {
            id: node.id,
            title: plainTextLabel,
            borderColor: borderColor,
            content: {
              sourceTable: "Error loading data",
              joinTables: "Error loading data",
              sqlQuery: "Error loading data",
            },
          };

          setInfoStack([errorNodeInfo]);
        });

      // Close any previously open info boxes and reset their node borders
      // First reset all node borders to default
      setNodes((nds) =>
        nds.map((n) => {
          // Reset all node borders to default
          return {
            ...n,
            style: {
              ...n.style,
              border: "1px solid var(--black)",
            },
          };
        })
      );

      // Node info is now set by the API response handler

      // Change node border to match background color
      setNodes((nds) =>
        nds.map((n) => {
          if (n.id === node.id) {
            return {
              ...n,
              style: {
                ...n.style,
                border: `2px solid ${borderColor}`,
                boxShadow: "0 0 5px rgba(0, 0, 0, 0.2)",
                transition: "all 0.3s ease",
              },
              className: `${n.className || ""} node-selected`,
            };
          }
          return n;
        })
      );
    }
  };

  const bringToFront = (id) => {
    setInfoStack((prev) => {
      const selected = prev.find((b) => b.id === id);
      const rest = prev.filter((b) => b.id !== id);
      return [selected, ...rest];
    });

    // Ensure this node's border remains highlighted
    setNodes((nds) =>
      nds.map((n) => {
        if (n.id === id) {
          // Find the node's border color from infoStack
          const nodeInfo = infoStack.find((box) => box.id === id);
          if (nodeInfo && nodeInfo.borderColor) {
            return {
              ...n,
              style: {
                ...n.style,
                border: `2px solid ${nodeInfo.borderColor}`,
              },
            };
          }
        }
        return n;
      })
    );
  };

  const getLineageData = (callSample = false) => {
    if (!projectId) return;
    setLineageData();
    const requestOptions = {
      method: "GET",
      url: `/api/v1/visitran/${
        selectedOrgId || "default_org"
      }/project/${projectId}/lineage`,
    };
    axios(requestOptions)
      .then(({ data: { data } }) => {
        data = transformLineageData(data, "lineage");
        setLineageData(data);
        const { nodes: layoutedNodes, edges: layoutedEdges } =
          getLayoutedElements(data.nodes, data.edges, lineageLayoutDirection);
        setNodes(layoutedNodes);
        setEdges(layoutedEdges);
      })
      .catch((error) => {
        console.error(error);
        notify({ error });
        setLineageData({});
      });
  };

  const isRevealSeqDisable = () => {
    for (const i in seqBackup.current) {
      if (seqBackup.current[i]) {
        return false;
      }
    }
    return true;
  };
  const handleDoubleClick = (columnKey) => {
    setEditingColumn(columnKey);
    setNewColumnName(columnKey || "");
  };

  const handleRename = async (columnKey) => {
    const originalColumnName = orginalColName.includes(columnKey)
      ? columnKey
      : null;

    if (!originalColumnName) {
      console.error("Original column name not found.");
      return;
    }

    const isDuplicateName = orginalColName.includes(newColumnName);

    if (isDuplicateName) {
      console.error("The new column name already exists in the schema.");
      return;
    }

    const draft = produce(spec, (draft) => draft);
    let renameColumnSpec = getRenameColumnSpec(
      draft?.transform,
      transformIds?.RENAME_COLUMN
    )?.mappings;

    // Check for an existing entry where `new_name` matches `old_name`
    const existingEntry = renameColumnSpec.find(
      (item) => item.new_name === columnKey
    );

    const newRenameEntry = {
      old_name: columnKey,
      new_name: newColumnName,
      original_name: existingEntry
        ? existingEntry.original_name
        : originalColumnName,
    };

    renameColumnSpec = renameColumnSpec.filter(
      (item) => item.old_name !== columnKey
    );
    renameColumnSpec.push(newRenameEntry);

    let result = {};
    if (renameColumnSpec?.length === 0) {
      const body = {
        step_id: transformIds?.RENAME_COLUMN,
      };
      result = await handleDeleteTransformation(body);
    } else {
      const body = {
        type: "rename_column",
        rename_column: { mappings: renameColumnSpec },
      };

      result = await saveTransformation(body, transformIds?.RENAME_COLUMN);
    }

    if (result?.status === "success") {
      setColumns((prevColumns) =>
        prevColumns.map((col) =>
          col.key === columnKey ? { ...col, title: newColumnName } : col
        )
      );
      setEditingColumn(null);
      setNewColumnName("");
      updateSpec(result?.spec);
    }
  };

  const handleCancel = () => {
    setEditingColumn(null);
    setNewColumnName("");
  };

  const hasRenameHistory = (columnKey) => {
    const renameList = getRenameColumnSpec(
      spec?.transform,
      transformIds?.RENAME_COLUMN
    )?.mappings;

    return renameList.some(
      (entry) => entry.new_name === columnKey || entry.old_name === columnKey
    );
  };

  const handleResetRename = async (columnKey) => {
    const draft = produce(spec, (draft) => draft);
    const renameList = getRenameColumnSpec(
      draft?.transform,
      transformIds?.RENAME_COLUMN
    )?.mappings;
    // // Remove all rename entries related to this original name
    const currentEntry = renameList.find(
      (entry) => entry.new_name === columnKey || entry.old_name === columnKey
    );
    const originalName = currentEntry.original_name;
    // actual chnage

    const body = {
      type: "rename_column",
      rename_column: {
        mappings: renameList.filter(
          (entry) => entry.original_name !== originalName
        ),
      },
    };

    let result = {};
    if (body?.rename_column?.mappings?.length === 0) {
      const payload = {
        step_id: transformIds?.RENAME_COLUMN,
      };
      result = await handleDeleteTransformation(payload);
    } else {
      result = await saveTransformation(body, transformIds?.RENAME_COLUMN);
    }

    if (result?.status === "success") {
      const renameList = getRenameColumnSpec(
        spec?.transform,
        transformIds?.RENAME_COLUMN
      )?.mappings;
      // Remove all rename entries related to this original name
      const currentEntry = renameList.find(
        (entry) => entry.new_name === columnKey || entry.old_name === columnKey
      );
      const originalName = currentEntry.original_name;
      if (!currentEntry) {
        console.error("No rename history found for column:", columnKey);
        return;
      }
      setColumns((prevColumns) =>
        prevColumns.map((col) =>
          col.key === columnKey || col.title === columnKey
            ? { ...col, title: originalName, key: originalName }
            : col
        )
      );
      updateSpec(result?.spec);
      setEditingColumn(null);
      setNewColumnName("");
    }
  };

  const updatedColumns = (Array.isArray(columns) ? columns : []).map(
    (col, index) => ({
      ...col,
      width: columnWidths[index] || col.width || 200,
      onHeaderCell: (column) => ({
        width: column.width,
        onResize: handleResize(index),
      }),
      title:
        editingColumn === col.key ? (
          <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
            <div>
              <Input
                value={newColumnName}
                onChange={(e) => {
                  const value = e.target.value.replace(/\s/g, "");
                  setNewColumnName(value);

                  const duplicate = orginalColName.includes(value);
                  setIsDuplicate(duplicate);
                }}
                onPressEnter={() =>
                  !isDuplicate && newColumnName && handleRename(col.key)
                }
                onKeyDown={(e) => e.key === "Escape" && handleCancel()}
                style={{
                  width: "revert-layer",
                  borderColor: isDuplicate ? "#dc4446" : undefined,
                }}
                placeholder="Enter new column name"
              />
              {isDuplicate && (
                <div
                  style={{
                    color: "#dc4446",
                    fontSize: "10px",
                    marginTop: "3px",
                  }}
                >
                  *Column name already exists.
                </div>
              )}
            </div>
            <div>
              <Button
                type="primary"
                icon={<CheckOutlined />}
                style={{
                  backgroundColor:
                    !isDuplicate && newColumnName ? "#1668dc" : "gray",
                  color: "white",
                  borderColor: !isDuplicate && newColumnName ? "green" : "gray",
                  cursor:
                    !isDuplicate && newColumnName ? "pointer" : "not-allowed",
                  width: 20,
                  height: 20,
                  padding: 0,
                }}
                onClick={() =>
                  !isDuplicate && newColumnName && handleRename(col.key)
                }
                disabled={isDuplicate || !newColumnName}
              />
              <Button
                type="danger"
                icon={<CloseOutlined />}
                style={{
                  marginLeft: 5,
                  cursor: "pointer",
                  width: 20,
                  height: 20,
                  padding: 0,
                }}
                onClick={handleCancel}
              />
              <Tooltip title={hasRenameHistory(col.key) ? "Reset" : "Disable"}>
                <Button
                  icon={<UndoOutlined />}
                  disabled={!hasRenameHistory(col.key)}
                  style={{
                    marginLeft: 5,
                    cursor: "pointer",
                    width: 20,
                    height: 20,
                    padding: 0,
                  }}
                  onClick={() => handleResetRename(col.key)}
                />
              </Tooltip>
            </div>
          </div>
        ) : (
          <div>{col.title}</div>
        ),
    })
  );

  const okText = [
    "sourceDestinationChangeAlert",
    "clearTransformsAlert",
  ].includes(openedModal)
    ? "I Understand"
    : "Apply";
  return (
    <div className="nocodeWrapper">
      <div ref={containerRef} className="nocode">
        <Space direction="vertical" size={5} className="no-code-topbar-layout">
          <div className="no-code-topbar-row">
            <NoCodeTopbar
              sourceTable={getTableName(
                spec?.source?.schema_name,
                spec?.source?.table_name
              )}
              destinationTable={getTableName(
                spec?.model?.schema_name,
                spec?.model?.table_name
              )}
              joinedTables={produce(
                getJoinSpec(spec?.transform, transformIds?.JOIN)?.tables || [],
                (draft) =>
                  draft?.map(
                    ({
                      joined_table: {
                        schema_name: schemaName,
                        table_name: tableName,
                      },
                    }) =>
                      `${isSchemaExists ? schemaName + "." : ""}${tableName}`
                  )
              )}
              spec={spec}
              updateSpec={updateSpec}
              handleModalOpen={handleModalOpen}
              modalData={modalData}
              disabled={
                spec?.source?.table_name === null ||
                previewTimeTravel ||
                !can_write
              }
              joinSeq={reveal ? seqOrder?.joins : null}
              refresh={() => getSampleData()}
              refreshLoading={isLoading}
            />
          </div>
          <NoCodeToolbar
            columnDetails={columnDetails}
            formulaColumns={formulaColumns}
            spec={spec}
            updateSpec={updateSpec}
            isLoading={isLoading}
            handleModalOpen={handleModalOpen}
            disabled={
              spec?.source?.table_name === null ||
              previewTimeTravel ||
              !can_write
            }
            seqOrder={seqOrder}
            reveal={reveal}
            openFormula={openFormula}
            setOpenFormula={setOpenFormula}
            selectedFormulaCol={selectedFormulaCol}
            setSelectedFormulaCol={setSelectedFormulaCol}
            disablePreview={disablePreview}
            modelName={nodeData.node.title}
            saveTransformation={saveTransformation}
            handleSetPresentation={handleSetPresentation}
            handleDeleteTransformation={handleDeleteTransformation}
            allColumnsDetails={columnDetails.allColumnsDetails}
            orginalColumnList={orginalColName}
            handleGetColumns={handleGetColumns}
            numberOfJoins={
              getJoinSpec(spec?.transform, transformIds?.JOIN)?.tables
                ?.length || 0
            }
          />

          <div>
            <Table
              className="no-code-table"
              components={components}
              columns={updatedColumns}
              dataSource={currentData}
              bordered
              loading={isLoading}
              locale={getLocale()}
              scroll={{
                x: "max-content",
                y: `${clientHeight}px`,
              }}
              pagination={false}
            />
            <div className="custom-pagination-container">
              <Space>
                <Space.Compact>
                  <Button
                    type="primary"
                    onClick={openQuickDeploy}
                    disabled={
                      spec?.source?.table_name === null ||
                      isLoading ||
                      previewTimeTravel ||
                      !can_write ||
                      !nodeData?.node?.title
                    }
                    icon={<PlayCircleOutlined />}
                  >
                    Quick Deploy
                  </Button>
                  <Dropdown
                    trigger={["click"]}
                    placement="bottomRight"
                    onOpenChange={(open) => {
                      if (open) fetchRecentRuns();
                    }}
                    dropdownRender={renderRecentRunsPanel}
                  >
                    <Button
                      type="primary"
                      icon={<DownOutlined />}
                      disabled={!nodeData?.node?.title}
                    />
                  </Dropdown>
                </Space.Compact>
                <Button
                  onClick={() => handleModalOpen("clearTransformsAlert")}
                  disabled={
                    spec?.source?.table_name === null || isLoading || !can_write
                  }
                  icon={<ClearOutlined />}
                >
                  Clear
                </Button>
                <Button
                  onClick={handleExportCsv}
                  disabled={
                    spec.source.table_name === null ||
                    isLoading ||
                    isExportingCsv ||
                    currentData.length === 0 ||
                    !can_write
                  }
                  loading={isExportingCsv}
                  icon={<DownloadOutlined />}
                >
                  Download CSV
                </Button>
                <Tooltip
                  title={
                    isLoading || isRevealSeqDisable()
                      ? "No transformation has been done yet"
                      : ""
                  }
                >
                  <Button
                    onClick={() => setReveal(!reveal)}
                    disabled={
                      spec?.source?.table_name === null ||
                      isLoading ||
                      isRevealSeqDisable() ||
                      !can_write
                    }
                  >
                    {reveal ? "Hide" : "Reveal"} Sequence
                  </Button>
                </Tooltip>
              </Space>
              {currentData.length > 0 && (
                <div>
                  <Pagination
                    className="custom-pagination"
                    current={currentPage}
                    pageSize={pageSize}
                    total={Math.min(totalCount, 1000)} // Limit total to 1000
                    showTotal={(total, range) =>
                      `Showing ${range[0]} to ${range[1]} of ${Math.min(
                        totalCount,
                        1000
                      )}entries`
                    }
                    showSizeChanger
                    onChange={handlePagination}
                  />
                </div>
              )}
            </div>
          </div>
        </Space>
        <Modal
          centered
          maskClosable={false}
          width={modalData[openedModal]?.width}
          title={
            <div className={"mb-16"}>
              {[
                "sourceDestinationChangeAlert",
                "clearTransformsAlert",
              ].includes(openedModal) && (
                <ExclamationCircleFilled className="warning-icon mr-10" />
              )}
              {modalData[openedModal]?.title}
            </div>
          }
          open={isModalOpen}
          onOk={() => handleOk(okText)}
          onCancel={handleModalClose}
          okText={okText}
          cancelText="Cancel"
          okButtonProps={{
            disabled:
              (columnDetails.columns.current?.length === 0 &&
                ![
                  "sourceDestination",
                  "joins",
                  "sourceDestinationChangeAlert",
                  "clearTransformsAlert",
                ].includes(openedModal)) ||
              isLoading ||
              !getCurrentModified() ||
              !can_write ||
              (openedModal === "joins" && joinValidationError),
            loading: isLoading,
          }}
          destroyOnClose
        >
          {modalData[openedModal]?.body}
        </Modal>
      </div>
      {spec?.source?.table_name !== null && (
        <EditorBottomSection>
          <div className="footer_wrapper">
            <Tabs items={BOTTOM_TABS} className="tabs_style" />
          </div>
        </EditorBottomSection>
      )}
      <Modal
        centered
        open={quickDeployModal.open}
        title={
          quickDeployModal.step === "empty"
            ? "No Deployment Job Found"
            : `Quick Deploy "${nodeData?.node?.title || ""}"`
        }
        onCancel={closeQuickDeploy}
        destroyOnClose
        footer={
          quickDeployModal.step === "empty" ? (
            <Space>
              <Button onClick={closeQuickDeploy}>Close</Button>
              <Button type="primary" onClick={goToScheduler}>
                Go to Scheduler
              </Button>
            </Space>
          ) : quickDeployModal.step === "loading" ? null : (
            <Space>
              <Button
                onClick={closeQuickDeploy}
                disabled={quickDeployModal.submitting}
              >
                Cancel
              </Button>
              <Button
                type="primary"
                loading={quickDeployModal.submitting}
                disabled={
                  !quickDeployModal.selectedTaskId ||
                  !quickDeployModal.selectedScope ||
                  !quickDeployModal.confirmed
                }
                onClick={confirmQuickDeploy}
              >
                {quickDeployModal.selectedScope === "job"
                  ? "Run full job"
                  : quickDeployModal.selectedScope === "model"
                  ? "Run model"
                  : "Run"}
              </Button>
            </Space>
          )
        }
      >
        {quickDeployModal.step === "loading" && (
          <div style={{ textAlign: "center", padding: 24 }}>
            <SpinnerLoader />
          </div>
        )}
        {quickDeployModal.step === "empty" && (
          <Typography.Paragraph>
            No job includes model <strong>{nodeData?.node?.title}</strong> yet.
            Create a scheduled job for this model first; then you can Quick
            Deploy against its configured environment.
          </Typography.Paragraph>
        )}
        {(quickDeployModal.step === "confirm" ||
          quickDeployModal.step === "pick") &&
          (() => {
            const selected = quickDeployModal.candidates.find(
              (c) => c.user_task_id === quickDeployModal.selectedTaskId
            );
            const multiple = quickDeployModal.candidates.length > 1;
            const modelCount = selected?.enabled_model_count || 0;
            return (
              <div>
                {multiple && (
                  <>
                    <Typography.Text strong>Pick a job</Typography.Text>
                    <Radio.Group
                      value={quickDeployModal.selectedTaskId}
                      onChange={(e) =>
                        setQuickDeployModal((prev) => ({
                          ...prev,
                          selectedTaskId: e.target.value,
                          selectedScope: null,
                          confirmed: false,
                        }))
                      }
                      style={{
                        display: "flex",
                        flexDirection: "column",
                        gap: 8,
                        margin: "8px 0 16px",
                      }}
                    >
                      {quickDeployModal.candidates.map((c) => (
                        <Radio key={c.user_task_id} value={c.user_task_id}>
                          <strong>{c.task_name}</strong>
                          <Typography.Text
                            type="secondary"
                            style={{ marginLeft: 8 }}
                          >
                            env: {c.environment_name || "(unnamed)"}
                          </Typography.Text>
                        </Radio>
                      ))}
                    </Radio.Group>
                  </>
                )}
                {!multiple && selected && (
                  <Typography.Paragraph style={{ marginBottom: 12 }}>
                    Job <strong>{selected.task_name}</strong> · environment{" "}
                    <strong>{selected.environment_name || "(unnamed)"}</strong>
                  </Typography.Paragraph>
                )}
                <Typography.Text strong>Choose scope</Typography.Text>
                <div
                  style={{
                    display: "grid",
                    gridTemplateColumns: "1fr 1fr",
                    gap: 12,
                    margin: "8px 0 16px",
                  }}
                >
                  {[
                    {
                      key: "model",
                      title: "Run this model only",
                      body: (
                        <>
                          Fast — runs just{" "}
                          <em>{nodeData?.node?.title || "this model"}</em>.
                        </>
                      ),
                    },
                    {
                      key: "job",
                      title: "Run full job",
                      body: (
                        <>
                          Runs all {modelCount || "enabled"} model
                          {modelCount === 1 ? "" : "s"} in the job.
                        </>
                      ),
                    },
                  ].map((opt) => {
                    const isSelected =
                      quickDeployModal.selectedScope === opt.key;
                    return (
                      <Card
                        key={opt.key}
                        hoverable={!quickDeployModal.submitting}
                        onClick={
                          quickDeployModal.submitting
                            ? undefined
                            : () =>
                                setQuickDeployModal((prev) => ({
                                  ...prev,
                                  selectedScope: opt.key,
                                  confirmed: false,
                                }))
                        }
                        styles={{ body: { padding: 12 } }}
                        style={{
                          cursor: quickDeployModal.submitting
                            ? "not-allowed"
                            : "pointer",
                          borderColor: isSelected
                            ? token.colorPrimary
                            : undefined,
                          boxShadow: isSelected
                            ? `0 0 0 2px ${token.controlOutline}`
                            : undefined,
                        }}
                      >
                        <Typography.Text strong>{opt.title}</Typography.Text>
                        <div>
                          <Typography.Text
                            type="secondary"
                            style={{ fontSize: 12 }}
                          >
                            {opt.body}
                          </Typography.Text>
                        </div>
                      </Card>
                    );
                  })}
                </div>
                <div
                  style={{
                    padding: "10px 12px",
                    border: `1px solid ${token.colorBorderSecondary}`,
                    borderRadius: token.borderRadiusLG,
                    background: token.colorFillQuaternary,
                  }}
                >
                  <Checkbox
                    checked={quickDeployModal.confirmed}
                    disabled={
                      !quickDeployModal.selectedScope ||
                      quickDeployModal.submitting
                    }
                    onChange={(e) =>
                      setQuickDeployModal((prev) => ({
                        ...prev,
                        confirmed: e.target.checked,
                      }))
                    }
                  >
                    I understand this will{" "}
                    {quickDeployModal.selectedScope === "job"
                      ? `run all ${modelCount || "enabled"} model${
                          modelCount === 1 ? "" : "s"
                        } in "${selected?.task_name || "this job"}"`
                      : quickDeployModal.selectedScope === "model"
                      ? `run "${nodeData?.node?.title || "this model"}" via "${
                          selected?.task_name || "the selected job"
                        }"`
                      : "run the selected scope"}{" "}
                    against environment{" "}
                    <strong>{selected?.environment_name || "(unnamed)"}</strong>
                    .
                  </Checkbox>
                </div>
              </div>
            );
          })()}
      </Modal>
    </div>
  );
}

NoCodeModel.propTypes = {
  nodeData: PropTypes.object.isRequired,
};

const getNodeBg = (type, categ, ind) => {
  if (type === "input") {
    return "#B0E3F960";
  } else if (type === "output") {
    return "#FFC8D260";
  }
  if (!type) {
    if (categ === "lineage") {
      return "#FFDD8A60";
    } else {
      const colors = [
        "#B0E3F960",
        "#FFC8D260",
        "#5D7B9660",
        "#FFDD8A60",
        "#C4DE8A60",
        "#33B8F160",
        "#B4C2CF60",
      ];

      return colors[ind % 7];
    }
  }
};
const iconMap = {
  aggregate: {
    icon: <CopyOutlined className="fs_25" style={{ color: "var(--black)" }} />,
    name: "aggreagate",
  },

  aggregate_filter: {
    icon: <CopyOutlined className="fs_25" style={{ color: "var(--black)" }} />,
    name: "agg filter",
  },

  distinct: {
    icon: <CopyOutlined className="fs_25" style={{ color: "var(--black)" }} />,
    name: "Drop dupe",
  },

  filters: {
    icon: (
      <FilterOutlined className="fs_25" style={{ color: "var(--black)" }} />
    ),
    name: "filter",
  },

  groups: {
    icon: (
      <ProfileOutlined className="fs_25" style={{ color: "var(--black)" }} />
    ),
    name: "group",
  },

  havings: {
    icon: (
      <ContainerOutlined className="fs_25" style={{ color: "var(--black)" }} />
    ),
    name: "having",
  },

  joins: {
    icon: <LinkOutlined className="fs_25" style={{ color: "var(--black)" }} />,
    name: "join",
  },

  sort_fields: {
    icon: (
      <LineHeightOutlined className="fs_25" style={{ color: "var(--black)" }} />
    ),
    name: "sort",
  },

  synthesize_column: {
    icon: (
      <PlusSquareOutlined className="fs_25" style={{ color: "var(--black)" }} />
    ),
    name: "add col",
  },

  unions: {
    icon: (
      <MergeCellsOutlined className="fs_25" style={{ color: "var(--black)" }} />
    ),
    name: "merge",
  },

  hidden_columns: {
    icon: (
      <EyeInvisibleOutlined
        className="fs_25"
        style={{ color: "var(--black)" }}
      />
    ),
    name: "hide",
  },
};

function LineageInfo({ helpText, color }) {
  return (
    <>
      <div
        style={{
          width: "10px",
          height: "10px",
          backgroundColor: color,
          display: "inline-block",
          border: "1px solid #3f8ec7",
          marginLeft: "20px",
        }}
      />
      <Text>{helpText}</Text>
    </>
  );
}

LineageInfo.propTypes = {
  helpText: PropTypes.string,
  color: PropTypes.string,
};

const getLayoutedElements = (nodes, edges, direction = "LR") => {
  const nodeWidth = 200; // Increased to accommodate longer labels
  const nodeHeight = 40;
  const topAndLeftPadding = 20;

  const dagreGraph = new dagre.graphlib.Graph();
  dagreGraph.setDefaultEdgeLabel(() => ({}));
  const isHorizontal = direction === "LR";

  // Configure dagre graph with spacing options to prevent overlapping
  dagreGraph.setGraph({
    rankdir: direction,
    nodesep: 60, // Spacing between nodes in same rank
    ranksep: 80, // Spacing between ranks
    edgesep: 20, // Spacing between edges
    marginx: 20,
    marginy: 20,
  });

  nodes.forEach((node) => {
    // Calculate dynamic width based on label length if available
    const labelLength = node.data?.originalLabel?.length || 10;
    // eslint-disable-next-line no-mixed-operators
    const dynamicWidth = Math.max(nodeWidth, labelLength * 8 + 80); // 8px per char + padding for icons
    dagreGraph.setNode(node.id, { width: dynamicWidth, height: nodeHeight });
  });

  edges.forEach((edge) => {
    dagreGraph.setEdge(edge.source, edge.target);
  });

  dagre.layout(dagreGraph);

  nodes.forEach((node) => {
    const nodeWithPosition = dagreGraph.node(node.id);
    const nodeW = nodeWithPosition.width || nodeWidth;
    const nodeH = nodeWithPosition.height || nodeHeight;

    node.targetPosition = isHorizontal ? Position.Left : Position.Top;
    node.sourcePosition = isHorizontal ? Position.Right : Position.Bottom;

    // Center the node on its calculated position
    /* eslint-disable no-mixed-operators */
    node.position = {
      // eslint-disable-next-line no-mixed-operators
      x: nodeWithPosition.x - nodeW / 2 + topAndLeftPadding,
      // eslint-disable-next-line no-mixed-operators
      y: nodeWithPosition.y - nodeH / 2 + topAndLeftPadding,
    };
    /* eslint-enable no-mixed-operators */

    return node;
  });

  return { nodes, edges };
};

export { NoCodeModel };
