import { useState, useEffect, useCallback, useRef } from "react";
import PropTypes from "prop-types";
import { Button, Space, Tooltip, Typography } from "antd";
import {
  CloseOutlined,
  CopyOutlined,
  DatabaseOutlined,
  InfoCircleOutlined,
  ReloadOutlined,
  SwapOutlined,
  ApartmentOutlined,
  FilterOutlined,
  LinkOutlined,
  ProfileOutlined,
  LineHeightOutlined,
  PlusSquareOutlined,
  MergeCellsOutlined,
  EyeInvisibleOutlined,
  ContainerOutlined,
  TableOutlined,
  ArrowDownOutlined,
} from "@ant-design/icons";
import ReactFlow, {
  Controls,
  ControlButton,
  MarkerType,
  Position,
  useNodesState,
  useEdgesState,
} from "reactflow";
import dagre from "dagre";
import { Prism as SyntaxHighlighter } from "react-syntax-highlighter";
import { oneDark, vs } from "react-syntax-highlighter/dist/esm/styles/prism";

import { useAxiosPrivate } from "../../../service/axios-service.js";
import { useProjectStore } from "../../../store/project-store.js";
import { useUserStore } from "../../../store/user-store.js";
import { orgStore } from "../../../store/org-store.js";
import { THEME } from "../../../common/constants.js";
import { SpinnerLoader } from "../../../widgets/spinner_loader/index.js";
import { useNotificationService } from "../../../service/notification-service.js";
import { Tech } from "../../../base/icons/index.js";
import { applyScopedStyles } from "../lineage-utils.js";

import "reactflow/dist/style.css";
import "./lineage-tab.css";

const { Text } = Typography;

function LineageInfo({ helpText, colorClass }) {
  return (
    <>
      <div
        className={`legend-item ${colorClass}`}
        style={{
          width: "10px",
          height: "10px",
          display: "inline-block",
          marginLeft: "20px",
        }}
      />
      <Text>{helpText}</Text>
    </>
  );
}

LineageInfo.propTypes = {
  helpText: PropTypes.string,
  colorClass: PropTypes.string,
};

const getNodeBg = (type) => {
  if (type === "input") {
    return "#B0E3F960";
  } else if (type === "output") {
    return "#FFC8D260";
  }
  return "#FFDD8A60";
};

// Icon map for sequence step types (handles both singular and plural forms)
const sequenceIconMap = {
  // Aggregate
  aggregate: { icon: <CopyOutlined />, name: "Aggregate", color: "#B0E3F9" },
  aggregates: { icon: <CopyOutlined />, name: "Aggregate", color: "#B0E3F9" },
  aggregate_filter: {
    icon: <CopyOutlined />,
    name: "Agg Filter",
    color: "#B0E3F9",
  },
  groups_and_aggregate: {
    icon: <CopyOutlined />,
    name: "Aggregate",
    color: "#B0E3F9",
  },
  // Filter
  filter: { icon: <FilterOutlined />, name: "Filter", color: "#FFDD8A" },
  filters: { icon: <FilterOutlined />, name: "Filter", color: "#FFDD8A" },
  // Distinct
  distinct: { icon: <CopyOutlined />, name: "Distinct", color: "#C4DE8A" },
  // Group
  group: { icon: <ProfileOutlined />, name: "Group", color: "#33B8F1" },
  groups: { icon: <ProfileOutlined />, name: "Group", color: "#33B8F1" },
  // Having
  having: { icon: <ContainerOutlined />, name: "Having", color: "#B4C2CF" },
  havings: { icon: <ContainerOutlined />, name: "Having", color: "#B4C2CF" },
  // Join
  join: { icon: <LinkOutlined />, name: "Join", color: "#FFC8D2" },
  joins: { icon: <LinkOutlined />, name: "Join", color: "#FFC8D2" },
  // Sort
  sort: { icon: <LineHeightOutlined />, name: "Sort", color: "#5D7B96" },
  sort_fields: { icon: <LineHeightOutlined />, name: "Sort", color: "#5D7B96" },
  // Synthesize / Add Column
  synthesize: {
    icon: <PlusSquareOutlined />,
    name: "Add Column",
    color: "#C4DE8A",
  },
  synthesize_column: {
    icon: <PlusSquareOutlined />,
    name: "Add Column",
    color: "#C4DE8A",
  },
  // Union / Merge
  union: { icon: <MergeCellsOutlined />, name: "Merge", color: "#33B8F1" },
  unions: { icon: <MergeCellsOutlined />, name: "Merge", color: "#33B8F1" },
  // Hidden columns
  hidden_columns: {
    icon: <EyeInvisibleOutlined />,
    name: "Hide",
    color: "#B4C2CF",
  },
};

// Get icon info for a sequence step
const getStepIconInfo = (label, type) => {
  if (type === "input") {
    return { icon: <DatabaseOutlined />, name: label, color: "#B0E3F9" };
  }
  if (type === "output") {
    return { icon: <TableOutlined />, name: label, color: "#FFC8D2" };
  }
  // Try exact match first, then lowercase
  const normalizedLabel = label?.toLowerCase?.() || label;
  return (
    sequenceIconMap[label] ||
    sequenceIconMap[normalizedLabel] || {
      icon: <ApartmentOutlined />,
      name: label,
      color: "#FFDD8A",
    }
  );
};

const getLayoutedElements = (nodes, edges, direction = "TB") => {
  const nodeWidth = 200; // Increased to accommodate longer labels
  const nodeHeight = 40;
  const topAndLeftPadding = 20;

  const dagreGraph = new dagre.graphlib.Graph();
  dagreGraph.setDefaultEdgeLabel(() => ({}));
  const isHorizontal = direction === "LR";

  // Configure dagre graph with spacing options to prevent overlapping
  dagreGraph.setGraph({
    rankdir: direction,
    nodesep: 60, // Horizontal spacing between nodes in same rank
    ranksep: 80, // Vertical spacing between ranks
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
      x: nodeWithPosition.x - nodeW / 2 + topAndLeftPadding,
      y: nodeWithPosition.y - nodeH / 2 + topAndLeftPadding,
    };
    /* eslint-enable no-mixed-operators */

    return node;
  });

  return { nodes, edges };
};

// Transform function moved outside component to avoid re-creation
const transformLineageData = (data) => {
  const position = { x: 0, y: 0 };
  data["edges"] = data.edges.map((edge) => ({
    ...edge,
    markerEnd: {
      type: MarkerType.ArrowClosed,
    },
  }));

  data["nodes"] = data["nodes"].map((node) => {
    const el = node.data.label;
    node.data.originalLabel = el;

    node.data.typeIcon =
      node.type === "input" ? (
        <DatabaseOutlined style={{ color: "var(--black)" }} />
      ) : (
        <Tech style={{ color: "var(--black)" }} />
      );

    node.data.iconStyleClass =
      node.type === "input"
        ? "icon_style node_color_blue"
        : !node.type
        ? "node_color_yellow icon_style"
        : "icon_style node_color_pink";

    node["data"]["label"] = (
      <>
        {node.type === "input" ? (
          <div className="icon_style node_color_blue">
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
        <Text style={{ padding: "4px 12px" }}>{node.data.originalLabel}</Text>
        {/* Sequence icon - only show for model nodes (not input/source) */}
        {node.type !== "input" && (
          <Space className="lineage-info-icon" data-action="sequence">
            <Tooltip title="View transformation sequence">
              <ApartmentOutlined style={{ color: "white", fontSize: "12px" }} />
            </Tooltip>
          </Space>
        )}
        <Space className="lineage-info-icon" data-action="info">
          <Tooltip title="View model info">
            <InfoCircleOutlined style={{ color: "white", fontSize: "12px" }} />
          </Tooltip>
        </Space>
      </>
    );

    return {
      ...node,
      position,
      style: {
        backgroundColor: getNodeBg(node.type),
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
};

function LineageTab({ nodeData, selectedModelName }) {
  const axios = useAxiosPrivate();
  const { selectedOrgId } = orgStore();
  const { projectId } = useProjectStore();
  const userDetails = useUserStore((state) => state.userDetails);
  const { notify } = useNotificationService();

  const [lineageData, setLineageData] = useState(null);
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [infoStack, setInfoStack] = useState([]);
  const [reactFlowInstance, setReactFlowInstance] = useState(null);
  const [lineageNodeCount, setLineageNodeCount] = useState(0);
  const [layoutDirection, setLayoutDirection] = useState("TB"); // TB = vertical, LR = horizontal

  // Sequence panel state
  const [sequencePanel, setSequencePanel] = useState(null);
  const [sequencePanelPosition, setSequencePanelPosition] = useState({
    x: 100,
    y: 100,
  });
  const [isDraggingPanel, setIsDraggingPanel] = useState(false);
  const dragStartRef = useRef({ x: 0, y: 0 });
  const hasFetchedRef = useRef(false);

  const isDarkTheme = userDetails.currentTheme === THEME.DARK;

  // Track node count changes for zoom adjustment
  useEffect(() => {
    const currentLength = nodes?.length || 0;
    if (currentLength !== lineageNodeCount) {
      setLineageNodeCount(currentLength);
    }
  }, [nodes?.length, lineageNodeCount]);

  // Handle zoom adjustment for small node counts
  useEffect(() => {
    if (reactFlowInstance && lineageNodeCount > 0) {
      setTimeout(() => {
        if (lineageNodeCount <= 3) {
          reactFlowInstance.fitView();
          reactFlowInstance.zoomTo(0.7);
        } else {
          reactFlowInstance.fitView();
        }
      }, 100);
    }
  }, [lineageNodeCount, reactFlowInstance]);

  const handleInfoClick = useCallback(
    (node) => {
      let borderColor;
      if (node.type === "input") {
        borderColor = "#B0E3F9";
      } else if (node.type === "output") {
        borderColor = "#FFC8D2";
      } else {
        borderColor = "#FFDD8A";
      }

      setInfoStack((prevStack) => {
        const isInfoBoxOpen = prevStack.some((box) => box.id === node.id);

        if (isInfoBoxOpen) {
          setNodes((nds) =>
            nds.map((n) => ({
              ...n,
              style: {
                ...n.style,
                border: "1px solid var(--black)",
              },
            }))
          );
          return prevStack.filter((b) => b.id !== node.id);
        } else {
          const plainTextLabel =
            typeof node.data.originalLabel === "string"
              ? node.data.originalLabel
              : node.data.label || "Node";

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
              setInfoStack([nodeInfo]);
            })
            .catch((error) => {
              console.error("Error fetching node info:", error);
              notify({ error });
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
              return {
                ...n,
                style: {
                  ...n.style,
                  border: "1px solid var(--black)",
                },
              };
            })
          );

          return [tempNodeInfo];
        }
      });
    },
    [selectedOrgId, projectId, setNodes]
  );

  const bringToFront = useCallback((id) => {
    setInfoStack((prev) => {
      const selected = prev.find((b) => b.id === id);
      const rest = prev.filter((b) => b.id !== id);
      return [selected, ...rest];
    });
  }, []);

  const handleRefresh = useCallback(() => {
    if (!projectId) return;

    // Clear current state
    setLineageData(null);
    setNodes([]);
    setEdges([]);
    setInfoStack([]);

    const requestOptions = {
      method: "GET",
      url: `/api/v1/visitran/${
        selectedOrgId || "default_org"
      }/project/${projectId}/lineage`,
    };

    axios(requestOptions)
      .then(({ data: { data } }) => {
        const transformedData = transformLineageData(data);
        setLineageData(transformedData);
        const { nodes: layoutedNodes, edges: layoutedEdges } =
          getLayoutedElements(
            transformedData.nodes,
            transformedData.edges,
            layoutDirection
          );
        if (selectedModelName) {
          const scoped = applyScopedStyles(
            layoutedNodes,
            layoutedEdges,
            selectedModelName
          );
          setNodes(scoped.nodes);
          setEdges(scoped.edges);
        } else {
          setNodes(layoutedNodes);
          setEdges(layoutedEdges);
        }
      })
      .catch((error) => {
        console.error(error);
        notify({ error });
        setLineageData({});
      });
  }, [
    projectId,
    selectedOrgId,
    setNodes,
    setEdges,
    layoutDirection,
    selectedModelName,
  ]);

  const handleToggleLayout = useCallback(() => {
    const newDirection = layoutDirection === "TB" ? "LR" : "TB";
    setLayoutDirection(newDirection);

    // Re-layout existing nodes with new direction
    if (lineageData && lineageData.nodes && lineageData.edges) {
      const { nodes: layoutedNodes, edges: layoutedEdges } =
        getLayoutedElements(lineageData.nodes, lineageData.edges, newDirection);
      if (selectedModelName) {
        const scoped = applyScopedStyles(
          layoutedNodes,
          layoutedEdges,
          selectedModelName
        );
        setNodes(scoped.nodes);
        setEdges(scoped.edges);
      } else {
        setNodes(layoutedNodes);
        setEdges(layoutedEdges);
      }
    }
  }, [layoutDirection, lineageData, setNodes, setEdges, selectedModelName]);

  // Fetch sequence data for a model
  const fetchSequenceData = useCallback(
    (modelName) => {
      const requestOptions = {
        method: "GET",
        url: `/api/v1/visitran/${
          selectedOrgId || "default_org"
        }/project/${projectId}/lineage/${modelName}/info`,
      };

      return axios(requestOptions)
        .then(({ data: { data } }) => {
          return data;
        })
        .catch((error) => {
          console.error("Error fetching sequence data:", error);
          notify({ error });
          return null;
        });
    },
    [projectId, selectedOrgId]
  );

  // Handle sequence icon click - show draggable panel
  const handleSequenceClick = useCallback(
    async (node, event) => {
      const modelName = node.data?.originalLabel;
      if (!modelName) return;

      // Get container bounds to calculate relative position
      const container = event.target.closest(".lineage-tab-container");
      const containerRect = container?.getBoundingClientRect() || {
        left: 0,
        top: 0,
      };

      // Position panel near the clicked node (relative to container)
      const nodeElement = event.target.closest(".react-flow__node");
      const nodeRect = nodeElement?.getBoundingClientRect();

      if (nodeRect) {
        setSequencePanelPosition({
          x: nodeRect.right - containerRect.left + 10,
          y: nodeRect.top - containerRect.top,
        });
      } else {
        // Fallback to click position
        const clickRect = event.target.getBoundingClientRect();
        setSequencePanelPosition({
          x: clickRect.right - containerRect.left + 10,
          y: clickRect.top - containerRect.top,
        });
      }

      // Show loading state
      setSequencePanel({
        node,
        data: null,
        loading: true,
      });

      // Fetch sequence data
      const data = await fetchSequenceData(modelName);
      if (data) {
        setSequencePanel({
          node,
          data,
          loading: false,
        });
      } else {
        setSequencePanel(null);
      }
    },
    [fetchSequenceData]
  );

  // Close sequence panel
  const closeSequencePanel = useCallback(() => {
    setSequencePanel(null);
  }, []);

  // Handle panel drag start
  const handlePanelDragStart = useCallback(
    (e) => {
      e.preventDefault();
      setIsDraggingPanel(true);
      dragStartRef.current = {
        x: e.clientX - sequencePanelPosition.x,
        y: e.clientY - sequencePanelPosition.y,
      };
    },
    [sequencePanelPosition]
  );

  // Handle panel drag
  const handlePanelDrag = useCallback(
    (e) => {
      if (!isDraggingPanel) return;
      setSequencePanelPosition({
        x: e.clientX - dragStartRef.current.x,
        y: e.clientY - dragStartRef.current.y,
      });
    },
    [isDraggingPanel]
  );

  // Handle panel drag end
  const handlePanelDragEnd = useCallback(() => {
    setIsDraggingPanel(false);
  }, []);

  // Add mouse event listeners for dragging
  useEffect(() => {
    if (isDraggingPanel) {
      document.addEventListener("mousemove", handlePanelDrag);
      document.addEventListener("mouseup", handlePanelDragEnd);
      return () => {
        document.removeEventListener("mousemove", handlePanelDrag);
        document.removeEventListener("mouseup", handlePanelDragEnd);
      };
    }
  }, [isDraggingPanel, handlePanelDrag, handlePanelDragEnd]);

  // Handle node click - detect which icon was clicked
  const handleNodeClick = useCallback(
    (event, node) => {
      const target = event.target;
      const actionElement = target.closest("[data-action]");
      const action = actionElement?.dataset?.action;

      if (action === "sequence" && node.type !== "input") {
        handleSequenceClick(node, event);
      } else if (action === "info") {
        handleInfoClick(node);
      } else {
        // Default: show info
        handleInfoClick(node);
      }
    },
    [handleSequenceClick, handleInfoClick]
  );

  // Fetch lineage data only once on mount
  useEffect(() => {
    if (!projectId || hasFetchedRef.current) return;

    hasFetchedRef.current = true;

    const requestOptions = {
      method: "GET",
      url: `/api/v1/visitran/${
        selectedOrgId || "default_org"
      }/project/${projectId}/lineage`,
    };

    axios(requestOptions)
      .then(({ data: { data } }) => {
        const transformedData = transformLineageData(data);
        setLineageData(transformedData);
        const { nodes: layoutedNodes, edges: layoutedEdges } =
          getLayoutedElements(
            transformedData.nodes,
            transformedData.edges,
            "TB"
          );
        if (selectedModelName) {
          const scoped = applyScopedStyles(
            layoutedNodes,
            layoutedEdges,
            selectedModelName
          );
          setNodes(scoped.nodes);
          setEdges(scoped.edges);
        } else {
          setNodes(layoutedNodes);
          setEdges(layoutedEdges);
        }
      })
      .catch((error) => {
        console.error(error);
        notify({ error });
        setLineageData({});
      });
  }, [projectId, selectedOrgId, setNodes, setEdges, selectedModelName]);

  if (!lineageData) {
    return <SpinnerLoader />;
  }

  if (lineageData && !lineageData.nodes) {
    return (
      <div className="lineage-tab-error">
        <Text>Error in fetching lineage data</Text>
      </div>
    );
  }

  return (
    <div className="lineage-tab-container">
      <div className="lineage-tab-header">
        <Space>
          <LineageInfo
            helpText="&nbsp;Parent / Independent model"
            colorClass="legend-color-blue"
          />
          <LineageInfo
            helpText="&nbsp;Derived model"
            colorClass="legend-color-yellow"
          />
          <LineageInfo
            helpText="&nbsp;Terminal Nodes"
            colorClass="legend-color-pink"
          />
        </Space>
      </div>

      <div className="lineage-tab-content">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onInit={setReactFlowInstance}
          snapToGrid={true}
          onNodeClick={handleNodeClick}
          fitView
        >
          <Controls position="top-left" orientation="horizontal">
            <ControlButton onClick={handleRefresh} title="Refresh lineage">
              <ReloadOutlined />
            </ControlButton>
            <ControlButton
              onClick={handleToggleLayout}
              title={
                layoutDirection === "TB"
                  ? "Switch to horizontal layout"
                  : "Switch to vertical layout"
              }
            >
              <SwapOutlined rotate={layoutDirection === "TB" ? 90 : 0} />
            </ControlButton>
          </Controls>
        </ReactFlow>
      </div>

      <div className="info-stack-container">
        {infoStack.map((box, index) => (
          <div
            key={box.id}
            className={`info-box stacked ${isDarkTheme ? "dark-theme" : ""}`}
            style={{
              zIndex: 1000 - index,
              borderColor: box.borderColor,
              borderWidth: "2px",
              borderStyle: "solid",
            }}
            onClick={() => bringToFront(box.id)}
          >
            <div className="info-box-header">
              <div className="info-box-title">
                {nodes.find((n) => n.id === box.id)?.data?.typeIcon && (
                  <div
                    className={
                      nodes.find((n) => n.id === box.id)?.data
                        ?.iconStyleClass || "icon_style"
                    }
                  >
                    {nodes.find((n) => n.id === box.id)?.data?.typeIcon}
                  </div>
                )}
                <span className="info-box-title-text">{box.title}</span>
              </div>
              <Button
                type="text"
                size="small"
                className="info-box-close"
                icon={<CloseOutlined />}
                onClick={(e) => {
                  e.stopPropagation();
                  setInfoStack((prev) => prev.filter((b) => b.id !== box.id));
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
              <div className="info-box-section-label">Source Table</div>
              <div className="info-box-section-content">
                {box.content.sourceTable}
              </div>
            </div>

            <div className="info-box-section">
              <div className="info-box-section-label">Join Tables</div>
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
                      navigator.clipboard.writeText(box.content.sqlQuery);
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
                  maxHeight: "350px",
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
        ))}
      </div>

      {/* Sequence Panel */}
      {sequencePanel && (
        <div
          className={`sequence-panel ${isDarkTheme ? "dark-theme" : ""}`}
          style={{
            left: sequencePanelPosition.x,
            top: sequencePanelPosition.y,
          }}
        >
          <div
            className="sequence-panel-header"
            onMouseDown={handlePanelDragStart}
          >
            <div className="sequence-panel-title">
              <ApartmentOutlined style={{ fontSize: "16px" }} />
              <span>{sequencePanel.node?.data?.originalLabel}</span>
            </div>
            <Button
              type="text"
              size="small"
              icon={<CloseOutlined />}
              onClick={closeSequencePanel}
              style={{ color: isDarkTheme ? "#fff" : "#000" }}
            />
          </div>

          {sequencePanel.loading ? (
            <div style={{ padding: "20px", textAlign: "center" }}>
              <SpinnerLoader />
            </div>
          ) : sequencePanel.data?.sequence_lineage?.data?.nodes ? (
            <div className="sequence-panel-body">
              <div
                className="sequence-flow-container"
                style={{ background: isDarkTheme ? "#141414" : "#fafafa" }}
              >
                <div className="sequence-steps-vertical">
                  {sequencePanel.data.sequence_lineage.data.nodes.map(
                    (stepNode, index) => {
                      const stepInfo = getStepIconInfo(
                        stepNode.data?.label,
                        stepNode.type
                      );
                      const isLast =
                        index ===
                        sequencePanel.data.sequence_lineage.data.nodes.length -
                          1;

                      return (
                        <div
                          key={stepNode.id || index}
                          className="sequence-step-wrapper"
                        >
                          <div className="sequence-step-node">
                            <div
                              className="sequence-step-icon"
                              style={{ backgroundColor: stepInfo.color }}
                            >
                              {stepInfo.icon}
                            </div>
                            <div className="sequence-step-content">
                              <span className="sequence-step-label">
                                {stepInfo.name}
                              </span>
                            </div>
                          </div>
                          {!isLast && (
                            <div className="sequence-step-arrow-icon">
                              <ArrowDownOutlined />
                            </div>
                          )}
                        </div>
                      );
                    }
                  )}
                </div>
              </div>
            </div>
          ) : (
            <div
              style={{ padding: "20px", textAlign: "center", color: "#666" }}
            >
              No sequence data available
            </div>
          )}
        </div>
      )}
    </div>
  );
}

LineageTab.propTypes = {
  nodeData: PropTypes.object,
  selectedModelName: PropTypes.string,
};

export { LineageTab };
