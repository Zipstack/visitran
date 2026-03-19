import { useEffect, useState, useCallback, useRef } from "react";
import ReactFlow, {
  useNodesState,
  useEdgesState,
  Controls,
  MiniMap,
  Background,
} from "reactflow";
import dagre from "dagre";
import { Button, Tooltip, Spin, Tag, Select, Empty, Drawer, Tabs } from "antd";
import {
  ReloadOutlined,
  SwapOutlined,
  CopyOutlined,
  CloseOutlined,
} from "@ant-design/icons";
import { Prism as SyntaxHighlighter } from "react-syntax-highlighter";
import { oneDark, vs } from "react-syntax-highlighter/dist/esm/styles/prism";

import { useAxiosPrivate } from "../../../service/axios-service";
import { orgStore } from "../../../store/org-store";
import { useProjectStore } from "../../../store/project-store";
import { useUserStore } from "../../../store/user-store";
import { useNotificationService } from "../../../service/notification-service";
import { THEME } from "../../../common/constants";
import TableCardNode from "./table-card-node";
import LabeledEdge from "./labeled-edge";
import { LineageFlow } from "./lineage-flow";
import "reactflow/dist/style.css";
import "./sql-flow-tab.css";

// Register custom node types
const nodeTypes = {
  tableCard: TableCardNode,
};

// Register custom edge types
const edgeTypes = {
  labeled: LabeledEdge,
};

// Create dagre graph for layout calculation
const dagreGraph = new dagre.graphlib.Graph();
dagreGraph.setDefaultEdgeLabel(() => ({}));

// Dynamic height based on column count (header is taller with icon + subtitle)
// eslint-disable-next-line no-mixed-operators
// eslint-disable-next-line no-mixed-operators
const getNodeHeight = (columnCount) => Math.max(70, 54 + columnCount * 28);
const NODE_WIDTH = 220;

/**
 * Calculate automatic layout using dagre algorithm.
 * @param {Array} nodes - Array of node objects
 * @param {Array} edges - Array of edge objects
 * @param {string} direction - Layout direction ('TB' or 'LR')
 * @return {object} Object containing layouted nodes and edges
 */
const getLayoutedElements = (nodes, edges, direction = "TB") => {
  const isHorizontal = direction === "LR";
  dagreGraph.setGraph({
    rankdir: direction,
    nodesep: isHorizontal ? 100 : 120, // More horizontal space between nodes
    ranksep: isHorizontal ? 180 : 150, // More vertical space between ranks
    edgesep: 60, // More space for edges
    marginx: 50,
    marginy: 50,
  });

  // Clear previous graph
  dagreGraph.nodes().forEach((node) => dagreGraph.removeNode(node));

  nodes.forEach((node) => {
    const height = getNodeHeight(node.data?.columns?.length || 0);
    dagreGraph.setNode(node.id, { width: NODE_WIDTH, height });
  });

  edges.forEach((edge) => {
    dagreGraph.setEdge(edge.source, edge.target);
  });

  dagre.layout(dagreGraph);

  const layoutedNodes = nodes.map((node) => {
    const nodeWithPosition = dagreGraph.node(node.id);
    const height = getNodeHeight(node.data?.columns?.length || 0);
    return {
      ...node,
      /* eslint-disable no-mixed-operators */
      position: {
        // eslint-disable-next-line no-mixed-operators
        x: nodeWithPosition.x - NODE_WIDTH / 2,
        // eslint-disable-next-line no-mixed-operators
        y: nodeWithPosition.y - height / 2,
      },
      /* eslint-enable no-mixed-operators */
    };
  });

  return { nodes: layoutedNodes, edges };
};

/**
 * SQLFlowTab - Main component for Data Flow visualization.
 *
 * Displays an ER-diagram style view of table relationships across all models,
 * showing JOIN connections with column-level detail.
 * @return {JSX.Element} The rendered SQLFlowTab component
 */
function SQLFlowTab() {
  const axiosPrivate = useAxiosPrivate();
  const { selectedOrgId } = orgStore();
  const { projectId } = useProjectStore();
  const userDetails = useUserStore((state) => state.userDetails);
  const { notify } = useNotificationService();

  const isDarkTheme = userDetails?.currentTheme === THEME.DARK;

  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [allNodes, setAllNodes] = useState([]); // Unfiltered nodes
  const [allEdges, setAllEdges] = useState([]); // Unfiltered edges
  const [loading, setLoading] = useState(true);
  const [layoutDirection, setLayoutDirection] = useState("TB");
  const [stats, setStats] = useState(null);
  const [selectedEdge, setSelectedEdge] = useState(null);
  const [selectedNodeId, setSelectedNodeId] = useState(null); // Track selected node
  const [selectedSchema, setSelectedSchema] = useState("all"); // Schema filter
  const [availableSchemas, setAvailableSchemas] = useState([]); // List of schemas
  const [sqlModalData, setSqlModalData] = useState(null); // SQL modal state
  const [drawerTab, setDrawerTab] = useState("sql"); // "sql" or "lineage"

  const reactFlowInstance = useRef(null);
  const hasFetchedRef = useRef(false);

  // Get dependencies for a node (tables that flow INTO this node)
  const getNodeDependencies = useCallback(
    (nodeId) => {
      return edges
        .filter((edge) => edge.target === nodeId)
        .map((edge) => {
          const sourceNode = nodes.find((n) => n.id === edge.source);
          return {
            id: edge.source,
            label: sourceNode?.data?.label || edge.source,
            schema: sourceNode?.data?.schema,
            tableType: sourceNode?.data?.tableType,
            edgeType: edge.data?.edgeType,
          };
        });
    },
    [edges, nodes]
  );

  // Generate simple SELECT SQL for source tables
  const generateSourceSQL = useCallback((tableData) => {
    const { label, schema, columns } = tableData;
    const fullTableName = schema ? `${schema}.${label}` : label;
    const columnList =
      columns?.length > 0
        ? columns.map((c) => `    ${c.name}`).join(",\n")
        : "    *";
    return `SELECT\n${columnList}\nFROM ${fullTableName};`;
  }, []);

  // Common logic to build drawer data from table click
  const buildDrawerData = useCallback(
    (tableData) => {
      const nodeId = tableData.schema
        ? `${tableData.schema}.${tableData.label}`
        : tableData.label;
      const dependencies = getNodeDependencies(nodeId);

      let sql;
      if (tableData.tableType === "source") {
        sql = generateSourceSQL(tableData);
      } else {
        sql = tableData.sql || "-- No SQL available";
      }

      return { ...tableData, nodeId, dependencies, sql, loading: false };
    },
    [getNodeDependencies, generateSourceSQL]
  );

  // Handle SQL button click from node
  const handleSqlClick = useCallback(
    (tableData) => {
      setSqlModalData(buildDrawerData(tableData));
      setDrawerTab("sql");
    },
    [buildDrawerData]
  );

  // Handle Lineage button click from node
  const handleLineageClick = useCallback(
    (tableData) => {
      setSqlModalData(buildDrawerData(tableData));
      setDrawerTab("lineage");
    },
    [buildDrawerData]
  );

  // Copy SQL to clipboard
  const handleCopySQL = useCallback(() => {
    if (sqlModalData?.sql) {
      navigator.clipboard.writeText(sqlModalData.sql);
      notify({ message: "SQL copied to clipboard", type: "success" });
    }
  }, [sqlModalData, notify]);

  const fetchSQLFlow = useCallback(async () => {
    if (!projectId) return;

    setLoading(true);
    try {
      const response = await axiosPrivate({
        method: "GET",
        url: `/api/v1/visitran/${
          selectedOrgId || "default_org"
        }/project/${projectId}/sql-flow`,
      });

      const { nodes: rawNodes, edges: rawEdges, stats } = response.data;

      // Nodes come pre-configured from API
      const styledNodes = rawNodes.map((node) => ({
        ...node,
        type: "tableCard",
      }));

      // Style edges - curved bezier lines for cleaner flow
      const styledEdges = rawEdges.map((edge) => {
        const edgeType = edge.data?.edgeType;
        const hasLabel = ["join", "union", "reference"].includes(edgeType);

        return {
          ...edge,
          type: hasLabel ? "labeled" : "default", // "default" = bezier curve
          animated: false,
          style: {
            stroke: "#94A3B8",
            strokeWidth: 1.5,
          },
          markerEnd: {
            type: "arrowclosed",
            color: "#94A3B8",
            width: 15,
            height: 15,
          },
        };
      });

      // Store all nodes/edges for filtering
      setAllNodes(styledNodes);
      setAllEdges(styledEdges);
      setAvailableSchemas(stats?.schemas || []);

      // Apply layout
      const { nodes: layoutedNodes, edges: layoutedEdges } =
        getLayoutedElements(styledNodes, styledEdges, layoutDirection);

      setNodes(layoutedNodes);
      setEdges(layoutedEdges);
      setStats(stats);

      // Fit view after render
      setTimeout(() => {
        reactFlowInstance.current?.fitView({ padding: 0.2 });
      }, 100);
    } catch (error) {
      console.error("Failed to fetch SQL flow:", error);
      notify({ error });
    } finally {
      setLoading(false);
    }
  }, [projectId, selectedOrgId, axiosPrivate, layoutDirection, notify]);

  useEffect(() => {
    if (!hasFetchedRef.current) {
      fetchSQLFlow();
      hasFetchedRef.current = true;
    }
  }, [fetchSQLFlow]);

  const toggleLayout = useCallback(() => {
    const newDirection = layoutDirection === "TB" ? "LR" : "TB";
    setLayoutDirection(newDirection);

    const { nodes: layoutedNodes, edges: layoutedEdges } = getLayoutedElements(
      [...nodes],
      [...edges],
      newDirection
    );

    setNodes(layoutedNodes);
    setEdges(layoutedEdges);

    setTimeout(() => {
      reactFlowInstance.current?.fitView({ padding: 0.2 });
    }, 100);
  }, [layoutDirection, nodes, edges, setNodes, setEdges]);

  const onEdgeClick = useCallback((event, edge) => {
    setSelectedEdge(edge);
  }, []);

  // Handle node click - highlight connected edges
  const onNodeClick = useCallback(
    (event, node) => {
      const nodeId = node.id;

      // Toggle selection - if same node clicked, deselect
      if (selectedNodeId === nodeId) {
        setSelectedNodeId(null);
        // Reset all edges to default style
        setEdges((eds) =>
          eds.map((edge) => ({
            ...edge,
            style: {
              stroke: "#94A3B8",
              strokeWidth: 1.5,
            },
            markerEnd: {
              type: "arrowclosed",
              color: "#94A3B8",
              width: 15,
              height: 15,
            },
            data: { ...edge.data, highlighted: false },
          }))
        );
        return;
      }

      setSelectedNodeId(nodeId);

      // Highlight edges connected to this node
      setEdges((eds) =>
        eds.map((edge) => {
          const isInput = edge.target === nodeId; // Edge coming INTO this node
          const isOutput = edge.source === nodeId; // Edge going OUT of this node
          const isConnected = isInput || isOutput;

          if (isConnected) {
            return {
              ...edge,
              style: {
                stroke: isInput ? "#8B5CF6" : "#10B981", // Purple for input, Green for output
                strokeWidth: 2.5,
              },
              markerEnd: {
                type: "arrowclosed",
                color: isInput ? "#8B5CF6" : "#10B981",
                width: 18,
                height: 18,
              },
              data: {
                ...edge.data,
                highlighted: true,
                highlightType: isInput ? "input" : "output",
              },
            };
          }

          // Dim non-connected edges
          return {
            ...edge,
            style: {
              stroke: "#E2E8F0",
              strokeWidth: 1,
            },
            markerEnd: {
              type: "arrowclosed",
              color: "#E2E8F0",
              width: 12,
              height: 12,
            },
            data: { ...edge.data, highlighted: false },
          };
        })
      );
    },
    [selectedNodeId, setEdges]
  );

  // Handle pane click - deselect node
  const onPaneClick = useCallback(() => {
    if (selectedNodeId) {
      setSelectedNodeId(null);
      // Reset all edges to default style
      setEdges((eds) =>
        eds.map((edge) => ({
          ...edge,
          style: {
            stroke: "#94A3B8",
            strokeWidth: 1.5,
          },
          markerEnd: {
            type: "arrowclosed",
            color: "#94A3B8",
            width: 15,
            height: 15,
          },
          data: { ...edge.data, highlighted: false },
        }))
      );
    }
    setSelectedEdge(null);
  }, [selectedNodeId, setEdges]);

  // Filter nodes by schema
  const handleSchemaChange = useCallback(
    (schema) => {
      setSelectedSchema(schema);

      let filteredNodes = allNodes;
      let filteredEdges = allEdges;

      if (schema !== "all") {
        // Filter nodes by schema
        const nodeIds = new Set();
        filteredNodes = allNodes.filter((node) => {
          if (node.data.schema === schema) {
            nodeIds.add(node.id);
            return true;
          }
          return false;
        });

        // Filter edges to only include those between filtered nodes
        filteredEdges = allEdges.filter(
          (edge) => nodeIds.has(edge.source) && nodeIds.has(edge.target)
        );
      }

      const { nodes: layoutedNodes, edges: layoutedEdges } =
        getLayoutedElements(
          [...filteredNodes],
          [...filteredEdges],
          layoutDirection
        );

      setNodes(layoutedNodes);
      setEdges(layoutedEdges);

      setTimeout(() => {
        reactFlowInstance.current?.fitView({ padding: 0.2 });
      }, 100);
    },
    [allNodes, allEdges, layoutDirection, setNodes, setEdges]
  );

  const handleRefresh = useCallback(() => {
    hasFetchedRef.current = false;
    setSelectedSchema("all");
    fetchSQLFlow();
  }, [fetchSQLFlow]);

  const isEmpty = !loading && nodes.length === 0;

  return (
    <div className="sql-flow-container">
      {/* Header */}
      <div className="sql-flow-header">
        <div className="sql-flow-controls">
          <Tooltip title="Refresh">
            <Button
              icon={<ReloadOutlined spin={loading} />}
              onClick={handleRefresh}
              disabled={loading}
            />
          </Tooltip>
          <Tooltip
            title={`Switch to ${
              layoutDirection === "TB" ? "Horizontal" : "Vertical"
            }`}
          >
            <Button
              icon={<SwapOutlined rotate={layoutDirection === "TB" ? 0 : 90} />}
              onClick={toggleLayout}
              disabled={loading || isEmpty}
            />
          </Tooltip>

          {/* Schema Filter Dropdown */}
          {availableSchemas.length > 1 && (
            <Select
              value={selectedSchema}
              onChange={handleSchemaChange}
              style={{ width: 150 }}
              disabled={loading}
            >
              <Select.Option value="all">All Schemas</Select.Option>
              {availableSchemas.map((schema) => (
                <Select.Option key={schema} value={schema}>
                  {schema}
                </Select.Option>
              ))}
            </Select>
          )}
        </div>

        <div className="sql-flow-legend">
          <span className="legend-item">
            <span className="legend-color" style={{ background: "#8B5CF6" }} />
            Source
          </span>
          <span className="legend-item">
            <span className="legend-color" style={{ background: "#3B82F6" }} />
            Model
          </span>
          <span className="legend-item">
            <span className="legend-color" style={{ background: "#10B981" }} />
            Final
          </span>
          <span className="legend-item legend-divider">|</span>
          <span className="legend-item">
            <span className="legend-line dashed" />
            Data Flow
          </span>
        </div>

        {stats && !isEmpty && (
          <div className="sql-flow-stats">
            <Tag>{nodes.length} Tables</Tag>
            <Tag>{edges.length} Connections</Tag>
            {availableSchemas.length > 1 && (
              <Tag color="purple">{availableSchemas.length} Schemas</Tag>
            )}
          </div>
        )}
      </div>

      {/* Flow Canvas */}
      <div className="sql-flow-canvas">
        {loading ? (
          <div className="sql-flow-loading">
            <Spin size="large" tip="Loading Data Flow..." />
          </div>
        ) : isEmpty ? (
          <div className="sql-flow-empty">
            <Empty
              description="No models found"
              image={Empty.PRESENTED_IMAGE_SIMPLE}
            >
              <p className="empty-hint">
                Create models with source tables to see the data lineage graph.
              </p>
            </Empty>
          </div>
        ) : (
          <ReactFlow
            nodes={nodes.map((node) => ({
              ...node,
              data: {
                ...node.data,
                isSelected: node.id === selectedNodeId,
                onSqlClick: handleSqlClick,
                onLineageClick: handleLineageClick,
                isDarkTheme,
              },
            }))}
            edges={edges}
            nodeTypes={nodeTypes}
            edgeTypes={edgeTypes}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onNodeClick={onNodeClick}
            onEdgeClick={onEdgeClick}
            onPaneClick={onPaneClick}
            onInit={(instance) => {
              reactFlowInstance.current = instance;
            }}
            fitView
            attributionPosition="bottom-left"
            connectionLineType="default"
          >
            <Controls />
            <MiniMap
              nodeColor={(node) => {
                const colors = {
                  source: "#8B5CF6", // Purple
                  model: "#3B82F6", // Blue
                  terminal: "#10B981", // Green
                };
                return colors[node.data?.tableType] || "#ccc";
              }}
              maskColor="rgba(0,0,0,0.1)"
            />
            <Background variant="dots" gap={16} size={1} />
          </ReactFlow>
        )}
      </div>

      {/* Edge Info Panel */}
      {selectedEdge && (
        <div className="sql-flow-edge-info">
          <div className="edge-info-header">
            <strong>Connection Details</strong>
            <Button
              type="text"
              size="small"
              onClick={() => setSelectedEdge(null)}
            >
              ×
            </Button>
          </div>
          <div className="edge-info-content">
            <p>
              <strong>Type:</strong>{" "}
              {selectedEdge.data?.edgeType?.replace("_", " ") || "flow"}
            </p>
            <p>
              <strong>From:</strong> {selectedEdge.source}
            </p>
            <p>
              <strong>To:</strong> {selectedEdge.target}
            </p>
            <p>
              <strong>Model:</strong> {selectedEdge.data?.modelName || "N/A"}
            </p>
          </div>
        </div>
      )}

      {/* Details Panel (Right Drawer) */}
      <Drawer
        open={!!sqlModalData}
        onClose={() => setSqlModalData(null)}
        placement="right"
        width={420}
        closable={false}
        className={`sql-details-drawer ${isDarkTheme ? "dark-theme" : ""}`}
        styles={{ body: { padding: 0 } }}
      >
        {sqlModalData && (
          <div
            className={`sql-details-panel ${isDarkTheme ? "dark-theme" : ""}`}
          >
            {/* Header */}
            <div
              className="sql-panel-header"
              style={{
                borderLeftColor:
                  sqlModalData.tableType === "source"
                    ? "#8B5CF6"
                    : sqlModalData.tableType === "terminal"
                    ? "#10B981"
                    : "#3B82F6",
              }}
            >
              <Button
                type="text"
                size="small"
                icon={<CloseOutlined />}
                onClick={() => setSqlModalData(null)}
                className="close-btn"
              />
              <div className="sql-panel-uri">
                {sqlModalData.schema
                  ? `${sqlModalData.schema}.${sqlModalData.label}`
                  : sqlModalData.label}
              </div>
              <h2 className="sql-panel-title">{sqlModalData.label}</h2>
              <Tag
                color={
                  sqlModalData.tableType === "source"
                    ? "purple"
                    : sqlModalData.tableType === "terminal"
                    ? "green"
                    : "gold"
                }
              >
                {sqlModalData.tableType === "source"
                  ? "Source Table"
                  : sqlModalData.tableType === "terminal"
                  ? "Final Output"
                  : "Model"}
              </Tag>
            </div>

            {/* Tabs: SQL | Lineage */}
            <Tabs
              activeKey={drawerTab}
              onChange={setDrawerTab}
              size="small"
              style={{ padding: "0 16px" }}
              items={[
                {
                  key: "sql",
                  label: "SQL",
                  children: (
                    <>
                      {/* Depends On Section */}
                      {sqlModalData.dependencies?.length > 0 && (
                        <div className="sql-panel-section">
                          <div className="section-label">
                            Depends on ({sqlModalData.dependencies.length})
                          </div>
                          <div className="section-content">
                            <ul className="depends-list">
                              {sqlModalData.dependencies.map((dep) => (
                                <li key={dep.id} className="depends-item">
                                  <span className="depends-name">
                                    {dep.label}
                                  </span>
                                  <span className="depends-path">
                                    ({dep.schema || "default"})
                                  </span>
                                  {dep.edgeType &&
                                    dep.edgeType !== "source" && (
                                      <Tag
                                        size="small"
                                        className="depends-type"
                                      >
                                        {dep.edgeType.toUpperCase()}
                                      </Tag>
                                    )}
                                </li>
                              ))}
                            </ul>
                          </div>
                        </div>
                      )}

                      {/* Columns Section */}
                      <div className="sql-panel-section">
                        <div className="section-label">
                          Columns ({sqlModalData.columns?.length || 0})
                        </div>
                        <div className="section-content columns-list">
                          {sqlModalData.columns?.length > 0 ? (
                            sqlModalData.columns.map((col) => (
                              <span key={col.name} className="column-chip">
                                {col.name}
                              </span>
                            ))
                          ) : (
                            <span className="no-columns-text">
                              No columns defined
                            </span>
                          )}
                        </div>
                      </div>

                      {/* SQL Section */}
                      <div className="sql-panel-section sql-section">
                        <div
                          className="section-label"
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
                              onClick={handleCopySQL}
                            />
                          </Tooltip>
                        </div>
                        <div
                          className="section-content"
                          style={{
                            maxHeight: "350px",
                            overflowY: "auto",
                            borderRadius: "4px",
                            border: "1px solid var(--left-border)",
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
                            {sqlModalData.sql || "-- No SQL available"}
                          </SyntaxHighlighter>
                        </div>
                      </div>
                    </>
                  ),
                },
                {
                  key: "lineage",
                  label: "Sequence",
                  children: sqlModalData.modelName ? (
                    <LineageFlow
                      isOpen={drawerTab === "lineage"}
                      modelName={sqlModalData.modelName}
                    />
                  ) : (
                    <Empty
                      description="No lineage available for source tables"
                      style={{ marginTop: 50 }}
                      image={Empty.PRESENTED_IMAGE_SIMPLE}
                    />
                  ),
                },
              ]}
            />
          </div>
        )}
      </Drawer>
    </div>
  );
}

export { SQLFlowTab };
