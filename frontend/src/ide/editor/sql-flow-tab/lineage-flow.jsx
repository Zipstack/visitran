import { useEffect, useState, useCallback, useRef } from "react";
import PropTypes from "prop-types";
import ReactFlow, {
  useNodesState,
  useEdgesState,
  Controls,
  MarkerType,
} from "reactflow";
import dagre from "dagre";
import axios from "axios";
import { Spin, Empty, Typography } from "antd";

import { useProjectStore } from "../../../store/project-store";
import { useUserStore } from "../../../store/user-store";
import { orgStore } from "../../../store/org-store";
import { THEME } from "../../../common/constants";
import { LineageStepNode } from "./lineage-step-node";

const { Text } = Typography;

// Register custom node types (must be outside component to avoid re-creation)
const nodeTypes = {
  lineageStep: LineageStepNode,
};

// Transform labels (reused from Body.jsx)
const TRANSFORM_LABELS = {
  filters: "Filter",
  joins: "Join",
  groups: "Aggregation",
  groups_and_aggregation: "Aggregation",
  havings: "Having",
  sort: "Sort",
  sort_fields: "Sort",
  synthesize: "Add Column",
  synthesize_column: "Add Column",
  unions: "Merge",
  hidden_columns: "Hide Columns",
  distinct: "Drop Duplicate",
  aggregate: "Aggregation",
  aggregate_filter: "Aggregate Filter",
  pivot: "Pivot",
  unpivot: "Unpivot",
  combine_columns: "Combine Columns",
  find_and_replace: "Find & Replace",
  rename: "Rename",
};

// Extract transform type from key (reused from Body.jsx)
const getTransformType = (transformId) => {
  if (transformId.startsWith("groups_and_aggregation"))
    return "groups_and_aggregation";
  if (transformId.startsWith("find_and_replace")) return "find_and_replace";
  if (transformId.startsWith("combine_columns")) return "combine_columns";
  if (transformId.startsWith("hidden_columns")) return "hidden_columns";
  if (transformId.startsWith("synthesize")) return "synthesize";
  if (transformId.startsWith("filter")) return "filters";
  if (transformId.startsWith("join")) return "joins";
  if (transformId.startsWith("sort")) return "sort";
  if (transformId.startsWith("distinct")) return "distinct";
  if (transformId.startsWith("rename")) return "rename";
  if (transformId.startsWith("groups")) return "groups";
  if (transformId.startsWith("aggregate")) return "aggregate";
  if (transformId.startsWith("pivot")) return "pivot";
  if (transformId.startsWith("unpivot")) return "unpivot";
  if (transformId.startsWith("union")) return "unions";
  return transformId.split("_")[0];
};

// Generate brief summary for a transform's details
const getTransformSummary = (type, details) => {
  if (!details) return null;

  switch (type) {
    case "filters":
      if (details.conditions?.length === 1) {
        const c = details.conditions[0];
        return `${c.column} ${c.operator} ${c.value}`;
      }
      return details.conditions?.length
        ? `${details.conditions.length} conditions`
        : null;

    case "joins":
      if (details.joins?.length > 0) {
        const j = details.joins[0];
        return `${j.join_type?.toUpperCase()} JOIN ${j.right_table}`;
      }
      if (details.right_table) {
        return `${(details.join_type || "INNER").toUpperCase()} JOIN ${
          details.right_table
        }`;
      }
      return null;

    case "sort":
    case "sort_fields":
      if (details.columns?.length > 0) {
        const s = details.columns[0];
        const dir = s.order_by === "ASC" ? "↑" : "↓";
        const suffix =
          details.columns.length > 1
            ? ` +${details.columns.length - 1} more`
            : "";
        return `${s.column} ${dir} ${s.order_by}${suffix}`;
      }
      return null;

    case "synthesize":
    case "synthesize_column":
      if (details.columns?.length === 1) {
        return details.columns[0].name;
      }
      return details.columns?.length
        ? `${details.columns.length} new columns`
        : null;

    case "rename":
      if (details.mappings?.length === 1) {
        const m = details.mappings[0];
        return `${m.old_name} → ${m.new_name}`;
      }
      return details.mappings?.length
        ? `${details.mappings.length} columns renamed`
        : null;

    case "hidden_columns":
      return details.columns?.length
        ? `${details.columns.length} columns hidden`
        : null;

    case "distinct":
      return details.columns?.length > 0
        ? `On ${details.columns.length} columns`
        : "All columns";

    case "groups":
    case "groups_and_aggregation":
    case "aggregate":
      if (details.group_by?.length > 0) {
        return `Group by ${details.group_by.join(", ")}`;
      }
      return details.aggregations?.length
        ? `${details.aggregations.length} aggregations`
        : null;

    case "unions":
      return details.tables?.length
        ? `${details.tables.length} tables merged`
        : null;

    case "combine_columns":
      return details.combinations?.length
        ? `${details.combinations.length} combinations`
        : null;

    case "find_and_replace":
      return details.replacements?.length
        ? `${details.replacements.length} replacements`
        : null;

    case "pivot":
      return details.pivot_column ? `Pivot on ${details.pivot_column}` : null;

    case "unpivot":
      return details.columns?.length
        ? `${details.columns.length} columns unpivoted`
        : null;

    default:
      return null;
  }
};

// Layout nodes using dagre
const NODE_WIDTH = 320;
const NODE_HEIGHT = 56;

const getLayoutedElements = (nodes, edges) => {
  const dagreGraph = new dagre.graphlib.Graph();
  dagreGraph.setDefaultEdgeLabel(() => ({}));
  dagreGraph.setGraph({
    rankdir: "TB",
    nodesep: 40,
    ranksep: 50,
    marginx: 10,
    marginy: 10,
  });

  nodes.forEach((node) => {
    dagreGraph.setNode(node.id, {
      width: NODE_WIDTH,
      height: NODE_HEIGHT,
    });
  });

  edges.forEach((edge) => {
    dagreGraph.setEdge(edge.source, edge.target);
  });

  dagre.layout(dagreGraph);

  const layoutedNodes = nodes.map((node) => {
    const pos = dagreGraph.node(node.id);
    return {
      ...node,
      /* eslint-disable no-mixed-operators */
      position: {
        x: pos.x - NODE_WIDTH / 2,
        y: pos.y - NODE_HEIGHT / 2,
      },
      /* eslint-enable no-mixed-operators */
    };
  });

  return { nodes: layoutedNodes, edges };
};

function LineageFlow({ modelName, isOpen }) {
  const { projectId } = useProjectStore();
  const selectedOrgId = orgStore((state) => state.selectedOrgId);
  const userDetails = useUserStore((state) => state.userDetails);
  const isDarkTheme = userDetails?.currentTheme === THEME.DARK;

  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const reactFlowRef = useRef(null);
  const prevModelRef = useRef(null);

  const processData = useCallback(
    (response) => {
      const sequence_orders = response.data.sequence_orders;
      const model_data = response.data.model_data || {};
      const transformation_details = response.data.transformation_details || {};

      if (!sequence_orders || Object.keys(sequence_orders).length === 0) {
        setNodes([]);
        setEdges([]);
        return;
      }

      // Build ordered transform list
      const transforms = Object.entries(sequence_orders)
        .filter(
          ([, order]) => order !== null && order !== undefined && order > 0
        )
        .sort(([, a], [, b]) => a - b)
        .map(([key]) => {
          const transformType = getTransformType(key);
          const details = transformation_details[key] || null;
          const summary = getTransformSummary(transformType, details);

          return {
            id: `transform-${key}`,
            stepType: transformType,
            label:
              TRANSFORM_LABELS[transformType] ||
              key.replace(/_/g, " ").replace(/\b\w/g, (l) => l.toUpperCase()),
            detail: summary,
          };
        });

      const totalItems = transforms.length + 2; // +2 for source and output

      // Build nodes
      const sourceTable = model_data?.source;
      const outputTable = model_data?.model;

      const flowNodes = [
        {
          id: "source",
          type: "lineageStep",
          data: {
            stepType: "source",
            label: "SOURCE",
            tableName: sourceTable?.schema_name
              ? `${sourceTable.schema_name}.${sourceTable.table_name}`
              : sourceTable?.table_name,
            isDarkTheme,
            isFirst: true,
            isLast: totalItems === 1,
          },
          position: { x: 0, y: 0 },
        },
        ...transforms.map((t, idx) => ({
          id: t.id,
          type: "lineageStep",
          data: {
            stepType: t.stepType,
            label: t.label,
            detail: t.detail,
            isDarkTheme,
            isFirst: false,
            isLast: idx === transforms.length - 1 && !outputTable,
          },
          position: { x: 0, y: 0 },
        })),
        {
          id: "output",
          type: "lineageStep",
          data: {
            stepType: "output",
            label: "OUTPUT",
            tableName: outputTable?.schema_name
              ? `${outputTable.schema_name}.${outputTable.table_name}`
              : outputTable?.table_name,
            isDarkTheme,
            isFirst: false,
            isLast: true,
          },
          position: { x: 0, y: 0 },
        },
      ];

      // Build edges: source → transform1 → transform2 → ... → output
      const nodeIds = flowNodes.map((n) => n.id);
      const flowEdges = [];
      for (let i = 0; i < nodeIds.length - 1; i++) {
        flowEdges.push({
          id: `edge-${nodeIds[i]}-${nodeIds[i + 1]}`,
          source: nodeIds[i],
          target: nodeIds[i + 1],
          type: "smoothstep",
          animated: true,
          style: { stroke: "#94A3B8", strokeWidth: 1.5 },
          markerEnd: {
            type: MarkerType.ArrowClosed,
            color: "#94A3B8",
            width: 12,
            height: 12,
          },
        });
      }

      // Apply dagre layout
      const { nodes: layoutedNodes, edges: layoutedEdges } =
        getLayoutedElements(flowNodes, flowEdges);

      setNodes(layoutedNodes);
      setEdges(layoutedEdges);

      // Fit view after render
      setTimeout(() => {
        reactFlowRef.current?.fitView({
          padding: 0.1,
          includeHiddenNodes: false,
        });
      }, 100);
    },
    [isDarkTheme, setNodes, setEdges]
  );

  // Fit view when tab becomes visible
  useEffect(() => {
    if (isOpen && nodes.length > 0 && reactFlowRef.current) {
      setTimeout(() => {
        reactFlowRef.current.fitView({
          padding: 0.1,
          includeHiddenNodes: false,
        });
      }, 50);
    }
  }, [isOpen, nodes.length]);

  useEffect(() => {
    if (!isOpen || !modelName || !projectId) return;

    // Don't re-fetch if same model
    if (prevModelRef.current === modelName && nodes.length > 0) return;
    prevModelRef.current = modelName;

    const fetchData = async () => {
      setLoading(true);
      setError(null);
      try {
        const response = await axios.get(
          `/api/v1/visitran/${
            selectedOrgId || "default_org"
          }/project/${projectId}/reload?file_name=${modelName}`
        );
        processData(response);
      } catch (err) {
        setError(
          err.response?.data?.message ||
            err.response?.data?.error ||
            err.message ||
            "Failed to load lineage"
        );
        setNodes([]);
        setEdges([]);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [isOpen, modelName, projectId, selectedOrgId, processData]);

  if (loading) {
    return (
      <div className="lineage-flow-loading">
        <Spin size="large" />
        <Text style={{ display: "block", marginTop: 16 }}>
          Loading lineage...
        </Text>
      </div>
    );
  }

  if (error) {
    return (
      <div className="lineage-flow-loading">
        <Empty
          description={<Text type="danger">{error}</Text>}
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        />
      </div>
    );
  }

  if (!nodes.length) {
    return (
      <div className="lineage-flow-loading">
        <Empty
          description="No transformations found"
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        />
      </div>
    );
  }

  return (
    <div className="lineage-flow-container">
      <div className="lineage-flow-subtitle">
        Shows the sequence in which transformations are applied
      </div>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onInit={(instance) => {
          reactFlowRef.current = instance;
        }}
        fitView
        fitViewOptions={{ padding: 0.1 }}
        panOnDrag
        zoomOnScroll
        preventScrolling={false}
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable={false}
        attributionPosition="bottom-left"
      >
        <Controls showInteractive={false} />
      </ReactFlow>
    </div>
  );
}

LineageFlow.propTypes = {
  modelName: PropTypes.string.isRequired,
  isOpen: PropTypes.bool.isRequired,
};

export { LineageFlow };
