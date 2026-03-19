import { useState, useCallback, useEffect } from "react";
import ReactFlow, {
  Background,
  MiniMap,
  useNodesState,
  useEdgesState,
  addEdge,
  Panel,
  MarkerType,
} from "reactflow";
import "reactflow/dist/style.css";
import {
  Spin,
  Empty,
  Alert,
  Typography,
  Button,
  Tooltip,
  Switch,
  Space,
  Select,
} from "antd";
import {
  ReloadOutlined,
  FullscreenOutlined,
  ZoomInOutlined,
  ZoomOutOutlined,
} from "@ant-design/icons";

import DatabaseSchemaNode from "./components/database-schema-node";
import { useDbSchemaService } from "./db-schema-service.js";
import "./db-schema-visualization.css";

const { Text, Title } = Typography;
const { Option } = Select;

// Define custom node types
const nodeTypes = {
  databaseSchema: DatabaseSchemaNode,
};

// Edge styles with animated markers
const defaultEdgeOptions = {
  animated: true,
  style: { stroke: "var(--ant-color-primary)", strokeWidth: 2 },
  type: "smoothstep",
  markerEnd: {
    type: MarkerType.ArrowClosed,
    color: "var(--ant-color-primary)",
    width: 20,
    height: 20,
  },
};

const DbSchemaVisualization = () => {
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [showMinimap, setShowMinimap] = useState(false);
  const [reactFlowInstance, setReactFlowInstance] = useState(null);
  // All relationships are shown by default
  const [schemaFilter, setSchemaFilter] = useState([]);
  const [availableSchemas, setAvailableSchemas] = useState([]);
  // const [relationshipStats, setRelationshipStats] = useState({
  //   total: 0,
  //   high: 0,
  //   medium: 0,
  //   low: 0,
  // });

  const { fetchDbSchema, transformDbExplorerToFlowData } = useDbSchemaService();

  // Fetch database schema data on component mount
  useEffect(() => {
    loadDbSchema();
  }, []);

  // We're now handling filter changes directly in the handleSchemaChange function
  // No need for a separate effect to watch for filter changes

  // Function to load database schema data
  const loadDbSchema = async () => {
    try {
      setLoading(true);
      const dbSchemaData = await fetchDbSchema();
      const { nodes: schemaNodes, edges: schemaEdges } =
        transformDbExplorerToFlowData(dbSchemaData);

      if (schemaNodes.length === 0) {
        // No schema data available — empty state will render
        setNodes([]);
        setEdges([]);
        // setRelationshipStats({
        //   total: 0,
        //   high: 0,
        //   medium: 0,
        //   low: 0,
        // });
      } else {
        // Extract available schemas for filtering
        const schemas = [
          ...new Set(schemaNodes.map((node) => node.data.schemaName)),
        ];
        setAvailableSchemas(schemas);
        setSchemaFilter(schemas); // Initially show all schemas

        // Calculate relationship statistics
        // const stats = {
        //   total: schemaEdges.length,
        //   high: schemaEdges.filter(
        //     (edge) => edge.data?.relationship?.confidence === "high"
        //   ).length,
        //   medium: schemaEdges.filter(
        //     (edge) => edge.data?.relationship?.confidence === "medium"
        //   ).length,
        //   low: schemaEdges.filter(
        //     (edge) =>
        //       !edge.data?.relationship?.confidence ||
        //       edge.data?.relationship?.confidence === "low"
        //   ).length,
        // };
        // setRelationshipStats(stats);

        setNodes(schemaNodes);
        setEdges(
          schemaEdges.map((edge) => ({
            ...edge,
            ...defaultEdgeOptions,
          }))
        );
      }
      setError(null);
    } catch (err) {
      console.error("Failed to load database schema:", err);
      setError("Failed to load database schema. Please try again later.");
      setNodes([]);
      setEdges([]);
      // setRelationshipStats({
      //   total: 0,
      //   high: 0,
      //   medium: 0,
      //   low: 0,
      // });
    } finally {
      setLoading(false);
    }
  };

  // Apply filters to the nodes and edges - used for initial load and refresh
  // const applyFilters = useCallback(async () => {
  //   try {
  //     setLoading(true);

  //     // Re-fetch the data to get the original set
  //     const dbSchemaData = await fetchDbSchema();
  //     const { nodes: allNodes, edges: allEdges } =
  //       transformDbExplorerToFlowData(dbSchemaData);

  //     // Get current schema filter
  //     const currentFilter = [...schemaFilter];

  //     // Filter nodes by schema
  //     const filteredNodes =
  //       currentFilter.length > 0
  //         ? allNodes.filter((node) =>
  //             currentFilter.includes(node.data.schemaName)
  //           )
  //         : allNodes; // If no schemas selected, show all nodes

  //     // Update nodes and edges
  //     setNodes(filteredNodes);
  //     setEdges([]);

  //     // If we have the flow instance, fit the view to show all nodes
  //     if (reactFlowInstance) {
  //       setTimeout(() => {
  //         reactFlowInstance.fitView({ padding: 0.2 });
  //       }, 100);
  //     }
  //   } catch (err) {
  //     console.error("Error in applyFilters:", err);
  //   } finally {
  //     setLoading(false);
  //   }
  // }, [
  //   schemaFilter,
  //   reactFlowInstance,
  //   fetchDbSchema,
  //   transformDbExplorerToFlowData,
  // ]);

  // Handle connection between nodes
  const onConnect = useCallback(
    (params) =>
      setEdges((eds) => addEdge({ ...params, ...defaultEdgeOptions }, eds)),
    [setEdges]
  );

  // Handle zoom to fit
  const onZoomToFit = useCallback(() => {
    if (reactFlowInstance) {
      reactFlowInstance.fitView({ padding: 0.2 });
    }
  }, [reactFlowInstance]);

  // Handle zoom in/out
  const onZoomIn = useCallback(() => {
    if (reactFlowInstance) {
      reactFlowInstance.zoomIn();
    }
  }, [reactFlowInstance]);

  const onZoomOut = useCallback(() => {
    if (reactFlowInstance) {
      reactFlowInstance.zoomOut();
    }
  }, [reactFlowInstance]);

  // Toggle minimap
  const toggleMinimap = useCallback(() => {
    setShowMinimap(!showMinimap);
  }, [showMinimap]);

  // No confidence filter

  // Handle schema filter change
  const handleSchemaChange = (values) => {
    // Store the new values in a variable to ensure we're using the latest values
    const newSchemaFilter = [...values];

    // Update state with the new filter values
    setSchemaFilter(newSchemaFilter);

    // Use the actual values directly in the filter function instead of relying on state
    const applyNewFilters = async () => {
      try {
        setLoading(true);

        // Re-fetch the data to get the original set
        const dbSchemaData = await fetchDbSchema();
        const { nodes: allNodes } = transformDbExplorerToFlowData(dbSchemaData);

        // Filter nodes by schema using the new filter values directly
        const filteredNodes =
          newSchemaFilter.length > 0
            ? allNodes.filter((node) =>
                newSchemaFilter.includes(node.data.schemaName)
              )
            : allNodes; // If no schemas selected, show all nodes

        // Get IDs of visible nodes
        // const visibleNodeIds = new Set(filteredNodes.map((node) => node.id));

        // No edges to filter
        const filteredEdges = [];

        // Update nodes and edges
        setNodes(filteredNodes);
        setEdges(
          filteredEdges.map((edge) => ({
            ...edge,
            ...defaultEdgeOptions,
          }))
        );

        // If we have the flow instance, fit the view to show all nodes
        if (reactFlowInstance) {
          setTimeout(() => {
            reactFlowInstance.fitView({ padding: 0.2 });
          }, 100);
        }
      } catch (err) {
        console.error("Error applying filters:", err);
      } finally {
        setLoading(false);
      }
    };

    // Execute the filter function
    applyNewFilters();
  };

  if (loading && nodes.length === 0) {
    return (
      <div className="db-schema-loading">
        <Spin size="large" tip="Loading database schema..." />
      </div>
    );
  }

  if (error) {
    return (
      <div className="db-schema-error">
        <Alert message="Error" description={error} type="error" showIcon />
      </div>
    );
  }

  if (nodes.length === 0) {
    return (
      <div className="db-schema-empty">
        <Empty
          description="No database schema found. Please set up a connection in the Connections page."
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        />
      </div>
    );
  }

  return (
    <div className="db-schema-visualization">
      <div className="db-schema-header">
        <div className="db-schema-title">
          <Title level={5} style={{ margin: 0 }}>
            Database Schema Visualization
          </Title>
        </div>

        <div className="db-schema-filters">
          <Space>
            <Text>Schema:</Text>
            <Select
              mode="multiple"
              value={schemaFilter}
              onChange={handleSchemaChange}
              style={{ minWidth: 350, maxWidth: 600 }}
              placeholder="Filter by schema"
              maxTagCount="responsive"
              autoAdjustOverflow={true}
            >
              {availableSchemas.map((schema) => (
                <Option key={schema} value={schema}>
                  {schema}
                </Option>
              ))}
            </Select>

            <Tooltip title="Refresh Schema">
              <Button
                icon={<ReloadOutlined />}
                onClick={loadDbSchema}
                loading={loading}
              />
            </Tooltip>
          </Space>
        </div>
      </div>

      <div className="db-schema-flow-container">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          nodeTypes={nodeTypes}
          defaultEdgeOptions={defaultEdgeOptions}
          fitView
          attributionPosition="none"
          onInit={setReactFlowInstance}
          proOptions={{ hideAttribution: true }}
        >
          <Background color="transparent" />
          {showMinimap && <MiniMap nodeStrokeWidth={3} zoomable pannable />}
          <Panel position="top-right" className="db-schema-controls">
            <Space>
              <Tooltip title="Fit View">
                <Button icon={<FullscreenOutlined />} onClick={onZoomToFit} />
              </Tooltip>
              <Tooltip title="Zoom In">
                <Button icon={<ZoomInOutlined />} onClick={onZoomIn} />
              </Tooltip>
              <Tooltip title="Zoom Out">
                <Button icon={<ZoomOutOutlined />} onClick={onZoomOut} />
              </Tooltip>
              <Tooltip title="Toggle Minimap">
                <Switch
                  checked={showMinimap}
                  onChange={toggleMinimap}
                  size="small"
                  checkedChildren="Map"
                  unCheckedChildren="Map"
                />
              </Tooltip>
            </Space>
          </Panel>
          {/* Relationship legend removed */}
        </ReactFlow>
      </div>
    </div>
  );
};

export default DbSchemaVisualization;
