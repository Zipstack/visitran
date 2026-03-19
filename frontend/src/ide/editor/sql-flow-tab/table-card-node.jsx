import { memo } from "react";
import PropTypes from "prop-types";
import { Handle, Position } from "reactflow";
import { Tooltip } from "antd";
import { CodeOutlined } from "@ant-design/icons";

import { LinearScale } from "../../../base/icons";
import "./sql-flow-tab.css";

// Border colors by table type (matching reference design)
const borderColors = {
  source: "#8B5CF6", // Purple - source tables
  model: "#3B82F6", // Blue - intermediate models
  terminal: "#10B981", // Green - final outputs
};

/**
 * TableCardNode - Clean table card for lineage visualization.
 *
 * Displays:
 * - Schema badge (if available)
 * - Table name with colored top border indicating type
 * - SQL icon and lineage icon
 * - All columns in a simple list
 * - Connection handles for data flow edges
 */
const TableCardNode = memo(({ data, isConnectable }) => {
  const {
    label,
    schema,
    columns = [],
    tableType,
    modelName,
    isSelected,
    onSqlClick,
    onLineageClick,
    sql,
    isDarkTheme,
  } = data;
  const borderColor = borderColors[tableType] || borderColors.model;

  const handleSqlClick = (e) => {
    e.stopPropagation(); // Prevent node selection
    if (onSqlClick) {
      onSqlClick({ label, schema, columns, tableType, modelName, sql });
    }
  };

  const handleLineageClick = (e) => {
    e.stopPropagation(); // Prevent node selection
    if (onLineageClick) {
      onLineageClick({ label, schema, columns, tableType, modelName, sql });
    }
  };

  return (
    <div
      className={`table-card-node ${isSelected ? "selected" : ""} ${
        isDarkTheme ? "dark-theme" : ""
      }`}
      style={{ borderTopColor: borderColor }}
    >
      {/* Table Header: Schema → Name → SQL icon → Lineage icon */}
      <div className="table-card-header">
        {schema && <span className="table-schema">{schema}</span>}
        <div className="table-card-title">
          <span className="table-name">{label}</span>
        </div>
        <div className="table-card-actions">
          <Tooltip title="View SQL">
            <button className="sql-btn" onClick={handleSqlClick}>
              <CodeOutlined />
            </button>
          </Tooltip>
          <Tooltip title="View Sequence">
            <button
              className="sql-btn lineage-btn"
              onClick={handleLineageClick}
            >
              <LinearScale style={{ width: 14, height: 14 }} />
            </button>
          </Tooltip>
        </div>
      </div>

      {/* Column List */}
      <div className="table-card-columns">
        {columns?.length > 0 ? (
          columns.map((col) => (
            <div key={col?.name} className="table-column">
              <span className="column-name">{col?.name}</span>
              {col?.type && <span className="column-type">{col.type}</span>}
            </div>
          ))
        ) : (
          <div className="table-column no-columns">
            <span className="column-name muted">No columns</span>
          </div>
        )}
      </div>

      {/* Connection handles */}
      <Handle
        type="source"
        position={Position.Right}
        id="default"
        className="flow-handle"
        isConnectable={isConnectable}
      />
      <Handle
        type="target"
        position={Position.Left}
        id="default"
        className="flow-handle"
        isConnectable={isConnectable}
      />
    </div>
  );
});

TableCardNode.displayName = "TableCardNode";

TableCardNode.propTypes = {
  data: PropTypes.shape({
    label: PropTypes.string,
    schema: PropTypes.string,
    columns: PropTypes.arrayOf(
      PropTypes.shape({
        name: PropTypes.string,
        type: PropTypes.string,
      })
    ),
    tableType: PropTypes.string,
    modelName: PropTypes.string,
    isSelected: PropTypes.bool,
    onSqlClick: PropTypes.func,
    onLineageClick: PropTypes.func,
    sql: PropTypes.string,
    isDarkTheme: PropTypes.bool,
  }).isRequired,
  isConnectable: PropTypes.bool,
};

export default TableCardNode;
