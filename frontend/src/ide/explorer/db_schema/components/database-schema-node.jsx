import { memo } from "react";
import PropTypes from "prop-types";
import { Typography } from "antd";
import "./database-schema-node.css";

const DatabaseSchemaNode = memo(({ data, isConnectable }) => {
  const {
    label,
    title,
    schema = [],
    solidColor = "var(--ant-color-primary)",
    color = "var(--ant-color-primary)",
  } = data;

  // Extract schema name and table name from the title or label
  const [schemaName, tableName] = (title || label).split(".");

  return (
    <div className="database-schema-node" style={{ borderColor: color }}>
      <div className="schema-header" style={{ backgroundColor: solidColor }}>
        <div className="schema-title">
          <span className="schema-name">{schemaName}</span>
          <span className="dot">.</span>
          <span className="table-name">{tableName}</span>
        </div>
      </div>

      <div className="schema-content">
        {schema.map((field, index) => (
          <div key={index} className="schema-field">
            <Typography className="field-name">{field.title}</Typography>
            <div className="field-type">
              <span className="type-label">{field.type}</span>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
});

DatabaseSchemaNode.displayName = "DatabaseSchemaNode";

DatabaseSchemaNode.propTypes = {
  data: PropTypes.shape({
    label: PropTypes.string.isRequired,
    title: PropTypes.string,
    schema: PropTypes.arrayOf(
      PropTypes.shape({
        title: PropTypes.string.isRequired,
        type: PropTypes.string,
      })
    ),
    solidColor: PropTypes.string,
    color: PropTypes.string,
  }),
  isConnectable: PropTypes.bool,
};

export default DatabaseSchemaNode;
