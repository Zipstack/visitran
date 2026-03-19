import { useEffect, useState } from "react";
import PropTypes from "prop-types";
import axios from "axios";
import { Spin, Empty, Typography, Collapse } from "antd";
import {
  DatabaseOutlined,
  TableOutlined,
  CopyOutlined,
} from "@ant-design/icons";

import {
  FilterLightIcon,
  FilterDarkIcon,
  SortLightIcon,
  SortDarkIcon,
  AddColumnRightLightIcon,
  AddColumnRightDarkIcon,
  OrganiserLightIcon,
  OrganiserDarkIcon,
  FindReplaceLightIcon,
  FindReplaceDarkIcon,
  CombineColumnsLightIcon,
  CombineColumnsDarkIcon,
  AggregateLightIcon,
  AggregateDarkIcon,
  JoinLightIcon,
  JoinDarkIcon,
  MergeLightIcon,
  MergeDarkIcon,
  PivotLightIcon,
  PivotDarkIcon,
} from "../../base/icons/index.js";
import { useProjectStore } from "../../store/project-store";
import { useUserStore } from "../../store/user-store";
import { orgStore } from "../../store/org-store";
import { getActiveModelName } from "../../common/helpers";
import { THEME } from "../../common/constants.js";
import "./SequenceDrawer.css";

const { Title, Text } = Typography;
const { Panel } = Collapse;

// Helper function to extract transform type from transform ID
const getTransformType = (transformId) => {
  // Transform IDs are in format: "type_uuid" (e.g., "filter_abc-123")
  // or "type_type_index" (e.g., "synthesize_synthesize_1")
  const parts = transformId.split("_");

  // Handle common transform types - order matters! Check longer strings first
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

  // Default: return first part
  return parts[0];
};

const getTransformIcon = (key, isDarkTheme) => {
  const transformType = getTransformType(key);
  const iconStyle = { width: "24px", height: "24px" };
  const iconMap = {
    filters: isDarkTheme ? (
      <FilterDarkIcon style={iconStyle} />
    ) : (
      <FilterLightIcon style={iconStyle} />
    ),
    joins: isDarkTheme ? (
      <JoinDarkIcon style={iconStyle} />
    ) : (
      <JoinLightIcon style={iconStyle} />
    ),
    groups: isDarkTheme ? (
      <AggregateDarkIcon style={iconStyle} />
    ) : (
      <AggregateLightIcon style={iconStyle} />
    ),
    groups_and_aggregation: isDarkTheme ? (
      <AggregateDarkIcon style={iconStyle} />
    ) : (
      <AggregateLightIcon style={iconStyle} />
    ),
    havings: isDarkTheme ? (
      <AggregateDarkIcon style={iconStyle} />
    ) : (
      <AggregateLightIcon style={iconStyle} />
    ),
    aggregate: isDarkTheme ? (
      <AggregateDarkIcon style={iconStyle} />
    ) : (
      <AggregateLightIcon style={iconStyle} />
    ),
    aggregate_filter: isDarkTheme ? (
      <AggregateDarkIcon style={iconStyle} />
    ) : (
      <AggregateLightIcon style={iconStyle} />
    ),
    sort: isDarkTheme ? (
      <SortDarkIcon style={iconStyle} />
    ) : (
      <SortLightIcon style={iconStyle} />
    ),
    sort_fields: isDarkTheme ? (
      <SortDarkIcon style={iconStyle} />
    ) : (
      <SortLightIcon style={iconStyle} />
    ),
    synthesize: isDarkTheme ? (
      <AddColumnRightDarkIcon style={iconStyle} />
    ) : (
      <AddColumnRightLightIcon style={iconStyle} />
    ),
    synthesize_column: isDarkTheme ? (
      <AddColumnRightDarkIcon style={iconStyle} />
    ) : (
      <AddColumnRightLightIcon style={iconStyle} />
    ),
    unions: isDarkTheme ? (
      <MergeDarkIcon style={iconStyle} />
    ) : (
      <MergeLightIcon style={iconStyle} />
    ),
    hidden_columns: isDarkTheme ? (
      <OrganiserDarkIcon style={iconStyle} />
    ) : (
      <OrganiserLightIcon style={iconStyle} />
    ),
    distinct: <CopyOutlined style={{ fontSize: "24px" }} />,
    pivot: isDarkTheme ? (
      <PivotDarkIcon style={iconStyle} />
    ) : (
      <PivotLightIcon style={iconStyle} />
    ),
    unpivot: isDarkTheme ? (
      <PivotDarkIcon style={iconStyle} />
    ) : (
      <PivotLightIcon style={iconStyle} />
    ),
    combine_columns: isDarkTheme ? (
      <CombineColumnsDarkIcon style={iconStyle} />
    ) : (
      <CombineColumnsLightIcon style={iconStyle} />
    ),
    find_and_replace: isDarkTheme ? (
      <FindReplaceDarkIcon style={iconStyle} />
    ) : (
      <FindReplaceLightIcon style={iconStyle} />
    ),
    rename: isDarkTheme ? (
      <FindReplaceDarkIcon style={iconStyle} />
    ) : (
      <FindReplaceLightIcon style={iconStyle} />
    ),
  };
  return (
    iconMap[transformType] || <CopyOutlined style={{ fontSize: "24px" }} />
  );
};

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

const getStepDescription = (key) => {
  const descriptions = {
    filters: "Apply conditions to filter rows based on criteria",
    joins: "Combine data from multiple tables using relationships",
    groups: "Group rows and calculate aggregate functions",
    havings: "Filter grouped data based on aggregate conditions",
    sort: "Order rows by specified columns",
    sort_fields: "Order rows by specified columns",
    synthesize: "Create new calculated columns",
    synthesize_column: "Create new calculated columns",
    unions: "Combine results from multiple queries",
    hidden_columns: "Hide selected columns from the output",
    distinct: "Remove duplicate rows from the dataset",
    aggregate: "Calculate summary statistics",
    aggregate_filter: "Apply filters on aggregated data",
    pivot: "Rotate rows to columns for analysis",
    unpivot: "Convert columns back to rows",
    combine_columns: "Merge multiple columns into one",
    find_and_replace: "Replace values in columns",
    rename: "Change column names",
  };
  return descriptions[key] || "Transform and process your data";
};

const renderTransformationDetails = (details) => {
  if (!details) return null;

  const { type } = details;

  switch (type) {
    case "synthesize":
    case "synthesize_column":
      return (
        <div className="transform-details dev-mode">
          <div className="code-block">
            {details.columns &&
              details.columns.map((col, idx) => (
                <div key={idx} className="code-line">
                  <span className="line-bullet">•</span>
                  <span className="code-content">
                    <span className="code-var">{col.name}</span>
                    <span className="code-operator"> = </span>
                    <span className="code-value">{col.formula}</span>
                  </span>
                </div>
              ))}
          </div>
        </div>
      );

    case "sort_fields":
    case "sort":
      return (
        <div className="transform-details dev-mode">
          <div className="code-block">
            {details.columns &&
              details.columns.map((field, idx) => (
                <div key={idx} className="code-line">
                  <span className="line-bullet">•</span>
                  <span className="code-content">
                    <span className="code-var">{field.column}</span>
                    <span
                      className={`code-sort ${
                        field.order_by === "ASC" ? "ascending" : "descending"
                      }`}
                    >
                      {field.order_by === "ASC"
                        ? " ↑ Ascending"
                        : " ↓ Descending"}
                    </span>
                  </span>
                </div>
              ))}
          </div>
        </div>
      );

    case "rename":
    case "rename_column":
      return (
        <div className="transform-details dev-mode">
          <div className="code-block">
            {details.mappings &&
              details.mappings.map((mapping, idx) => (
                <div key={idx} className="code-line">
                  <span className="line-bullet">•</span>
                  <span className="code-content">
                    <span className="code-old">{mapping.old_name}</span>
                    <span className="code-operator"> → </span>
                    <span className="code-new">{mapping.new_name}</span>
                  </span>
                </div>
              ))}
          </div>
        </div>
      );

    case "filter":
    case "filters":
      return (
        <div className="transform-details dev-mode">
          <div className="code-section">
            <div className="code-section-title">Filters Applied:</div>
            <div className="code-block">
              {details.conditions &&
                details.conditions.map((cond, idx) => (
                  <div key={idx} className="code-line">
                    <span className="line-bullet">•</span>
                    <span className="code-content">
                      <span className="code-value">
                        {cond.column} {cond.operator} {cond.value}
                      </span>
                    </span>
                  </div>
                ))}
            </div>
          </div>
        </div>
      );

    case "join":
    case "joins":
      return (
        <div className="transform-details dev-mode">
          {details.joins && details.joins.length > 0 ? (
            // Multiple joins display
            <div className="code-section">
              <div className="code-section-title">Joins:</div>
              <div className="code-block">
                {details.joins.map((join, idx) => (
                  <div key={idx} className="code-line">
                    <span className="line-bullet">•</span>
                    <span className="code-content">
                      <span className="code-keyword">
                        {join.join_type?.toUpperCase() || "INNER"}
                      </span>
                      <span className="code-operator">: </span>
                      <span className="code-var">{join.left_table}</span>
                      <span className="code-operator"> ⟗ </span>
                      <span className="code-var">{join.right_table}</span>
                      {join.on && (
                        <>
                          <span className="code-operator"> on </span>
                          <span className="code-value">{join.on}</span>
                        </>
                      )}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          ) : (
            // Single join display (legacy format)
            <>
              <div className="code-section">
                <div className="code-section-title">
                  {details.join_type && details.join_type.toUpperCase()} Join:
                </div>
                <div className="code-block">
                  <div className="code-line">
                    <span className="line-bullet">•</span>
                    <span className="code-content">
                      <span className="code-var">{details.left_table}</span>
                      <span className="code-operator"> ⟗ </span>
                      <span className="code-var">{details.right_table}</span>
                    </span>
                  </div>
                </div>
              </div>
              {details.on && (
                <div className="code-section">
                  <div className="code-section-title">On:</div>
                  <div className="code-block">
                    <div className="code-line">
                      <span className="line-bullet">•</span>
                      <span className="code-content">
                        <span className="code-value">{details.on}</span>
                      </span>
                    </div>
                  </div>
                </div>
              )}
            </>
          )}
        </div>
      );

    case "groups":
    case "aggregate":
    case "groups_and_aggregation":
      return (
        <div className="transform-details dev-mode">
          {details.group_by && details.group_by.length > 0 && (
            <div className="code-section">
              <div className="code-section-title">
                Group By:{" "}
                <span className="code-inline">
                  {details.group_by.join(", ")}
                </span>
              </div>
            </div>
          )}

          {details.aggregations && details.aggregations.length > 0 && (
            <div className="code-section">
              <div className="code-section-title">Aggregations:</div>
              <div className="code-block">
                {details.aggregations.map((agg, idx) => (
                  <div key={idx} className="code-line">
                    <span className="line-bullet">•</span>
                    <span className="code-content">
                      <span className="code-function">{agg.function}</span>
                      <span className="code-operator">(</span>
                      <span className="code-var">{agg.column}</span>
                      <span className="code-operator">)</span>
                      {agg.alias && (
                        <>
                          <span className="code-operator"> → </span>
                          <span className="code-new">{agg.alias}</span>
                        </>
                      )}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {details.having && details.having.length > 0 && (
            <div className="code-section">
              <div className="code-section-title">Having:</div>
              <div className="code-block">
                {details.having.map((condition, idx) => (
                  <div key={idx} className="code-line">
                    <span className="line-bullet">•</span>
                    <span className="code-content">
                      <span className="code-value">{condition}</span>
                    </span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {details.where && details.where.length > 0 && (
            <div className="code-section">
              <div className="code-section-title">Where:</div>
              <div className="code-block">
                {details.where.map((condition, idx) => (
                  <div key={idx} className="code-line">
                    <span className="line-bullet">•</span>
                    <span className="code-content">
                      <span className="code-value">{condition}</span>
                    </span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      );

    case "distinct":
      return (
        <div className="transform-details dev-mode">
          <div className="code-block">
            <div className="code-line">
              <span className="line-bullet">•</span>
              <span className="code-content">
                {details.columns && details.columns.length > 0 ? (
                  <>
                    <span className="code-operator">
                      Remove duplicates based on:{" "}
                    </span>
                    <span className="code-var">
                      {details.columns.join(", ")}
                    </span>
                  </>
                ) : (
                  <span className="code-value">
                    Remove duplicate rows from dataset
                  </span>
                )}
              </span>
            </div>
          </div>
        </div>
      );

    case "pivot":
      return (
        <div className="transform-details">
          <div className="detail-summary">
            <span className="detail-label">Pivot transformation</span>
          </div>
          <div className="detail-items">
            <div className="detail-item">
              <span className="detail-label-inline">Pivot Column:</span>
              <span className="column-name">{details.pivot_column}</span>
            </div>
            <div className="detail-item">
              <span className="detail-label-inline">Value Column:</span>
              <span className="column-name">{details.value_column}</span>
            </div>
            {details.index_columns && details.index_columns.length > 0 && (
              <div className="detail-item">
                <span className="detail-label-inline">Index Columns:</span>
                <div className="column-list">
                  {details.index_columns.map((col, idx) => (
                    <span key={idx} className="column-badge">
                      {col}
                    </span>
                  ))}
                </div>
              </div>
            )}
          </div>
        </div>
      );

    case "unpivot":
      return (
        <div className="transform-details">
          <div className="detail-summary">
            <span className="detail-label">Unpivot transformation</span>
          </div>
          <div className="detail-items">
            <div className="detail-item">
              <span className="detail-label-inline">Name Column:</span>
              <span className="column-name">{details.name_column}</span>
            </div>
            <div className="detail-item">
              <span className="detail-label-inline">Value Column:</span>
              <span className="column-name">{details.value_column}</span>
            </div>
            {details.columns && details.columns.length > 0 && (
              <div className="detail-item">
                <span className="detail-label-inline">Columns:</span>
                <div className="column-list">
                  {details.columns.map((col, idx) => (
                    <span key={idx} className="column-badge">
                      {col}
                    </span>
                  ))}
                </div>
              </div>
            )}
          </div>
        </div>
      );

    case "find_and_replace":
      return (
        <div className="transform-details">
          <div className="detail-items">
            {details.replacements &&
              details.replacements.map((r, idx) => (
                <div key={idx} className="replace-item">
                  <div className="column-name">{r.column}</div>
                  <div className="replace-expression">
                    <span className="old-value">&quot;{r.find}&quot;</span>
                    <span className="arrow">→</span>
                    <span className="new-value">&quot;{r.replace}&quot;</span>
                  </div>
                </div>
              ))}
          </div>
        </div>
      );

    case "combine_columns":
      return (
        <div className="transform-details dev-mode">
          <div className="code-section">
            <div className="code-section-title">Combinations:</div>
            <div className="code-block">
              {details.combinations && details.combinations.length > 0 ? (
                // New format: multiple combinations
                details.combinations.map((combo, idx) => (
                  <div key={idx} className="code-line">
                    <span className="line-bullet">•</span>
                    <span className="code-content">
                      <span className="code-var">{combo.target_column}</span>
                      <span className="code-operator"> = </span>
                      <span className="code-value">
                        {combo.source_columns.join(
                          ` ${combo.separator || ""} `
                        )}
                      </span>
                    </span>
                  </div>
                ))
              ) : (
                // Legacy format: single combination with direct properties
                <div className="code-line">
                  <span className="line-bullet">•</span>
                  <span className="code-content">
                    <span className="code-var">{details.target_column}</span>
                    <span className="code-operator"> = </span>
                    <span className="code-value">
                      {details.source_columns &&
                        details.source_columns.join(
                          ` ${details.separator || ""} `
                        )}
                    </span>
                  </span>
                </div>
              )}
            </div>
          </div>
        </div>
      );

    case "hidden_columns":
      return (
        <div className="transform-details">
          <div className="detail-items">
            <div className="column-list">
              {details.columns &&
                details.columns.map((col, idx) => (
                  <span key={idx} className="column-badge hidden">
                    {col}
                  </span>
                ))}
            </div>
          </div>
        </div>
      );

    case "union":
    case "unions":
      return (
        <div className="transform-details dev-mode">
          <div className="code-section">
            <div className="code-section-title">Merges:</div>
            <div className="code-block">
              {details.tables && details.tables.length > 0 && (
                <>
                  {details.tables.map((table, idx) => (
                    <div key={idx} className="code-line">
                      <span className="line-bullet">•</span>
                      <span className="code-content">
                        <span className="code-value">
                          {table.source_schema
                            ? `${table.source_schema}.${table.source_table}`
                            : table.source_table}
                          .{table.source_column}
                        </span>
                        <span className="code-operator"> → </span>
                        <span className="code-value">
                          {table.merge_schema
                            ? `${table.merge_schema}.${table.merge_table}`
                            : table.merge_table}
                          .{table.merge_column}
                        </span>
                        {details.ignore_duplicate !== undefined && (
                          <>
                            <span className="code-operator"> [</span>
                            <span className="code-keyword">
                              {details.ignore_duplicate
                                ? "Ignore Duplicates"
                                : "Keep Duplicates"}
                            </span>
                            <span className="code-operator">]</span>
                          </>
                        )}
                      </span>
                    </div>
                  ))}
                </>
              )}
            </div>
          </div>
        </div>
      );

    default:
      return (
        <div className="transform-details">
          <div className="detail-summary">
            <span className="detail-label secondary">
              {details.description || "Configuration details"}
            </span>
          </div>
        </div>
      );
  }
};

function Body({ isSequenceDrawerOpen, modelName: modelNameProp }) {
  const [loading, setLoading] = useState(false);
  const [sequenceData, setSequenceData] = useState([]);
  const [error, setError] = useState(null);
  const { projectId, projectDetails } = useProjectStore.getState();
  const modelName =
    modelNameProp || getActiveModelName(projectId, projectDetails);
  const selectedOrgId = orgStore((state) => state.selectedOrgId);
  const userDetails = useUserStore((state) => state.userDetails);
  const isDarkTheme = userDetails?.currentTheme === THEME.DARK;

  useEffect(() => {
    if (!isSequenceDrawerOpen || !modelName) return;

    const fetchSequence = async () => {
      setLoading(true);
      setError(null);
      const apiUrl = `/api/v1/visitran/${
        selectedOrgId || "default_org"
      }/project/${projectId}/reload?file_name=${modelName}`;
      try {
        const response = await axios.get(apiUrl);
        const sequence_orders = response.data.sequence_orders;
        const model_data = response.data.model_data || {};
        const transformation_details =
          response.data.transformation_details || {};

        if (!sequence_orders || Object.keys(sequence_orders).length === 0) {
          setSequenceData([]);
          setLoading(false);
          return;
        }

        const sequenceArray = Object.entries(sequence_orders)
          .filter(
            ([key, order]) => order !== null && order !== undefined && order > 0
          )
          .sort(([, a], [, b]) => a - b)
          .map(([key, order]) => {
            const transformType = getTransformType(key);
            let details = transformation_details[key] || null;

            // Special handling for combine_columns - extract from model_data.transform if not in transformation_details
            if (
              transformType === "combine_columns" &&
              !details &&
              model_data?.transform
            ) {
              // Find the combine_columns config in model_data.transform by matching the type
              for (const [, configData] of Object.entries(
                model_data.transform
              )) {
                if (
                  configData?.type === "combine_columns" &&
                  configData?.combine_columns
                ) {
                  const combineConfig = configData.combine_columns;
                  const columns = combineConfig.columns || [];

                  if (columns.length > 0) {
                    // Process ALL columns, not just the first one
                    const allCombinations = columns.map((column) => {
                      const values = column.values || [];

                      // Extract source columns and separator for this combination
                      const source_columns = [];
                      let separator = "";

                      for (const value of values) {
                        if (value.type === "column") {
                          source_columns.push(value.value);
                        } else if (value.type === "value") {
                          separator = value.value;
                        }
                      }

                      return {
                        target_column: column.columnName,
                        source_columns: source_columns,
                        separator: separator,
                      };
                    });

                    details = {
                      type: "combine_columns",
                      count: allCombinations.length,
                      combinations: allCombinations,
                    };
                  }
                  break;
                }
              }
            }

            // Special handling for unions (Merge) - extract from model_data.transform if not in transformation_details
            if (
              transformType === "unions" &&
              !details &&
              model_data?.transform
            ) {
              // Find the union config in model_data.transform by matching the type
              for (const [, configData] of Object.entries(
                model_data.transform
              )) {
                if (configData?.type === "union" && configData?.union) {
                  const unionConfig = configData.union;
                  const tables = unionConfig.tables || [];

                  if (tables.length > 0) {
                    const mergeInfo = tables.map((table) => ({
                      merge_table: table.merge_table,
                      merge_schema: table.merge_schema,
                      merge_column: table.merge_column,
                      source_table: table.source_table,
                      source_schema: table.source_schema,
                      source_column: table.source_column,
                    }));

                    details = {
                      type: "union",
                      count: tables.length,
                      tables: mergeInfo,
                      ignore_duplicate: unionConfig.ignore_duplicate,
                    };
                  }
                  break;
                }
              }
            }

            // Special handling for joins - extract ALL joins from model_data.transform
            if (transformType === "joins" && model_data?.transform) {
              // Find the join config in model_data.transform
              for (const [, configData] of Object.entries(
                model_data.transform
              )) {
                if (configData?.type === "join" && configData?.join) {
                  const joinConfig = configData.join;
                  const tables = joinConfig.tables || [];

                  if (tables.length > 0) {
                    // Extract all join information
                    const allJoins = tables.map((table, idx) => {
                      const criteria = table.criteria || [];
                      const condition = criteria[0]?.condition;

                      // Determine left and right tables
                      let leftTable = "";
                      let rightTable = "";
                      let onCondition = "";

                      if (condition) {
                        const lhs = condition.lhs?.column;
                        const rhs = condition.rhs?.column;
                        const operator = condition.operator || "=";

                        if (lhs && rhs) {
                          leftTable = lhs.schema_name
                            ? `${lhs.schema_name}.${lhs.table_name}`
                            : lhs.table_name;
                          rightTable = rhs.schema_name
                            ? `${rhs.schema_name}.${rhs.table_name}`
                            : rhs.table_name;
                          onCondition = `${leftTable}.${lhs.column_name} ${operator} ${rightTable}.${rhs.column_name}`;
                        }
                      }

                      return {
                        join_type: table.type || "Inner",
                        left_table:
                          leftTable ||
                          (idx === 0
                            ? model_data?.source?.table_name
                            : tables[idx - 1]?.joined_table?.table_name),
                        right_table:
                          rightTable || table.joined_table?.table_name,
                        on: onCondition,
                      };
                    });

                    details = {
                      type: "joins",
                      count: allJoins.length,
                      joins: allJoins,
                    };
                  }
                  break;
                }
              }
            }

            return {
              key,
              order,
              type: "transformation",
              label:
                TRANSFORM_LABELS[transformType] ||
                key.replace(/_/g, " ").replace(/\b\w/g, (l) => l.toUpperCase()),
              icon: getTransformIcon(key, isDarkTheme),
              details: details,
            };
          });

        // Create SOURCE item
        const sourceItem = {
          key: "source",
          type: "source",
          label: "SOURCE",
          tableName: model_data?.source?.table_name,
          schemaName: model_data?.source?.schema_name,
          description: "Raw data from your data warehouse",
        };

        // Create OUTPUT item
        const outputItem = {
          key: "output",
          type: "output",
          label: "OUTPUT",
          tableName: model_data?.model?.table_name,
          schemaName: model_data?.model?.schema_name,
          description: "Transformed data ready for analysis",
        };

        // Combine: SOURCE + transformations + OUTPUT
        const fullSequence = [sourceItem, ...sequenceArray, outputItem];
        setSequenceData(fullSequence);
      } catch (err) {
        setError(
          err.response?.data?.message ||
            err.response?.data?.error ||
            err.message ||
            "Failed to load transformation sequence"
        );
        setSequenceData([]);
      } finally {
        setLoading(false);
      }
    };

    fetchSequence();
  }, [isSequenceDrawerOpen, modelName, projectId]);

  if (loading) {
    return (
      <div style={{ textAlign: "center", padding: "50px" }}>
        <Spin size="large" />
        <Text style={{ display: "block", marginTop: "16px" }}>
          Loading transformation sequence...
        </Text>
      </div>
    );
  }

  if (error) {
    return (
      <div style={{ padding: "24px", textAlign: "center" }}>
        <Empty
          description={
            <span>
              <Text type="danger">{error}</Text>
            </span>
          }
        />
      </div>
    );
  }

  if (!sequenceData.length) {
    return (
      <div style={{ padding: "24px" }}>
        <Empty
          description="No transformations found. Apply transformations to see the execution sequence."
          style={{ marginTop: "50px" }}
        />
      </div>
    );
  }

  return (
    <div style={{ padding: "16px", height: "100%", overflowY: "auto" }}>
      <div style={{ marginBottom: "16px" }}>
        <Title level={5} style={{ margin: 0 }}>
          Execution Plan (
          {sequenceData.filter((item) => item.type === "transformation").length}{" "}
          {sequenceData.filter((item) => item.type === "transformation")
            .length === 1
            ? "step"
            : "steps"}
          )
        </Title>
      </div>

      <div className="custom-timeline">
        {sequenceData.map((item, index) => (
          <div key={item.key} className="timeline-item">
            {/* SOURCE Item */}
            {item.type === "source" && (
              <>
                <div className="timeline-left">
                  <div className="timeline-icon source-icon">
                    <DatabaseOutlined />
                  </div>
                  <div className="timeline-line" />
                </div>
                <div className="timeline-right">
                  <div className="timeline-badge source-badge">SOURCE</div>
                  <div className="timeline-description">
                    <Text type="secondary">→ {item.description}</Text>
                  </div>
                  {item.tableName && (
                    <div className="timeline-table-name">
                      <Text strong>
                        {item.schemaName
                          ? `${item.schemaName}.${item.tableName}`
                          : item.tableName}
                      </Text>
                    </div>
                  )}
                </div>
              </>
            )}

            {/* OUTPUT Item */}
            {item.type === "output" && (
              <>
                <div className="timeline-left">
                  <div className="timeline-icon output-icon">
                    <TableOutlined />
                  </div>
                </div>
                <div className="timeline-right">
                  <div className="timeline-badge output-badge">OUTPUT</div>
                  <div className="timeline-description">
                    <Text type="secondary">→ {item.description}</Text>
                  </div>
                  {item.tableName && (
                    <div className="timeline-table-name">
                      <Text strong>
                        {item.schemaName
                          ? `${item.schemaName}.${item.tableName}`
                          : item.tableName}
                      </Text>
                    </div>
                  )}
                </div>
              </>
            )}

            {/* TRANSFORMATION Item */}
            {item.type === "transformation" && (
              <>
                <div className="timeline-left">
                  <div className="timeline-icon transform-icon">
                    {item.icon}
                  </div>
                  <div className="timeline-line" />
                </div>
                <div className="timeline-right">
                  {item.details ? (
                    <Collapse
                      bordered={false}
                      ghost
                      className="sequence-collapse"
                      expandIconPosition="end"
                    >
                      <Panel
                        header={
                          <div className="timeline-header-wrapper">
                            <div className="timeline-badge step-badge">
                              {item.label}
                            </div>
                          </div>
                        }
                        key={item.key}
                      >
                        <div className="timeline-details">
                          {renderTransformationDetails(item.details)}
                        </div>
                      </Panel>
                    </Collapse>
                  ) : (
                    <>
                      <div className="timeline-badge step-badge">
                        {item.label}
                      </div>
                      <div className="timeline-description">
                        <Text type="secondary">
                          {getStepDescription(item.key)}
                        </Text>
                      </div>
                    </>
                  )}
                </div>
              </>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}

Body.propTypes = {
  isSequenceDrawerOpen: PropTypes.bool.isRequired,
  modelName: PropTypes.string,
};

export { Body };
