import { memo } from "react";
import PropTypes from "prop-types";
import { Handle, Position } from "reactflow";
import {
  DatabaseOutlined,
  TableOutlined,
  FilterOutlined,
  LinkOutlined,
  LineHeightOutlined,
  PlusSquareOutlined,
  MergeCellsOutlined,
  EyeInvisibleOutlined,
  CopyOutlined,
  ProfileOutlined,
  ContainerOutlined,
} from "@ant-design/icons";

import {
  FindReplaceLightIcon,
  FindReplaceDarkIcon,
  CombineColumnsLightIcon,
  CombineColumnsDarkIcon,
  PivotLightIcon,
  PivotDarkIcon,
} from "../../../base/icons/index.js";

// Color palette for different step types
const STEP_COLORS = {
  source: { border: "#8B5CF6", bg: "#8B5CF620", icon: "#8B5CF6" },
  output: { border: "#10B981", bg: "#10B98120", icon: "#10B981" },
  filters: { border: "#F59E0B", bg: "#F59E0B20", icon: "#F59E0B" },
  joins: { border: "#EC4899", bg: "#EC489920", icon: "#EC4899" },
  sort: { border: "#6366F1", bg: "#6366F120", icon: "#6366F1" },
  synthesize: { border: "#22C55E", bg: "#22C55E20", icon: "#22C55E" },
  groups: { border: "#3B82F6", bg: "#3B82F620", icon: "#3B82F6" },
  groups_and_aggregation: {
    border: "#3B82F6",
    bg: "#3B82F620",
    icon: "#3B82F6",
  },
  aggregate: { border: "#3B82F6", bg: "#3B82F620", icon: "#3B82F6" },
  unions: { border: "#06B6D4", bg: "#06B6D420", icon: "#06B6D4" },
  hidden_columns: { border: "#94A3B8", bg: "#94A3B820", icon: "#94A3B8" },
  distinct: { border: "#84CC16", bg: "#84CC1620", icon: "#84CC16" },
  rename: { border: "#F97316", bg: "#F9731620", icon: "#F97316" },
  find_and_replace: { border: "#A855F7", bg: "#A855F720", icon: "#A855F7" },
  combine_columns: { border: "#14B8A6", bg: "#14B8A620", icon: "#14B8A6" },
  pivot: { border: "#E11D48", bg: "#E11D4820", icon: "#E11D48" },
  unpivot: { border: "#E11D48", bg: "#E11D4820", icon: "#E11D48" },
  havings: { border: "#64748B", bg: "#64748B20", icon: "#64748B" },
};

const DEFAULT_COLOR = { border: "#94A3B8", bg: "#94A3B820", icon: "#94A3B8" };

// Get icon for step type
const getStepIcon = (stepType, isDarkTheme) => {
  const iconStyle = { fontSize: 14 };
  const svgStyle = { width: 14, height: 14 };

  switch (stepType) {
    case "source":
      return <DatabaseOutlined style={iconStyle} />;
    case "output":
      return <TableOutlined style={iconStyle} />;
    case "filters":
      return <FilterOutlined style={iconStyle} />;
    case "joins":
      return <LinkOutlined style={iconStyle} />;
    case "sort":
    case "sort_fields":
      return <LineHeightOutlined style={iconStyle} />;
    case "synthesize":
    case "synthesize_column":
      return <PlusSquareOutlined style={iconStyle} />;
    case "unions":
      return <MergeCellsOutlined style={iconStyle} />;
    case "hidden_columns":
      return <EyeInvisibleOutlined style={iconStyle} />;
    case "distinct":
      return <CopyOutlined style={iconStyle} />;
    case "groups":
    case "groups_and_aggregation":
    case "aggregate":
      return <ProfileOutlined style={iconStyle} />;
    case "havings":
      return <ContainerOutlined style={iconStyle} />;
    case "rename":
    case "find_and_replace":
      return isDarkTheme ? (
        <FindReplaceDarkIcon style={svgStyle} />
      ) : (
        <FindReplaceLightIcon style={svgStyle} />
      );
    case "combine_columns":
      return isDarkTheme ? (
        <CombineColumnsDarkIcon style={svgStyle} />
      ) : (
        <CombineColumnsLightIcon style={svgStyle} />
      );
    case "pivot":
    case "unpivot":
      return isDarkTheme ? (
        <PivotDarkIcon style={svgStyle} />
      ) : (
        <PivotLightIcon style={svgStyle} />
      );
    default:
      return <CopyOutlined style={iconStyle} />;
  }
};

const LineageStepNode = memo(({ data }) => {
  const { stepType, label, detail, tableName, isDarkTheme, isFirst, isLast } =
    data;
  const colors = STEP_COLORS[stepType] || DEFAULT_COLOR;

  return (
    <div
      className={`lineage-step-node ${isDarkTheme ? "dark-theme" : ""}`}
      style={{ borderLeftColor: colors.border }}
    >
      {/* Target handle (top) - not on first node */}
      {!isFirst && (
        <Handle
          type="target"
          position={Position.Top}
          className="lineage-handle"
        />
      )}

      <div className="step-header">
        <div
          className="step-icon"
          style={{ backgroundColor: colors.bg, color: colors.icon }}
        >
          {getStepIcon(stepType, isDarkTheme)}
        </div>
        <span className="step-label">{label}</span>
      </div>

      {(tableName || detail) && (
        <div className="step-detail">{tableName || detail}</div>
      )}

      {/* Source handle (bottom) - not on last node */}
      {!isLast && (
        <Handle
          type="source"
          position={Position.Bottom}
          className="lineage-handle"
        />
      )}
    </div>
  );
});

LineageStepNode.displayName = "LineageStepNode";

LineageStepNode.propTypes = {
  data: PropTypes.shape({
    stepType: PropTypes.string.isRequired,
    label: PropTypes.string.isRequired,
    detail: PropTypes.string,
    tableName: PropTypes.string,
    isDarkTheme: PropTypes.bool,
    isFirst: PropTypes.bool,
    isLast: PropTypes.bool,
  }).isRequired,
};

export { LineageStepNode };
