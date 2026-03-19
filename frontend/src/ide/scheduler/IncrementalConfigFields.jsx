import { useMemo, useCallback } from "react";
import PropTypes from "prop-types";
import { Select, Typography, AutoComplete, Popover, Button } from "antd";
import {
  ClockCircleOutlined,
  FieldNumberOutlined,
  CalendarOutlined,
  SyncOutlined,
  InfoCircleOutlined,
} from "@ant-design/icons";

const { Text } = Typography;

const DELTA_STRATEGIES = [
  { value: "timestamp", label: "Timestamp", icon: <ClockCircleOutlined /> },
  { value: "sequence", label: "Sequence", icon: <FieldNumberOutlined /> },
  { value: "date", label: "Date", icon: <CalendarOutlined /> },
  { value: "full_scan", label: "Full Scan", icon: <SyncOutlined /> },
];

const IncrementalConfigFields = ({
  config,
  onChange,
  destinationColumns,
  sourceColumns,
  disabled,
}) => {
  const uniqueKeys = config?.primary_key || [];
  const deltaStrategy = config?.delta_strategy?.type || "timestamp";
  const deltaColumn = config?.delta_strategy?.column || null;

  const updateConfig = useCallback(
    (updates) => {
      onChange({ ...config, ...updates });
    },
    [config, onChange]
  );

  const handleUniqueKeyChange = useCallback(
    (keys) => updateConfig({ primary_key: keys }),
    [updateConfig]
  );

  // Check if a column matches a given strategy type
  const columnMatchesStrategy = useCallback((col, strategy) => {
    const dt = (col?.data_type || "").toLowerCase();
    if (strategy === "timestamp")
      return (
        dt.includes("timestamp") ||
        dt.includes("datetime") ||
        dt.includes("time")
      );
    if (strategy === "sequence")
      return (
        dt.includes("int") ||
        dt.includes("serial") ||
        dt.includes("bigint") ||
        dt.includes("number")
      );
    if (strategy === "date")
      return dt.includes("date") && !dt.includes("datetime");
    return true;
  }, []);

  const handleDeltaStrategyChange = useCallback(
    (type) => {
      let newColumn = deltaColumn;

      if (type === "full_scan") {
        newColumn = null;
      } else if (deltaColumn) {
        const currentCol = sourceColumns?.find(
          (c) => c.column_name === deltaColumn
        );
        if (currentCol && !columnMatchesStrategy(currentCol, type)) {
          newColumn = null;
        }
      }

      updateConfig({
        delta_strategy: {
          type,
          column: newColumn,
        },
      });
    },
    [updateConfig, deltaColumn, sourceColumns, columnMatchesStrategy]
  );

  const handleDeltaColumnChange = useCallback(
    (column) =>
      updateConfig({ delta_strategy: { type: deltaStrategy, column } }),
    [updateConfig, deltaStrategy]
  );

  // Destination columns for unique key selection
  const uniqueKeyOptions = useMemo(
    () =>
      (destinationColumns || []).map((col) => ({
        label: col.data_type
          ? `${col.column_name} (${col.data_type})`
          : col.column_name,
        value: col.column_name,
      })),
    [destinationColumns]
  );

  // Source columns filtered based on delta strategy type
  const filterColumnOptions = useMemo(() => {
    const cols = sourceColumns || [];

    const matchesStrategy = (col) => {
      const dt = (col.data_type || "").toLowerCase();
      if (deltaStrategy === "timestamp")
        return (
          dt.includes("timestamp") ||
          dt.includes("datetime") ||
          dt.includes("time")
        );
      if (deltaStrategy === "sequence")
        return (
          dt.includes("int") ||
          dt.includes("serial") ||
          dt.includes("bigint") ||
          dt.includes("number")
        );
      if (deltaStrategy === "date")
        return dt.includes("date") && !dt.includes("datetime");
      return true;
    };

    return cols.filter(matchesStrategy).map((col) => ({
      label: col.data_type
        ? `${col.column_name} (${col.data_type})`
        : col.column_name,
      value: col.column_name,
    }));
  }, [sourceColumns, deltaStrategy]);

  const needsColumn = deltaStrategy !== "full_scan";
  const columnLabel =
    deltaStrategy === "timestamp"
      ? "Timestamp Column"
      : deltaStrategy === "sequence"
      ? "Sequence Column"
      : deltaStrategy === "date"
      ? "Date Column"
      : "";

  /* ─── SQL Behavior popover content ─── */
  const sqlContent = (
    <Text style={{ fontSize: 11, lineHeight: 1.6 }}>
      {deltaStrategy === "full_scan" ? (
        <>
          1. Select all rows from source table
          <br />
          2. Compare against destination table
          <br />
        </>
      ) : deltaColumn ? (
        <>
          1. Filter source rows where{" "}
          <Text code style={{ fontSize: 11 }}>
            {deltaColumn}
          </Text>{" "}
          &gt; last processed value
          <br />
        </>
      ) : (
        <>
          1. Filter source rows using {deltaStrategy} column (not selected)
          <br />
        </>
      )}
      {uniqueKeys.length > 0 ? (
        <>
          {deltaStrategy === "full_scan" ? "3" : "2"}. MERGE into destination
          using{" "}
          <Text code style={{ fontSize: 11 }}>
            {uniqueKeys.join(", ")}
          </Text>{" "}
          as key(s)
          <br />
          <Text type="secondary" style={{ fontSize: 10, marginLeft: 12 }}>
            → INSERT new rows, UPDATE existing rows
          </Text>
        </>
      ) : (
        <>
          {deltaStrategy === "full_scan" ? "3" : "2"}. INSERT into destination
          <br />
          <Text type="secondary" style={{ fontSize: 10, marginLeft: 12 }}>
            → Append only, no updates to existing rows
          </Text>
        </>
      )}
    </Text>
  );

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
      {/* Unique Key */}
      <div>
        <Text
          type="secondary"
          strong
          style={{ fontSize: 12, marginBottom: 4, display: "block" }}
        >
          Unique Key
        </Text>
        <Select
          mode="tags"
          size="small"
          value={uniqueKeys}
          onChange={handleUniqueKeyChange}
          disabled={disabled}
          placeholder="Column(s) for merge/upsert (destination)"
          style={{ width: "100%" }}
          maxTagCount={2}
          options={uniqueKeyOptions}
          optionFilterProp="label"
          tokenSeparators={[","]}
        />
        <Text
          type={uniqueKeys.length > 0 ? "success" : "warning"}
          style={{ fontSize: 11, marginTop: 2, display: "block" }}
        >
          {uniqueKeys.length > 0
            ? "Merge mode: New rows inserted, existing rows updated"
            : "Append mode: New rows added without updating existing data"}
        </Text>
      </div>

      {/* Filter Type + Column on same row */}
      <div style={{ display: "flex", gap: 12, alignItems: "flex-start" }}>
        <div style={{ flex: "0 0 160px" }}>
          <Text
            type="secondary"
            strong
            style={{ fontSize: 12, marginBottom: 4, display: "block" }}
          >
            Filter Type
          </Text>
          <Select
            size="small"
            value={deltaStrategy}
            onChange={handleDeltaStrategyChange}
            disabled={disabled}
            style={{ width: "100%" }}
            optionLabelProp="label"
          >
            {DELTA_STRATEGIES.map((s) => (
              <Select.Option key={s.value} value={s.value} label={s.label}>
                <span style={{ marginRight: 6 }}>{s.icon}</span>
                {s.label}
              </Select.Option>
            ))}
          </Select>
        </div>
        {needsColumn && (
          <div style={{ flex: 1 }}>
            <Text
              type="secondary"
              strong
              style={{ fontSize: 12, marginBottom: 4, display: "block" }}
            >
              {columnLabel}
            </Text>
            <AutoComplete
              size="small"
              value={deltaColumn}
              onChange={handleDeltaColumnChange}
              disabled={disabled}
              placeholder="Type or select column (source)"
              style={{ width: "100%" }}
              options={filterColumnOptions}
              filterOption={(inputValue, option) =>
                option?.value?.toLowerCase().includes(inputValue.toLowerCase())
              }
            />
          </div>
        )}
      </div>

      {/* Warning when no matching columns found */}
      {needsColumn &&
        filterColumnOptions.length === 0 &&
        sourceColumns?.length > 0 && (
          <Text type="warning" style={{ fontSize: 11 }}>
            No {deltaStrategy} columns found in source. Select a different
            filter type.
          </Text>
        )}

      {/* Full scan note */}
      {!needsColumn && (
        <Text type="secondary" style={{ fontSize: 11 }}>
          Compares all source rows against destination (slower, but no filter
          column needed)
        </Text>
      )}

      {/* SQL Behavior — popover */}
      <Popover
        title="SQL Behavior"
        content={sqlContent}
        trigger="hover"
        placement="bottom"
      >
        <Button
          type="link"
          size="small"
          icon={<InfoCircleOutlined />}
          style={{ fontSize: 11, padding: 0, alignSelf: "flex-start" }}
        >
          View SQL behavior
        </Button>
      </Popover>
    </div>
  );
};

IncrementalConfigFields.propTypes = {
  modelName: PropTypes.string,
  config: PropTypes.shape({
    primary_key: PropTypes.arrayOf(PropTypes.string),
    delta_strategy: PropTypes.shape({
      type: PropTypes.string,
      column: PropTypes.string,
    }),
  }),
  onChange: PropTypes.func.isRequired,
  destinationColumns: PropTypes.arrayOf(
    PropTypes.shape({
      column_name: PropTypes.string,
      data_type: PropTypes.string,
    })
  ),
  sourceColumns: PropTypes.arrayOf(
    PropTypes.shape({
      column_name: PropTypes.string,
      data_type: PropTypes.string,
    })
  ),
  disabled: PropTypes.bool,
};

IncrementalConfigFields.defaultProps = {
  config: {},
  destinationColumns: [],
  sourceColumns: [],
  disabled: false,
};

export default IncrementalConfigFields;
