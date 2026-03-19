import { useState, useEffect } from "react";
import PropTypes from "prop-types";
import {
  Form,
  Switch,
  Select,
  Input,
  Button,
  Alert,
  Spin,
  Typography,
  Card,
  Space,
  Tag,
} from "antd";
import {
  InfoCircleOutlined,
  SearchOutlined,
  CheckCircleOutlined,
} from "@ant-design/icons";

import { useJobService } from "./service";

const { Option } = Select;
const { Text } = Typography;

const WatermarkFields = ({
  form,
  disabled,
  projectId,
  environmentId,
  incrementalEnabled,
  onIncrementalChange,
}) => {
  const { detectWatermarkColumns } = useJobService();

  const [watermarkColumns, setWatermarkColumns] = useState({
    timestamp_candidates: [],
    sequence_candidates: [],
    table_info: {},
  });
  const [loading, setLoading] = useState(false);
  const [detectionComplete, setDetectionComplete] = useState(false);
  const [selectedStrategy, setSelectedStrategy] = useState("TIMESTAMP");

  const watermarkStrategies = [
    {
      value: "TIMESTAMP",
      label: "Timestamp-based",
      description:
        "Track using date/time columns (created_at, updated_at, etc.)",
      icon: "📅",
    },
    {
      value: "SEQUENCE",
      label: "Sequence-based",
      description: "Track using auto-increment IDs or sequence numbers",
      icon: "🔢",
    },
    {
      value: "CUSTOM",
      label: "Custom column",
      description:
        "Specify any column name manually (validated: letters, digits, underscores)",
      icon: "⚙️",
    },
  ];

  const detectColumns = async () => {
    if (!projectId || !environmentId) {
      return;
    }

    setLoading(true);
    try {
      const data = await detectWatermarkColumns(projectId, environmentId, null);
      setWatermarkColumns(
        data || {
          timestamp_candidates: [],
          sequence_candidates: [],
          table_info: {},
        }
      );
      setDetectionComplete(true);
    } catch (error) {
      console.error("Error detecting watermark columns:", error);
      // Show user-friendly error message
      setWatermarkColumns({
        timestamp_candidates: [],
        sequence_candidates: [],
        table_info: {},
        error: error?.message || "An unexpected error occurred",
      });
      setDetectionComplete(false);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (incrementalEnabled && projectId && environmentId) {
      detectColumns();
    }
  }, [incrementalEnabled, projectId, environmentId]);

  const getConfidenceColor = (confidence) => {
    if (confidence >= 0.8) return "success";
    if (confidence >= 0.6) return "warning";
    return "error";
  };

  const getConfidenceText = (confidence) => {
    if (confidence >= 0.8) return "High";
    if (confidence >= 0.6) return "Medium";
    return "Low";
  };

  const renderColumnOption = (column) => (
    <Option
      key={`${column.source_table || ""}.${column.column_name}`}
      value={column.column_name}
      label={column.column_name}
    >
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          padding: "2px 0",
        }}
      >
        <Space size={4}>
          <Text strong>{column.column_name}</Text>
          {column.source_table && (
            <Text type="secondary" style={{ fontSize: "11px" }}>
              ({column.source_table})
            </Text>
          )}
          <Text type="secondary" style={{ fontSize: "11px" }}>
            {column.data_type}
          </Text>
        </Space>
        <Tag
          color={getConfidenceColor(column.confidence)}
          style={{ marginRight: 0 }}
        >
          {getConfidenceText(column.confidence)}
        </Tag>
      </div>
    </Option>
  );

  const getCurrentCandidates = () => {
    if (selectedStrategy === "TIMESTAMP") {
      return watermarkColumns.timestamp_candidates || [];
    } else if (selectedStrategy === "SEQUENCE") {
      return watermarkColumns.sequence_candidates || [];
    }
    return [];
  };

  return (
    <Card
      title={
        <Space>
          <InfoCircleOutlined />
          Incremental Processing Configuration
        </Space>
      }
      style={{ marginBottom: 16 }}
    >
      <Form.Item
        name="incremental_enabled"
        valuePropName="checked"
        label="Enable Incremental Processing"
        tooltip="Process only new/changed data since the last run instead of full table scans"
      >
        <Switch
          disabled={disabled}
          onChange={onIncrementalChange}
          checkedChildren="Enabled"
          unCheckedChildren="Disabled"
        />
      </Form.Item>

      {incrementalEnabled && (
        <>
          <Alert
            message="Incremental Processing Benefits"
            description="Reduces processing time and resource usage by only processing new data since the last successful run. Ideal for large datasets with regular updates."
            type="info"
            showIcon
            style={{ marginBottom: 16 }}
          />

          <Form.Item
            name="watermark_strategy"
            label="Watermark Strategy"
            tooltip="Choose how to track processed data"
            rules={[
              { required: true, message: "Please select a watermark strategy" },
            ]}
          >
            <Select
              disabled={disabled}
              placeholder="Select watermark strategy"
              onChange={setSelectedStrategy}
              style={{ width: "100%" }}
              optionLabelProp="label"
              dropdownStyle={{ minWidth: "400px" }}
            >
              {watermarkStrategies.map((strategy) => (
                <Option
                  key={strategy.value}
                  value={strategy.value}
                  label={strategy.label}
                >
                  <div
                    style={{
                      display: "flex",
                      alignItems: "flex-start",
                      gap: "8px",
                      padding: "4px 0",
                    }}
                  >
                    <span style={{ fontSize: "16px", marginTop: "2px" }}>
                      {strategy.icon}
                    </span>
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div style={{ fontWeight: 500, lineHeight: "1.2" }}>
                        {strategy.label}
                      </div>
                      <div
                        style={{
                          fontSize: "11px",
                          color: "#666",
                          marginTop: "2px",
                          lineHeight: "1.3",
                          wordWrap: "break-word",
                        }}
                      >
                        {strategy.description}
                      </div>
                    </div>
                  </div>
                </Option>
              ))}
            </Select>
          </Form.Item>

          {(selectedStrategy === "TIMESTAMP" ||
            selectedStrategy === "SEQUENCE") && (
            <>
              <div
                style={{
                  marginBottom: 16,
                  display: "flex",
                  flexDirection: "column",
                  gap: "8px",
                }}
              >
                <Button
                  icon={<SearchOutlined />}
                  onClick={detectColumns}
                  loading={loading}
                  disabled={disabled || !projectId || !environmentId}
                  type="dashed"
                  style={{ alignSelf: "flex-start" }}
                >
                  {detectionComplete
                    ? "Re-detect Columns"
                    : "Detect Watermark Columns"}
                </Button>
                {!projectId || !environmentId ? (
                  <Text type="secondary" style={{ fontSize: "12px" }}>
                    Please select a project and environment first
                  </Text>
                ) : null}
                {detectionComplete && (
                  <Tag
                    icon={<CheckCircleOutlined />}
                    color="success"
                    style={{ alignSelf: "flex-start" }}
                  >
                    {watermarkColumns.table_info?.tables_analyzed
                      ? `${watermarkColumns.table_info.tables_analyzed} tables`
                      : watermarkColumns.table_info?.table_name}{" "}
                    • {watermarkColumns.table_info?.row_count?.toLocaleString()}{" "}
                    rows
                  </Tag>
                )}
              </div>

              {loading && (
                <div style={{ textAlign: "center", padding: "20px" }}>
                  <Spin size="large" />
                  <br />
                  <Text type="secondary">
                    Analyzing table schema for watermark columns...
                  </Text>
                </div>
              )}

              {detectionComplete && !loading && (
                <Form.Item
                  name="watermark_column"
                  label={`${
                    selectedStrategy === "TIMESTAMP" ? "Timestamp" : "Sequence"
                  } Column`}
                  tooltip={`Select the ${selectedStrategy.toLowerCase()} column to use for tracking processed data`}
                  rules={[
                    {
                      required: true,
                      message: "Please select a watermark column",
                    },
                  ]}
                >
                  <Select
                    disabled={disabled}
                    placeholder={`Select ${selectedStrategy.toLowerCase()} column`}
                    showSearch
                    optionFilterProp="label"
                  >
                    {getCurrentCandidates().map((column) =>
                      renderColumnOption(column)
                    )}
                  </Select>
                </Form.Item>
              )}

              {getCurrentCandidates().length === 0 &&
                detectionComplete &&
                !loading && (
                  <Alert
                    message={`No ${selectedStrategy.toLowerCase()} columns detected`}
                    description={`No suitable ${selectedStrategy.toLowerCase()} columns were found in the source table. Consider using a different strategy or adding appropriate columns to your data source.`}
                    type="warning"
                    showIcon
                    style={{ marginBottom: 16 }}
                  />
                )}
            </>
          )}

          {selectedStrategy === "CUSTOM" && (
            <Form.Item
              name="watermark_column"
              label="Custom Watermark Column"
              tooltip="Enter the name of the column to use for watermarking"
              rules={[
                {
                  required: true,
                  message: "Please enter a watermark column name",
                },
                {
                  pattern: /^[a-zA-Z_][a-zA-Z0-9_]*$/,
                  message: "Invalid column name format",
                },
              ]}
            >
              <Input
                disabled={disabled}
                placeholder="e.g., last_modified, version_number"
              />
            </Form.Item>
          )}
        </>
      )}
    </Card>
  );
};

WatermarkFields.propTypes = {
  form: PropTypes.object.isRequired,
  disabled: PropTypes.bool,
  projectId: PropTypes.string,
  environmentId: PropTypes.string,
  incrementalEnabled: PropTypes.bool,
  onIncrementalChange: PropTypes.func,
};

export default WatermarkFields;
