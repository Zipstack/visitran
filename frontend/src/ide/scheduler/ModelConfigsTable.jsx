import { useState, useEffect, useCallback, useMemo } from "react";
import PropTypes from "prop-types";
import {
  Table,
  Select,
  Switch,
  Button,
  Space,
  Tooltip,
  Typography,
  Spin,
  Empty,
} from "antd";
import {
  TableOutlined,
  EyeOutlined,
  ThunderboltOutlined,
  ReloadOutlined,
  InfoCircleOutlined,
} from "@ant-design/icons";

import { useJobService } from "./service";
import IncrementalConfigFields from "./IncrementalConfigFields";

const { Text } = Typography;

const MATERIALIZATION_OPTIONS = [
  {
    value: "TABLE",
    label: "Table",
    icon: <TableOutlined />,
    color: "blue",
    description:
      "Drops and rebuilds the destination table on every run. Slowest on large data but always consistent. Good default when unsure.",
  },
  {
    value: "VIEW",
    label: "View",
    icon: <EyeOutlined />,
    color: "green",
    description:
      "Stores the transformation as a SQL view — no data is written. Fast to build but every downstream read re-executes the query. Pick for lightweight transforms or small data.",
  },
  {
    value: "INCREMENTAL",
    label: "Incremental",
    icon: <ThunderboltOutlined />,
    color: "orange",
    description:
      "Appends/merges only new or changed rows using a watermark (primary key + delta column). Fastest on large, append-only data — but needs a reliable delta column and the right primary key.",
  },
];

const MATERIALIZATION_COLUMN_HELP = (
  <div style={{ maxWidth: 360 }}>
    <strong>How a model gets written to the warehouse</strong>
    <ul style={{ paddingLeft: 18, margin: "6px 0" }}>
      <li>
        <strong>Table</strong> — full rebuild every run. Safest, slowest on
        large data.
      </li>
      <li>
        <strong>View</strong> — stored as SQL only; nothing materialized. Cheap,
        but downstream reads re-run the query.
      </li>
      <li>
        <strong>Incremental</strong> — only new/changed rows appended via a
        watermark. Fastest on large data; needs a valid primary key + delta
        column.
      </li>
    </ul>
    <div style={{ marginTop: 6, opacity: 0.85 }}>
      Switching materialization later is safe, but the first run after a switch
      rebuilds the destination from scratch (and an Incremental model needs its
      watermark reset).
    </div>
  </div>
);

const ModelConfigsTable = ({
  models,
  modelConfigs,
  onModelConfigsChange,
  projectId,
  environmentId,
  disabled,
  loading: externalLoading,
  expanded,
}) => {
  const { getModelColumns } = useJobService();

  const [expandedRows, setExpandedRows] = useState([]);
  const [columnCache, setColumnCache] = useState({});
  const [loadingColumns, setLoadingColumns] = useState({});

  // Initialize model configs for all models if not present
  useEffect(() => {
    if (!models?.length) return;

    const currentConfigs = { ...modelConfigs };
    let hasChanges = false;

    models.forEach((modelName) => {
      if (!currentConfigs[modelName]) {
        currentConfigs[modelName] = {
          enabled: true,
          materialization: "TABLE",
          incremental_config: null,
        };
        hasChanges = true;
      }
    });

    if (hasChanges) {
      onModelConfigsChange(currentConfigs);
    }
  }, [models]);

  // Fetch columns for a model when needed (no environmentId required).
  // Pass { force: true } from Refresh to bypass the cache check; clearing
  // columnCache + calling this normally hits a stale-closure race where the
  // pre-deletion snapshot still reads as populated and the refetch no-ops.
  const fetchColumnsForModel = useCallback(
    async (modelName, { force = false } = {}) => {
      if (!projectId) return;
      if (!force && columnCache[modelName]) return;

      setLoadingColumns((prev) => ({ ...prev, [modelName]: true }));

      try {
        const { destinationColumns, sourceColumns } = await getModelColumns(
          projectId,
          modelName
        );
        // Store both destination columns (for unique key) and source columns (for filter)
        setColumnCache((prev) => ({
          ...prev,
          [modelName]: {
            destinationColumns: destinationColumns || [],
            sourceColumns: sourceColumns || [],
          },
        }));
      } catch (error) {
        console.error(`Failed to fetch columns for ${modelName}:`, error);
        setColumnCache((prev) => ({
          ...prev,
          [modelName]: {
            destinationColumns: [],
            sourceColumns: [],
            error: true,
          },
        }));
      } finally {
        setLoadingColumns((prev) => ({ ...prev, [modelName]: false }));
      }
    },
    [projectId, columnCache, getModelColumns]
  );

  // Handle row expansion
  const handleExpand = useCallback(
    (expanded, record) => {
      if (expanded) {
        setExpandedRows((prev) => [...prev, record.key]);
        const config = modelConfigs[record.modelName];
        if (config?.materialization === "INCREMENTAL") {
          fetchColumnsForModel(record.modelName);
        }
      } else {
        setExpandedRows((prev) => prev.filter((key) => key !== record.key));
      }
    },
    [modelConfigs, fetchColumnsForModel]
  );

  // Update a single model's config
  const updateModelConfig = useCallback(
    (modelName, updates) => {
      const newConfigs = {
        ...modelConfigs,
        [modelName]: {
          ...modelConfigs[modelName],
          ...updates,
        },
      };
      onModelConfigsChange(newConfigs);
    },
    [modelConfigs, onModelConfigsChange]
  );

  // Handle materialization change
  const handleMaterializationChange = useCallback(
    (modelName, value) => {
      const updates = { materialization: value };

      // Initialize incremental config when switching to INCREMENTAL
      if (value === "INCREMENTAL") {
        updates.incremental_config = {
          primary_key: [],
          delta_strategy: { type: "timestamp", column: null },
        };
        // Auto-expand row and fetch columns
        if (!expandedRows.includes(modelName)) {
          setExpandedRows((prev) => [...prev, modelName]);
        }
        fetchColumnsForModel(modelName);
      } else {
        updates.incremental_config = null;
      }

      updateModelConfig(modelName, updates);
    },
    [expandedRows, fetchColumnsForModel, updateModelConfig]
  );

  // Handle incremental config change
  const handleIncrementalConfigChange = useCallback(
    (modelName, incrementalConfig) => {
      updateModelConfig(modelName, { incremental_config: incrementalConfig });
    },
    [updateModelConfig]
  );

  // Build table data
  const tableData = useMemo(() => {
    return (models || []).map((modelName) => ({
      key: modelName,
      modelName,
      config: modelConfigs[modelName] || {
        enabled: true,
        materialization: "TABLE",
        incremental_config: null,
      },
    }));
  }, [models, modelConfigs]);

  // Render materialization select with icons
  const renderMaterializationSelect = (record) => {
    const config = record.config;

    return (
      <Select
        value={config.materialization}
        onChange={(value) =>
          handleMaterializationChange(record.modelName, value)
        }
        disabled={disabled || !config.enabled}
        style={{ width: 150 }}
        popupMatchSelectWidth={320}
        optionLabelProp="label"
      >
        {MATERIALIZATION_OPTIONS.map((opt) => (
          <Select.Option
            key={opt.value}
            value={opt.value}
            label={
              <Space size={4}>
                {opt.icon}
                <span>{opt.label}</span>
              </Space>
            }
          >
            <div style={{ padding: "4px 0" }}>
              <Space size={6}>
                {opt.icon}
                <strong>{opt.label}</strong>
              </Space>
              <div
                style={{
                  fontSize: 12,
                  opacity: 0.75,
                  marginTop: 2,
                  whiteSpace: "normal",
                  lineHeight: 1.4,
                }}
              >
                {opt.description}
              </div>
            </div>
          </Select.Option>
        ))}
      </Select>
    );
  };

  // Table columns
  const columns = [
    {
      title: "Model",
      dataIndex: "modelName",
      key: "modelName",
      render: (modelName, record) => (
        <Text
          strong={record.config.enabled}
          type={record.config.enabled ? undefined : "secondary"}
        >
          {modelName}
        </Text>
      ),
    },
    {
      title: "Enabled",
      key: "enabled",
      width: 100,
      render: (_, record) => (
        <Switch
          checked={record.config.enabled}
          onChange={(checked) =>
            updateModelConfig(record.modelName, { enabled: checked })
          }
          disabled={disabled}
          size="small"
        />
      ),
    },
    {
      title: (
        <Space size={4}>
          Materialization
          <Tooltip title={MATERIALIZATION_COLUMN_HELP} placement="top">
            <InfoCircleOutlined style={{ opacity: 0.55, cursor: "help" }} />
          </Tooltip>
        </Space>
      ),
      key: "materialization",
      width: 200,
      render: (_, record) => renderMaterializationSelect(record),
    },
  ];

  // Expanded row render for incremental config
  const expandedRowRender = (record) => {
    if (record.config.materialization !== "INCREMENTAL") {
      return null;
    }

    const cachedData = columnCache[record.modelName] || {
      destinationColumns: [],
      sourceColumns: [],
    };
    const destinationColumns = cachedData.destinationColumns || [];
    const sourceColumns = cachedData.sourceColumns || [];
    const isLoading = loadingColumns[record.modelName];
    const hasColumns =
      destinationColumns.length > 0 || sourceColumns.length > 0;

    return (
      <div className="incremental-config-panel">
        <div className="incremental-config-panel-header">
          <Text type="secondary" strong style={{ fontSize: 12 }}>
            <ThunderboltOutlined style={{ marginRight: 4 }} />
            Incremental Settings — {record.modelName}
          </Text>
          <Button
            size="small"
            type="text"
            icon={<ReloadOutlined spin={isLoading} />}
            onClick={() => {
              fetchColumnsForModel(record.modelName, { force: true });
            }}
            disabled={isLoading}
          >
            {isLoading ? "Loading..." : hasColumns ? "Refresh" : "Load Columns"}
          </Button>
        </div>
        <div className="incremental-config-panel-body">
          <IncrementalConfigFields
            modelName={record.modelName}
            config={record.config.incremental_config || {}}
            onChange={(config) =>
              handleIncrementalConfigChange(record.modelName, config)
            }
            destinationColumns={destinationColumns}
            sourceColumns={sourceColumns}
            disabled={disabled}
          />
        </div>
      </div>
    );
  };

  if (externalLoading) {
    return (
      <div style={{ textAlign: "center", padding: 48 }}>
        <Spin size="large" />
        <Text type="secondary" style={{ display: "block", marginTop: 16 }}>
          Loading models...
        </Text>
      </div>
    );
  }

  if (!models?.length) {
    return (
      <Empty
        description="No models found. Please select a project first."
        image={Empty.PRESENTED_IMAGE_SIMPLE}
      />
    );
  }

  return (
    <div className="model-configs-table">
      <Table
        columns={columns}
        dataSource={tableData}
        rowKey="key"
        pagination={false}
        size="small"
        expandable={{
          expandedRowRender,
          expandedRowKeys: expandedRows,
          onExpand: handleExpand,
          rowExpandable: (record) =>
            record.config.materialization === "INCREMENTAL",
        }}
        scroll={{ y: expanded ? 600 : 400 }}
      />

      <div style={{ marginTop: 12 }}>
        <Space>
          <Text type="secondary">
            {tableData.filter((r) => r.config.enabled).length} of{" "}
            {tableData.length} models enabled
          </Text>
          <Text type="secondary">|</Text>
          <Text type="secondary">
            {
              tableData.filter(
                (r) => r.config.materialization === "INCREMENTAL"
              ).length
            }{" "}
            incremental
          </Text>
        </Space>
      </div>
    </div>
  );
};

ModelConfigsTable.propTypes = {
  models: PropTypes.arrayOf(PropTypes.string),
  modelConfigs: PropTypes.object,
  onModelConfigsChange: PropTypes.func.isRequired,
  projectId: PropTypes.string,
  environmentId: PropTypes.string,
  disabled: PropTypes.bool,
  loading: PropTypes.bool,
  expanded: PropTypes.bool,
};

ModelConfigsTable.defaultProps = {
  models: [],
  modelConfigs: {},
  expanded: false,
  disabled: false,
  loading: false,
};

export default ModelConfigsTable;
