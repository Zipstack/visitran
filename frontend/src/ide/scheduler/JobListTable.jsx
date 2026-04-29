/* eslint-disable react/prop-types */
import { memo, useMemo, useState } from "react";
import {
  Table,
  Typography,
  Tooltip,
  Tag,
  Space,
  Button,
  Switch,
  Empty,
  theme,
} from "antd";
import {
  EditOutlined,
  DeleteOutlined,
  PlayCircleOutlined,
  LoadingOutlined,
  HistoryOutlined,
  CheckCircleFilled,
  CloseCircleFilled,
  SyncOutlined,
  ClockCircleOutlined,
  PauseCircleOutlined,
  FireFilled,
  ExperimentOutlined,
  CodeOutlined,
  FieldTimeOutlined,
} from "@ant-design/icons";
import PropTypes from "prop-types";
import { useNavigate } from "react-router-dom";

import { useJobService } from "./service";
import {
  getTooltipText,
  getRelativeTime,
  formatDateTime,
} from "../../common/helpers";
import { useNotificationService } from "../../service/notification-service";

const { Text } = Typography;

/* ── Duration formatter ── */
const formatDurationMs = (ms) => {
  if (!ms && ms !== 0) return null;
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  const m = Math.floor(ms / 60000);
  const s = ((ms % 60000) / 1000).toFixed(0);
  return `${m}m ${s}s`;
};

/* ── Environment badge ── */
const EnvironmentBadge = ({ type, name }) => {
  const config = {
    PROD: { color: "error", label: "PROD", icon: <FireFilled /> },
    STG: { color: "warning", label: "STG", icon: <ExperimentOutlined /> },
    DEV: { color: "blue", label: "DEV", icon: <CodeOutlined /> },
  };
  const c = config[type] || config.DEV;
  return (
    <Space direction="vertical" size={1}>
      <Tag
        color={c.color}
        icon={c.icon}
        style={{ fontWeight: 700, fontSize: 10, letterSpacing: 0.4, margin: 0 }}
      >
        {c.label}
      </Tag>
      <Text type="secondary" style={{ fontSize: 11, fontFamily: "monospace" }}>
        {name}
      </Text>
    </Space>
  );
};

/* ── Schedule badge ── */
const ScheduleBadge = ({ type, details }) => {
  const isCron = type === "cron";
  const expression = isCron ? details?.cron?.cron_expression : null;
  let description = "";
  try {
    if (isCron && expression) {
      description =
        getTooltipText({ cron_expression: expression }, type) + " (UTC)";
    } else if (!isCron && details?.interval) {
      description = getTooltipText(details.interval, type);
    }
  } catch {
    description = expression || "";
  }
  return (
    <Space direction="vertical" size={1}>
      <Space size={4}>
        <Tag
          color={isCron ? "purple" : "cyan"}
          icon={isCron ? <FieldTimeOutlined /> : <SyncOutlined />}
          style={{
            fontWeight: 700,
            fontSize: 10,
            letterSpacing: 0.4,
            margin: 0,
          }}
        >
          {isCron ? "CRON" : "INTERVAL"}
        </Tag>
        {expression && (
          <Text code style={{ fontSize: 11 }}>
            {expression}
          </Text>
        )}
      </Space>
      <Text type="secondary" style={{ fontSize: 11 }}>
        {description}
      </Text>
    </Space>
  );
};

const JobListTable = memo(
  ({
    data,
    onRowClick,
    setIsDeleteModalOpen,
    setDelTaskDetail,
    tableLoading,
    onToggleSuccess,
  }) => {
    const { updateTask, runTask } = useJobService();
    const [loading, setLoading] = useState({});
    const { notify } = useNotificationService();
    const navigate = useNavigate();
    const { token } = theme.useToken();

    const goToRunHistory = (userTaskId) =>
      navigate(`/project/job/history?task=${userTaskId}`);

    const handleSwitchSchedular = async (item, checked) => {
      try {
        const {
          user_task_id,
          project,
          periodic_task_details,
          description,
          task_name,
          task_type,
          environment,
        } = item;
        const res = await updateTask(project.id, user_task_id, {
          ...periodic_task_details[task_type],
          task_type,
          task_name,
          description,
          project: project.id,
          environment: environment.id,
          enabled: checked,
        });
        if (res.status && onToggleSuccess) onToggleSuccess();
      } catch (error) {
        notify({ error });
      }
    };

    const handleDelete = (record) => {
      setDelTaskDetail({
        projectId: record.project.id,
        taskId: record.periodic_task_details.id,
      });
      setIsDeleteModalOpen(true);
    };

    const handleRun = async (projectId, taskId) => {
      setLoading((prev) => ({ ...prev, [taskId]: true }));
      try {
        await runTask(projectId, taskId);
        notify({ type: "success", message: "Job triggered successfully" });
      } catch (error) {
        notify({ error });
      } finally {
        setLoading((prev) => {
          // eslint-disable-next-line no-unused-vars
          const { [taskId]: _, ...rest } = prev;
          return rest;
        });
      }
    };

    const columns = useMemo(
      () => [
        {
          title: "Job",
          dataIndex: "task_name",
          key: "task_name",
          sorter: (a, b) =>
            (a.task_name || "").localeCompare(b.task_name || ""),
          render: (text, record) => {
            const isFailed = [
              "FAILED",
              "FAILED PERMANENTLY",
              "FAILURE",
            ].includes(record.task_status);
            const isSuccess = record.task_status === "SUCCESS";
            const isRunning = ["RUNNING", "STARTED", "PENDING"].includes(
              record.task_status
            );
            const isPaused = !record.periodic_task_details?.enabled;
            let statusIcon;
            let statusClass;
            let tooltipText;
            if (isPaused) {
              statusIcon = <PauseCircleOutlined />;
              statusClass = "paused";
              tooltipText = "Paused — will not run";
            } else if (isFailed) {
              statusIcon = <CloseCircleFilled />;
              statusClass = "failed";
              tooltipText = "Last run failed — needs attention";
            } else if (isSuccess) {
              statusIcon = <CheckCircleFilled />;
              statusClass = "success";
              tooltipText = "Healthy — last run succeeded";
            } else if (isRunning) {
              statusIcon = <SyncOutlined spin />;
              statusClass = "running";
              tooltipText = "Running";
            } else {
              statusIcon = <ClockCircleOutlined />;
              statusClass = "paused";
              tooltipText = "Scheduled — has not run yet";
            }
            return (
              <div className="jl-job-name">
                <Tooltip title={tooltipText} placement="right">
                  <div className={`jl-job-icon ${statusClass}`}>
                    {statusIcon}
                  </div>
                </Tooltip>
                <div>
                  <Button
                    type="link"
                    size="small"
                    style={{ padding: 0, height: "auto", fontWeight: 600 }}
                    onClick={() => goToRunHistory(record.user_task_id)}
                  >
                    {text}
                  </Button>
                  {record.description && (
                    <div>
                      <Text type="secondary" style={{ fontSize: 11 }}>
                        {record.description}
                      </Text>
                    </div>
                  )}
                </div>
              </div>
            );
          },
        },
        {
          title: "Project",
          dataIndex: "project",
          key: "project",
          render: (project) => <Text>{project?.name}</Text>,
        },
        {
          title: "Environment",
          dataIndex: "environment",
          key: "environment",
          render: (env) =>
            env ? (
              <EnvironmentBadge type={env.type} name={env.name} />
            ) : (
              <Text type="secondary">—</Text>
            ),
        },
        {
          title: "Schedule",
          key: "schedule",
          render: (_, record) => (
            <ScheduleBadge
              type={record.task_type}
              details={record.periodic_task_details}
            />
          ),
        },
        {
          title: "Last Run",
          key: "last_run",
          sorter: (a, b) =>
            new Date(a.task_completion_time || 0) -
            new Date(b.task_completion_time || 0),
          render: (_, record) => {
            if (!record.task_status) return <Text type="secondary">—</Text>;
            const isFailed = [
              "FAILED",
              "FAILED PERMANENTLY",
              "FAILURE",
            ].includes(record.task_status);
            const isSuccess = record.task_status === "SUCCESS";
            const isRunning = ["RUNNING", "STARTED", "PENDING"].includes(
              record.task_status
            );

            // Compute duration if both times available
            let duration = null;
            if (record.task_run_time && record.task_completion_time) {
              const ms =
                new Date(record.task_completion_time) -
                new Date(record.task_run_time);
              if (ms > 0) duration = formatDurationMs(ms);
            }

            return (
              <Space direction="vertical" size={2}>
                <Tag
                  icon={
                    isSuccess ? (
                      <CheckCircleFilled />
                    ) : isFailed ? (
                      <CloseCircleFilled />
                    ) : isRunning ? (
                      <SyncOutlined spin />
                    ) : null
                  }
                  color={
                    isSuccess
                      ? "success"
                      : isFailed
                      ? "error"
                      : isRunning
                      ? "processing"
                      : "default"
                  }
                >
                  {record.task_status === "FAILURE"
                    ? "FAILED"
                    : record.task_status}
                </Tag>
                {record.task_completion_time && (
                  <Tooltip
                    title={new Date(record.task_completion_time).toISOString()}
                  >
                    <Text style={{ fontSize: 12 }}>
                      {formatDateTime(record.task_completion_time)}
                    </Text>
                  </Tooltip>
                )}
                <Text type="secondary" style={{ fontSize: 11 }}>
                  {record.task_completion_time
                    ? getRelativeTime(record.task_completion_time)
                    : ""}
                  {duration ? ` · ${duration}` : ""}
                </Text>
              </Space>
            );
          },
        },
        {
          title: "Next Run",
          dataIndex: "next_run_time",
          key: "next_run",
          sorter: (a, b) =>
            new Date(a.next_run_time || 0) - new Date(b.next_run_time || 0),
          render: (text, record) => {
            if (!text) {
              if (!record.periodic_task_details?.enabled) {
                return (
                  <Space size={4}>
                    <PauseCircleOutlined
                      style={{ color: token.colorTextSecondary }}
                    />
                    <Text type="secondary">Paused</Text>
                  </Space>
                );
              }
              return <Text type="secondary">—</Text>;
            }
            // Compute "in Xm" countdown
            const diff = new Date(text) - new Date();
            let countdown = null;
            if (diff > 0) {
              const mins = Math.floor(diff / 60000);
              if (mins < 60) countdown = `in ${mins}m`;
              else if (mins < 1440)
                countdown = `in ${Math.floor(mins / 60)}h ${mins % 60}m`;
              else countdown = `in ${Math.floor(mins / 1440)}d`;
            }
            return (
              <Space direction="vertical" size={1}>
                <Text style={{ fontSize: 12 }}>{formatDateTime(text)}</Text>
                {countdown && (
                  <Tag
                    color="blue"
                    style={{
                      fontSize: 10,
                      lineHeight: "16px",
                      padding: "0 4px",
                      margin: 0,
                    }}
                  >
                    <ClockCircleOutlined /> {countdown}
                  </Tag>
                )}
              </Space>
            );
          },
        },
        {
          title: "Status",
          key: "status",
          render: (_, record) => {
            const enabled = record.periodic_task_details?.enabled;
            return (
              <Space size={8}>
                <Switch
                  checked={enabled}
                  onChange={(checked) => handleSwitchSchedular(record, checked)}
                  size="small"
                />
                <Text type="secondary" style={{ fontSize: 12 }}>
                  {enabled ? "Enabled" : "Paused"}
                </Text>
              </Space>
            );
          },
        },
        {
          title: "Actions",
          key: "actions",
          width: 140,
          render: (_, record) => (
            <Space size={4}>
              <Tooltip title="Run now">
                <Button
                  type="text"
                  size="small"
                  icon={
                    loading[record.user_task_id] ? (
                      <LoadingOutlined />
                    ) : (
                      <PlayCircleOutlined
                        style={{ color: token.colorPrimary }}
                      />
                    )
                  }
                  onClick={() =>
                    handleRun(record.project?.id, record.user_task_id)
                  }
                  disabled={loading[record.user_task_id]}
                />
              </Tooltip>
              <Tooltip title="Run history">
                <Button
                  type="text"
                  size="small"
                  icon={<HistoryOutlined />}
                  onClick={() => goToRunHistory(record.user_task_id)}
                />
              </Tooltip>
              <Tooltip title="Edit">
                <Button
                  type="text"
                  size="small"
                  icon={<EditOutlined />}
                  onClick={() => onRowClick(record.user_task_id)}
                />
              </Tooltip>
              <Tooltip title="Delete">
                <Button
                  type="text"
                  size="small"
                  danger
                  icon={<DeleteOutlined />}
                  onClick={() => handleDelete(record)}
                />
              </Tooltip>
            </Space>
          ),
        },
      ],
      [token, loading, onRowClick]
    );

    return (
      <Table
        columns={columns}
        dataSource={data}
        rowKey="user_task_id"
        loading={tableLoading}
        size="middle"
        pagination={false}
        locale={{
          emptyText: (
            <Empty
              image={Empty.PRESENTED_IMAGE_SIMPLE}
              description="No jobs created yet"
            />
          ),
        }}
      />
    );
  }
);

JobListTable.displayName = "JobListTable";

JobListTable.propTypes = {
  data: PropTypes.array,
  onRowClick: PropTypes.func,
  setIsDeleteModalOpen: PropTypes.func,
  setDelTaskDetail: PropTypes.func,
  tableLoading: PropTypes.bool,
  onToggleSuccess: PropTypes.func,
};

export { JobListTable };
