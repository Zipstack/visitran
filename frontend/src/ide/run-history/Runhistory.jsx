import { useEffect, useState, useMemo, useCallback } from "react";
import {
  Alert,
  Select,
  Table,
  Tag,
  theme,
  Typography,
  Empty,
  Button,
  Space,
  Tooltip,
} from "antd";
import {
  ReloadOutlined,
  CalendarOutlined,
  DatabaseOutlined,
  CheckCircleFilled,
  CloseCircleFilled,
  ClockCircleOutlined,
  SyncOutlined,
} from "@ant-design/icons";
import { useSearchParams } from "react-router-dom";

import { useAxiosPrivate } from "../../service/axios-service";
import { orgStore } from "../../store/org-store";
import { useNotificationService } from "../../service/notification-service";
import { runHistoryTagColor } from "../../common/constants";
import { usePagination } from "../../widgets/hooks/usePagination";
import {
  getTooltipText,
  getRelativeTime,
  formatDateTime,
} from "../../common/helpers";
import "./RunHistory.css";

/* ─── Parse duration string to milliseconds for sorting ─── */
const parseDurationMs = (duration) => {
  if (!duration) return 0;
  const parts = duration.split(":");
  if (parts.length !== 3) return 0;
  const hours = parseInt(parts[0], 10);
  const mins = parseInt(parts[1], 10);
  const secs = parseFloat(parts[2]);
  const h = hours * 3600;
  const m = mins * 60;
  return (h + m + secs) * 1000;
};

/* ─── Duration formatter: "HH:MM:SS.sss" → human-readable ─── */
const formatDuration = (duration) => {
  if (!duration) return "—";
  const parts = duration.split(":");
  if (parts.length !== 3) return duration;
  const hours = parseInt(parts[0], 10);
  const mins = parseInt(parts[1], 10);
  const secs = parseFloat(parts[2]);
  if (hours > 0) return `${hours}h ${mins}m ${Math.round(secs)}s`;
  if (mins > 0) return `${mins}m ${secs.toFixed(1)}s`;
  if (secs >= 1) return `${secs.toFixed(1)}s`;
  return `${Math.round(secs * 1000)}ms`;
};

const STATUS_OPTIONS = [
  { label: "Failed", value: "FAILURE" },
  { label: "Success", value: "SUCCESS" },
  { label: "Retry", value: "RETRY" },
  { label: "Started", value: "STARTED" },
  { label: "Revoked", value: "REVOKED" },
];

const getRunTriggerScope = (row) => {
  const kw = row?.kwargs || {};
  const legacyQuick = kw.source === "quick_deploy";
  const models = kw.models_override || [];
  const trigger = kw.trigger || (legacyQuick ? "manual" : "scheduled");
  const scope =
    kw.scope || (models.length > 0 || legacyQuick ? "model" : "job");
  return { trigger, scope, models };
};

const Runhistory = () => {
  const axios = useAxiosPrivate();
  const {
    currentPage,
    pageSize,
    totalCount,
    setTotalCount,
    setCurrentPage,
    setPageSize,
  } = usePagination();
  const [jobListItems, setJobListItems] = useState([]);
  const [backUpData, setBackUpData] = useState([]);
  const [JobHistoryData, setJobHistoryData] = useState([]);
  const [jobSchedule, setJobSchedule] = useState({});
  const [expandedRowKeys, setExpandedRowKeys] = useState([]);
  const [filterQueries, setFilterQuery] = useState({
    status: "",
    job: "",
    trigger: "",
    scope: "",
  });

  const [envInfo, setEnvInfo] = useState({
    env_type: "",
    job_name: "",
    id: "",
  });
  const [loading, setLoading] = useState(false);
  const { selectedOrgId } = orgStore();
  const { token } = theme.useToken();
  const { notify } = useNotificationService();
  const [searchParams, setSearchParams] = useSearchParams();

  /* ─── API calls ─── */
  const getRunHistoryList = useCallback(
    async (Id, page = currentPage, limit = pageSize) => {
      setLoading(true);
      try {
        const res = await axios({
          method: "GET",
          url: `/api/v1/visitran/${
            selectedOrgId || "default_org"
          }/project/_all/jobs/run-history/${Id}`,
          params: { page, limit },
        });
        const { page_items, total_items, current_page } = res.data.data;
        setTotalCount(total_items);
        setCurrentPage(current_page);
        const { env_type, job_name, run_history, id } = page_items;
        setEnvInfo({ env_type, job_name, id });
        setJobHistoryData(run_history);
        setBackUpData(run_history);
      } catch (error) {
        notify({ error });
      } finally {
        setLoading(false);
      }
    },
    [axios, selectedOrgId, currentPage, pageSize, notify]
  );

  const getJobList = async () => {
    setLoading(true);
    try {
      const res = await axios({
        method: "GET",
        url: `/api/v1/visitran/${
          selectedOrgId || "default_org"
        }/project/_all/jobs/list-periodic-tasks`,
      });
      const { page_items } = res.data.data;
      const scheduledObj = {};
      const jobIds = page_items.map((el) => {
        const taskDetails = el.periodic_task_details?.[el.task_type];
        if (taskDetails) {
          scheduledObj[el.user_task_id] = getTooltipText(
            taskDetails,
            el.task_type
          );
        }
        return { label: el.task_name, value: el.user_task_id };
      });
      setJobSchedule(scheduledObj);
      setJobListItems(jobIds);
      if (jobIds.length) {
        const taskFromUrl = searchParams.get("task");
        const taskFromUrlNum = taskFromUrl ? Number(taskFromUrl) : NaN;
        const matchedFromUrl = !Number.isNaN(taskFromUrlNum)
          ? jobIds.find((j) => j.value === taskFromUrlNum)
          : null;
        const initial = matchedFromUrl?.value ?? jobIds[0].value;
        setFilterQuery((prev) => ({ ...prev, job: initial }));
        getRunHistoryList(initial);
      }
    } catch (error) {
      console.error("Failed to load jobs", error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    getJobList();
  }, []);

  /* ─── client-side status + trigger + scope filters ─── */
  useEffect(() => {
    let filtered = backUpData;
    if (filterQueries.status) {
      filtered = filtered.filter((el) => el.status === filterQueries.status);
    }
    if (filterQueries.trigger) {
      filtered = filtered.filter(
        (el) => getRunTriggerScope(el).trigger === filterQueries.trigger
      );
    }
    if (filterQueries.scope) {
      filtered = filtered.filter(
        (el) => getRunTriggerScope(el).scope === filterQueries.scope
      );
    }
    setJobHistoryData(filtered);
  }, [
    filterQueries.status,
    filterQueries.trigger,
    filterQueries.scope,
    backUpData,
  ]);

  /* ─── auto-expand on fresh data load ─── */
  useEffect(() => {
    const ids = [];
    const fromDeepLink = searchParams.has("task");
    if (fromDeepLink && backUpData.length > 0) {
      ids.push(backUpData[0].id);
    }
    (backUpData || [])
      .filter((r) => r.status === "FAILURE" && r.error_message)
      .forEach((r) => {
        if (!ids.includes(r.id)) ids.push(r.id);
      });
    setExpandedRowKeys(ids);
  }, [backUpData, searchParams]);

  /* ─── handlers ─── */
  const handleJobChange = useCallback(
    (value) => {
      setFilterQuery({ status: "", job: value, trigger: "", scope: "" });
      getRunHistoryList(value);
      setSearchParams(
        (prev) => {
          const next = new URLSearchParams(prev);
          if (value) next.set("task", String(value));
          else next.delete("task");
          return next;
        },
        { replace: true }
      );
    },
    [setSearchParams, getRunHistoryList]
  );

  const handleTriggerChange = useCallback((value) => {
    setFilterQuery((prev) => ({ ...prev, trigger: value || "" }));
  }, []);

  const handleScopeChange = useCallback((value) => {
    setFilterQuery((prev) => ({ ...prev, scope: value || "" }));
  }, []);

  const handleStatusChange = useCallback((value) => {
    setFilterQuery((prev) => ({ ...prev, status: value || "" }));
  }, []);

  const handleRefresh = useCallback(() => {
    if (filterQueries.job) {
      getRunHistoryList(filterQueries.job);
    }
  }, [filterQueries.job]);

  const handlePagination = useCallback(
    (newPage, newPageSize) => {
      if (currentPage !== newPage || pageSize !== newPageSize) {
        setCurrentPage(newPage);
        setPageSize(newPageSize);
        getRunHistoryList(envInfo.id, newPage, newPageSize);
      }
    },
    [currentPage, pageSize, envInfo.id]
  );

  const handleExpand = useCallback((expanded, record) => {
    setExpandedRowKeys((prev) =>
      expanded ? [...prev, record.id] : prev.filter((k) => k !== record.id)
    );
  }, []);

  /* ─── table columns ─── */
  const columns = useMemo(
    () => [
      {
        title: "Status",
        dataIndex: "status",
        key: "status",
        width: 120,
        render: (status) => (
          <Tag color={runHistoryTagColor[status]}>
            {status === "FAILURE" ? "FAILED" : status}
          </Tag>
        ),
      },
      {
        title: "Trigger",
        key: "trigger",
        width: 120,
        render: (_, record) => {
          const { trigger } = getRunTriggerScope(record);
          return trigger === "manual" ? (
            <Tag color="blue">Manual</Tag>
          ) : (
            <Tag>Scheduled</Tag>
          );
        },
      },
      {
        title: "Scope",
        key: "scope",
        width: 220,
        render: (_, record) => {
          const { scope, models } = getRunTriggerScope(record);
          if (scope === "model") {
            return (
              <Space size={4} wrap>
                <Tag color="purple">Single model</Tag>
                {models.length > 0 && (
                  <Typography.Text type="secondary">
                    {models.join(", ")}
                  </Typography.Text>
                )}
              </Space>
            );
          }
          return <Typography.Text type="secondary">Full job</Typography.Text>;
        },
      },
      {
        title: "Triggered",
        dataIndex: "start_time",
        key: "start_time",
        sorter: (a, b) => {
          if (!a.start_time) return -1;
          if (!b.start_time) return 1;
          return new Date(a.start_time) - new Date(b.start_time);
        },
        defaultSortOrder: "descend",
        render: (text) => {
          if (!text) {
            return (
              <Typography.Text type="secondary">
                Not started yet
              </Typography.Text>
            );
          }
          return (
            <Tooltip title={new Date(text).toISOString()}>
              <Space direction="vertical" size={0}>
                <Typography.Text>{formatDateTime(text)}</Typography.Text>
                <Typography.Text type="secondary" style={{ fontSize: 11 }}>
                  {getRelativeTime(text)}
                </Typography.Text>
              </Space>
            </Tooltip>
          );
        },
      },
      {
        title: "Duration",
        dataIndex: "duration",
        key: "duration",
        width: 140,
        sorter: (a, b) => {
          if (!a.duration) return -1;
          if (!b.duration) return 1;
          return parseDurationMs(a.duration) - parseDurationMs(b.duration);
        },
        render: (duration) => (
          <Typography.Text>{formatDuration(duration)}</Typography.Text>
        ),
      },
    ],
    []
  );

  const STATUS_META = useMemo(
    () => ({
      SUCCESS: {
        icon: <CheckCircleFilled />,
        label: "Succeeded",
        color: token.colorSuccess,
        bg: token.colorSuccessBg,
      },
      FAILURE: {
        icon: <CloseCircleFilled />,
        label: "Failed",
        color: token.colorError,
        bg: token.colorErrorBg,
      },
      STARTED: {
        icon: <SyncOutlined spin />,
        label: "Running",
        color: token.colorInfo,
        bg: token.colorInfoBg,
      },
      RUNNING: {
        icon: <SyncOutlined spin />,
        label: "Running",
        color: token.colorInfo,
        bg: token.colorInfoBg,
      },
      RETRY: {
        icon: <SyncOutlined spin />,
        label: "Retrying",
        color: token.colorWarning,
        bg: token.colorWarningBg,
      },
      REVOKED: {
        icon: <ClockCircleOutlined />,
        label: "Revoked",
        color: token.colorTextSecondary,
        bg: token.colorFillQuaternary,
      },
      PENDING: {
        icon: <ClockCircleOutlined />,
        label: "Pending",
        color: token.colorTextSecondary,
        bg: token.colorFillQuaternary,
      },
    }),
    [token]
  );

  /* ─── empty text ─── */
  const emptyDescription = useMemo(() => {
    if (!jobListItems.length) return "No jobs created yet";
    if (!filterQueries.job) return "Select a job to view run history";
    if (filterQueries.status || filterQueries.trigger || filterQueries.scope)
      return "No matching runs found";
    return "No run history available";
  }, [
    jobListItems.length,
    filterQueries.job,
    filterQueries.status,
    filterQueries.trigger,
    filterQueries.scope,
  ]);

  return (
    <div className="runhistory-container">
      {/* ─── Page Title ─── */}
      <Typography.Text className="font-size-16 runhistory-title">
        Run History
      </Typography.Text>

      {/* ─── Filters ─── */}
      <div className="runhistory-filters">
        <Space>
          <Select
            showSearch
            placeholder="Select job"
            optionFilterProp="label"
            className="runhistory-job-select"
            onChange={handleJobChange}
            options={jobListItems}
            value={filterQueries.job || undefined}
          />
          <Select
            allowClear
            placeholder="Status"
            className="runhistory-status-select"
            onChange={handleStatusChange}
            options={STATUS_OPTIONS}
            value={filterQueries.status || undefined}
          />
          <Select
            allowClear
            placeholder="Trigger"
            className="runhistory-status-select"
            onChange={handleTriggerChange}
            options={[
              { label: "Manual", value: "manual" },
              { label: "Scheduled", value: "scheduled" },
            ]}
            value={filterQueries.trigger || undefined}
          />
          <Select
            allowClear
            placeholder="Scope"
            className="runhistory-status-select"
            onChange={handleScopeChange}
            options={[
              { label: "Full job", value: "job" },
              { label: "Single model", value: "model" },
            ]}
            value={filterQueries.scope || undefined}
          />
        </Space>
        <Button
          icon={<ReloadOutlined spin={loading} />}
          onClick={handleRefresh}
          disabled={loading}
        />
      </div>

      {/* ─── Job Info Banner ─── */}
      {filterQueries.job && envInfo.job_name && (
        <div className="runhistory-job-info">
          <Typography.Text strong>
            {envInfo.job_name} #{envInfo.id}
          </Typography.Text>
          <Space>
            {envInfo.env_type && (
              <Tag color="geekblue" icon={<DatabaseOutlined />}>
                {envInfo.env_type}
              </Tag>
            )}
            {jobSchedule[envInfo.id] && (
              <Tooltip title={jobSchedule[envInfo.id]}>
                <Tag color="purple" icon={<CalendarOutlined />}>
                  SCHEDULED
                </Tag>
              </Tooltip>
            )}
          </Space>
        </div>
      )}

      {/* ─── History Table ─── */}
      <div className="runhistory-table-container">
        <Table
          columns={columns}
          dataSource={JobHistoryData}
          rowKey="id"
          loading={loading}
          size="small"
          bordered
          rowClassName={(record) =>
            expandedRowKeys.includes(record.id) ? "runhistory-row-expanded" : ""
          }
          onRow={(record) =>
            expandedRowKeys.includes(record.id)
              ? {
                  style: {
                    boxShadow: `inset 3px 0 0 0 ${token.colorError}`,
                  },
                }
              : {}
          }
          expandable={{
            expandedRowRender: (record) => {
              const meta = STATUS_META[record.status] || STATUS_META.PENDING;
              const { scope, models } = getRunTriggerScope(record);
              const isFailure = record.status === "FAILURE";
              return (
                <div
                  style={{
                    borderLeft: `3px solid ${meta.color}`,
                    padding: "10px 12px",
                    background: meta.bg,
                  }}
                >
                  <div
                    style={{
                      display: "flex",
                      alignItems: "center",
                      gap: 8,
                      marginBottom: 8,
                    }}
                  >
                    <span style={{ color: meta.color }}>{meta.icon}</span>
                    <Typography.Text strong style={{ color: meta.color }}>
                      {meta.label}
                    </Typography.Text>
                    {record.start_time && (
                      <Tooltip
                        title={new Date(record.start_time).toISOString()}
                      >
                        <Typography.Text
                          type="secondary"
                          style={{ fontSize: 12 }}
                        >
                          · {formatDateTime(record.start_time)} (
                          {getRelativeTime(record.start_time)})
                        </Typography.Text>
                      </Tooltip>
                    )}
                    {record.duration && (
                      <Typography.Text
                        type="secondary"
                        style={{ fontSize: 12 }}
                      >
                        · {formatDuration(record.duration)}
                      </Typography.Text>
                    )}
                  </div>
                  <Space
                    size={8}
                    wrap
                    style={{ marginBottom: isFailure ? 8 : 0 }}
                  >
                    <Tag color={scope === "model" ? "purple" : "default"}>
                      {scope === "model" ? "Single model" : "Full job"}
                    </Tag>
                    <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                      {models.length > 0
                        ? `Models attempted: ${models.join(", ")}`
                        : "No model configuration recorded for this run."}
                    </Typography.Text>
                  </Space>
                  {isFailure && record.error_message && (
                    <Alert
                      type="error"
                      showIcon={false}
                      message={
                        <pre
                          style={{
                            margin: 0,
                            maxHeight: 240,
                            overflow: "auto",
                            whiteSpace: "pre-wrap",
                            wordBreak: "break-word",
                            fontFamily: token.fontFamilyCode,
                            fontSize: 12,
                          }}
                        >
                          {record.error_message}
                        </pre>
                      }
                    />
                  )}
                </div>
              );
            },
            rowExpandable: (record) =>
              ["SUCCESS", "FAILURE", "RETRY", "REVOKED"].includes(
                record.status
              ),
            expandedRowKeys,
            onExpand: handleExpand,
          }}
          locale={{
            emptyText: (
              <Empty
                image={Empty.PRESENTED_IMAGE_SIMPLE}
                description={emptyDescription}
              />
            ),
          }}
          pagination={{
            current: currentPage,
            pageSize,
            total: Math.min(totalCount, 1000),
            showTotal: (total, range) => `${range[0]}-${range[1]} of ${total}`,
            showSizeChanger: true,
            onChange: handlePagination,
          }}
        />
      </div>
    </div>
  );
};

export { Runhistory };
