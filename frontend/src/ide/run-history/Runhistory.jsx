import { useEffect, useState, useMemo, useCallback } from "react";
import {
  Select,
  Table,
  Tag,
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
} from "@ant-design/icons";

import { useAxiosPrivate } from "../../service/axios-service";
import { orgStore } from "../../store/org-store";
import { useNotificationService } from "../../service/notification-service";
import { runHistoryTagColor } from "../../common/constants";
import { usePagination } from "../../widgets/hooks/usePagination";
import { getTooltipText } from "../../common/helpers";
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
  });
  const [envInfo, setEnvInfo] = useState({
    env_type: "",
    job_name: "",
    id: "",
  });
  const [loading, setLoading] = useState(false);
  const { selectedOrgId } = orgStore();
  const { notify } = useNotificationService();

  /* ─── API calls ─── */
  const getRunHistoryList = async (
    Id,
    page = currentPage,
    limit = pageSize
  ) => {
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
  };

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
        scheduledObj[el.user_task_id] = getTooltipText(
          el.periodic_task_details[el.task_type],
          el.task_type
        );
        return { label: el.task_name, value: el.user_task_id };
      });
      setJobSchedule(scheduledObj);
      setJobListItems(jobIds);
      if (jobIds.length) {
        setFilterQuery((prev) => ({ ...prev, job: jobIds[0].value }));
        getRunHistoryList(jobIds[0].value);
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

  /* ─── client-side status filter ─── */
  useEffect(() => {
    if (filterQueries.status) {
      setJobHistoryData(
        backUpData.filter((el) => el.status === filterQueries.status)
      );
    } else {
      setJobHistoryData(backUpData);
    }
  }, [filterQueries.status, backUpData]);

  /* ─── auto-expand failed rows ─── */
  useEffect(() => {
    const failedIds = (JobHistoryData || [])
      .filter((r) => r.status === "FAILURE" && r.error_message)
      .map((r) => r.id);
    setExpandedRowKeys(failedIds);
  }, [JobHistoryData]);

  /* ─── handlers ─── */
  const handleJobChange = useCallback((value) => {
    setFilterQuery({ status: "", job: value });
    getRunHistoryList(value);
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
        title: "Source",
        key: "source",
        width: 200,
        render: (_, record) => {
          const isQuickDeploy = record.kwargs?.source === "quick_deploy";
          if (!isQuickDeploy) {
            return (
              <Typography.Text type="secondary">Scheduled</Typography.Text>
            );
          }
          const models = record.kwargs?.models_override || [];
          return (
            <Space size={4} wrap>
              <Tag color="blue">Quick Deploy</Tag>
              {models.length > 0 && (
                <Typography.Text type="secondary">
                  {models.join(", ")}
                </Typography.Text>
              )}
            </Space>
          );
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
        render: (text) => (
          <Typography.Text>{text || "Not started yet"}</Typography.Text>
        ),
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

  /* ─── empty text ─── */
  const emptyDescription = useMemo(() => {
    if (!jobListItems.length) return "No jobs created yet";
    if (!filterQueries.job) return "Select a job to view run history";
    if (filterQueries.status) return "No matching runs found";
    return "No run history available";
  }, [jobListItems.length, filterQueries.job, filterQueries.status]);

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
          expandable={{
            expandedRowRender: (record) => (
              <div className="runhistory-error-row">
                <Typography.Text type="danger">
                  {record.error_message}
                </Typography.Text>
              </div>
            ),
            rowExpandable: (record) =>
              record.status === "FAILURE" && !!record.error_message,
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
