import { useEffect, useState, useMemo, useCallback, useRef } from "react";
import {
  Select,
  Table,
  Tag,
  theme,
  Typography,
  Empty,
  Button,
  Space,
  Tooltip,
  Input,
  Card,
  Badge,
  Row,
  Col,
  Timeline,
  DatePicker,
} from "antd";
import {
  ReloadOutlined,
  CalendarOutlined,
  CheckCircleFilled,
  CloseCircleFilled,
  ClockCircleOutlined,
  SyncOutlined,
  SearchOutlined,
  CopyOutlined,
  EyeOutlined,
  RedoOutlined,
  ArrowUpOutlined,
  ArrowDownOutlined,
  UserOutlined,
  EditOutlined,
  UpOutlined,
  DownOutlined,
  MinusOutlined,
} from "@ant-design/icons";
import { useSearchParams, useNavigate } from "react-router-dom";

import { useAxiosPrivate } from "../../service/axios-service";
import { orgStore } from "../../store/org-store";
import { useNotificationService } from "../../service/notification-service";
import { runHistoryTagColor } from "../../common/constants";
import { usePagination } from "../../widgets/hooks/usePagination";
import { getTooltipText, getRelativeTime, formatDateTime } from "../../common/helpers";
import "./RunHistory.css";

const { Text, Title } = Typography;
const { RangePicker } = DatePicker;

/* ── Duration helpers ── */
const formatDurationMs = (ms) => {
  if (!ms && ms !== 0) return "—";
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  const m = Math.floor(ms / 60000);
  const s = ((ms % 60000) / 1000).toFixed(0);
  return `${m}m ${s}s`;
};

const parseDurationMs = (d) => {
  if (!d) return 0;
  if (typeof d === "number") return d;
  const p = d.split(":");
  if (p.length !== 3) return 0;
  return (parseInt(p[0], 10) * 3600 + parseInt(p[1], 10) * 60 + parseFloat(p[2])) * 1000;
};

/* ── Sparkline SVG — plots real duration data points ── */
const Sparkline = ({ color = "#3b82f6", data = [] }) => {
  if (!data || data.length < 2) {
    // Fallback — flat line
    return (
      <svg style={{ width: "100%", height: 28, display: "block" }} viewBox="0 0 100 28" preserveAspectRatio="none">
        <line x1="0" y1="14" x2="100" y2="14" stroke={color} strokeWidth="1.5" opacity="0.3" />
      </svg>
    );
  }
  const max = Math.max(...data);
  const min = Math.min(...data);
  const range = max - min || 1;
  const points = data.map((v, i) => {
    const x = (i / (data.length - 1)) * 100;
    const y = 26 - ((v - min) / range) * 24; // 2px top padding, 26px range
    return `${x},${y}`;
  }).join(" ");
  return (
    <svg style={{ width: "100%", height: 28, display: "block" }} viewBox="0 0 100 28" preserveAspectRatio="none">
      <polyline fill="none" stroke={color} strokeWidth="1.5" points={points} />
    </svg>
  );
};

/* ── StatCard ── */
const StatCard = ({ label, icon, value, valueColor, subtext, spark }) => {
  return (
    <Card size="small" styles={{ body: { padding: 14 } }}>
      <Space size={4} direction="vertical" style={{ width: "100%" }}>
        <Text type="secondary" style={{ fontSize: 11, fontWeight: 600, letterSpacing: 0.4, textTransform: "uppercase" }}>
          {icon}{icon ? " " : ""}{label}
        </Text>
        <div style={{ fontSize: 22, fontWeight: 700, lineHeight: 1, color: valueColor, fontVariantNumeric: "tabular-nums" }}>
          {value}
        </div>
        {spark}
        {subtext && <Text style={{ fontSize: 11, display: "block" }}>{subtext}</Text>}
      </Space>
    </Card>
  );
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
  const navigate = useNavigate();
  const { token } = theme.useToken();
  const { notify } = useNotificationService();
  const { selectedOrgId } = orgStore();
  const [searchParams, setSearchParams] = useSearchParams();
  const { currentPage, pageSize, totalCount, setTotalCount, setCurrentPage, setPageSize } = usePagination();

  const [jobListItems, setJobListItems] = useState([]);
  const [jobHistoryData, setJobHistoryData] = useState([]);
  const [jobSchedule, setJobSchedule] = useState({});
  const [expandedRowKeys, setExpandedRowKeys] = useState([]);
  const [loading, setLoading] = useState(false);
  const [stats, setStats] = useState(null);
  const [statsLoading, setStatsLoading] = useState(false);
  const [filters, setFilters] = useState({ job: "", status: "", trigger: "", search: "" });
  const [datePreset, setDatePreset] = useState("24h");
  const [customDateRange, setCustomDateRange] = useState(null);
  const [showCustomDate, setShowCustomDate] = useState(false);
  const [envInfo, setEnvInfo] = useState({ env_type: "", job_name: "", id: "", project_id: "" });
  const deepLinkConsumed = useRef(false);
  const orgId = selectedOrgId || "default_org";

  /* ── APIs ── */
  const fetchStats = useCallback(async (taskId) => {
    setStatsLoading(true);
    try { const res = await axios.get(`/api/v1/visitran/${orgId}/project/_all/jobs/run-stats/${taskId}`); setStats(res.data.data); }
    catch { setStats(null); }
    finally { setStatsLoading(false); }
  }, [axios, orgId]);

  const fetchHistory = useCallback(async (taskId, page = 1, limit = pageSize, f = filters) => {
    setLoading(true);
    try {
      const params = { page, limit };
      if (f.status) params.status = f.status;
      if (f.trigger) params.trigger = f.trigger;
      if (f.search) params.search = f.search;
      // Date filter — preset or custom range
      if (datePreset === "custom" && customDateRange?.[0]) {
        params.date_from = customDateRange[0].toISOString();
        if (customDateRange[1]) params.date_to = customDateRange[1].toISOString();
      } else if (datePreset && datePreset !== "all") {
        const now = new Date();
        const presetMs = { "24h": 86400000, "7d": 604800000, "30d": 2592000000 };
        if (presetMs[datePreset]) params.date_from = new Date(now - presetMs[datePreset]).toISOString();
      }
      const res = await axios.get(`/api/v1/visitran/${orgId}/project/_all/jobs/run-history/${taskId}`, { params });
      const { page_items, total_items, current_page } = res.data.data;
      setTotalCount(total_items);
      setCurrentPage(current_page);
      setEnvInfo({ env_type: page_items.env_type, job_name: page_items.job_name, id: page_items.id, project_id: page_items.project_id });
      setJobHistoryData(page_items.run_history || []);
    } catch (error) { notify({ error }); }
    finally { setLoading(false); }
  }, [axios, orgId, pageSize, notify, datePreset, customDateRange]);

  const fetchJobs = async () => {
    try {
      const res = await axios.get(`/api/v1/visitran/${orgId}/project/_all/jobs/list-periodic-tasks`);
      const { page_items } = res.data.data;
      const schedObj = {};
      const jobs = page_items.map((el) => {
        const td = el.periodic_task_details?.[el.task_type];
        if (td) schedObj[el.user_task_id] = getTooltipText(td, el.task_type);
        return { label: el.task_name, value: el.user_task_id };
      });
      setJobSchedule(schedObj);
      setJobListItems(jobs);
      if (jobs.length) {
        const fromUrl = searchParams.get("task");
        const matched = fromUrl ? jobs.find((j) => j.value === Number(fromUrl)) : null;
        setFilters((p) => ({ ...p, job: matched?.value ?? jobs[0].value }));
      }
    } catch (error) { console.error("Failed to load jobs", error); }
  };

  useEffect(() => { fetchJobs(); }, []);
  useEffect(() => { if (!filters.job) return; fetchHistory(filters.job, 1, pageSize, filters); fetchStats(filters.job); }, [filters.job, filters.status, filters.trigger, filters.search, datePreset, customDateRange]);

  // Don't auto-expand — keep collapsed on load
  useEffect(() => { setExpandedRowKeys([]); }, [jobHistoryData]);

  const handleFilterChange = (key, value) => {
    setFilters((p) => ({ ...p, [key]: value || "" }));
    if (key === "job") setSearchParams((prev) => { const next = new URLSearchParams(prev); if (value) next.set("task", String(value)); else next.delete("task"); return next; }, { replace: true });
  };
  const handleRefresh = () => { if (filters.job) { fetchHistory(filters.job, currentPage, pageSize, filters); fetchStats(filters.job); } };
  const handleRetry = async (taskId) => {
    try { await axios.post(`/api/v1/visitran/${orgId}/project/_all/jobs/trigger-periodic-task/${taskId}`, {}, { headers: { "X-CSRFToken": document.cookie.match(/csrftoken=([^;]+)/)?.[1] } }); notify({ type: "success", message: "Job submitted" }); setTimeout(handleRefresh, 2000); }
    catch (error) { notify({ error }); }
  };
  const handleCopyError = (text) => { navigator.clipboard.writeText(text); notify({ type: "success", message: "Copied to clipboard" }); };

  const activeFilterCount = [filters.status, filters.trigger, filters.search, datePreset !== "24h" ? datePreset : null].filter(Boolean).length;

  /* ── Table columns ── */
  const columns = useMemo(() => [
    {
      title: "Run", dataIndex: "run_number", key: "run_number", width: 70,
      render: (n) => <Text strong style={{ fontFamily: token.fontFamilyCode || "monospace" }}>#{n || "—"}</Text>,
    },
    {
      title: "Status", dataIndex: "status", key: "status", width: 110,
      render: (s) => {
        if (s === "FAILURE") return <Tag icon={<CloseCircleFilled />} color="error">Failed</Tag>;
        if (s === "SUCCESS") return <Tag icon={<CheckCircleFilled />} color="success">Success</Tag>;
        if (s === "STARTED" || s === "RUNNING") return <Tag icon={<SyncOutlined spin />} color="processing">Running</Tag>;
        return <Tag>{s}</Tag>;
      },
    },
    {
      title: "Trigger", key: "trigger", width: 170,
      render: (_, r) => {
        const trigger = r.trigger || r.kwargs?.trigger || "scheduled";
        const user = r.triggered_by;
        return (
          <Space size={6}>
            <span className="rh-trigger-icon"><UserOutlined style={{ fontSize: 11 }} /></span>
            <Text style={{ fontSize: 13 }}>{trigger === "manual" ? "Manual" : "Scheduled"}{user ? ` · ${user.username}` : ""}</Text>
          </Space>
        );
      },
    },
    {
      title: "Scope", key: "scope", width: 150,
      render: (_, r) => {
        const scope = r.scope || "job";
        const count = r.model_count || r.result?.total || 0;
        const models = r.kwargs?.models_override || [];
        return (
          <Space direction="vertical" size={0}>
            <Text style={{ fontSize: 13 }}>{scope === "model" ? models.join(", ") : "Full job"}</Text>
            <Text type="secondary" style={{ fontSize: 11, fontFamily: token.fontFamilyCode || "monospace" }}>{count} models</Text>
          </Space>
        );
      },
    },
    {
      title: "Changes", key: "changes", width: 200,
      render: (_, r) => {
        if (r.status !== "SUCCESS" || !r.result) return <Text type="secondary">—</Text>;
        const added = r.result?.rows_added ?? null;
        const modified = r.result?.rows_modified ?? null;
        const deleted = r.result?.rows_deleted ?? null;
        if (added === null && modified === null && deleted === null) return <Text type="secondary">—</Text>;
        return (
          <Space size={8}>
            {added !== null && <Text style={{ color: token.colorSuccess, fontSize: 12, fontFamily: token.fontFamilyCode || "monospace" }}>+ +{added.toLocaleString()}</Text>}
            {modified !== null && <Text style={{ color: token.colorPrimary, fontSize: 12, fontFamily: token.fontFamilyCode || "monospace" }}>✎ ~{modified.toLocaleString()}</Text>}
            {deleted !== null && <Text style={{ color: token.colorError, fontSize: 12, fontFamily: token.fontFamilyCode || "monospace" }}>⊟ −{deleted.toLocaleString()}</Text>}
          </Space>
        );
      },
    },
    {
      title: "Triggered", dataIndex: "start_time", key: "start_time",
      sorter: (a, b) => new Date(a.start_time || 0) - new Date(b.start_time || 0),
      defaultSortOrder: "descend",
      render: (t) => t ? (
        <Space direction="vertical" size={0}>
          <Text style={{ fontSize: 13, fontWeight: 500 }}>{formatDateTime(t)}</Text>
          <Text type="secondary" style={{ fontSize: 11 }}>{getRelativeTime(t)}</Text>
        </Space>
      ) : <Text type="secondary">Not started</Text>,
    },
    {
      title: "Duration", key: "duration", width: 130,
      sorter: (a, b) => (a.duration_ms || 0) - (b.duration_ms || 0),
      render: (_, r) => {
        const ms = r.duration_ms || parseDurationMs(r.duration);
        const isFail = r.status === "FAILURE";
        const pct = stats?.expected_duration_ms ? Math.min((ms / stats.expected_duration_ms) * 100, 100) : (isFail ? 70 : 35);
        return (
          <div>
            <Text style={{ fontSize: 13, fontFamily: token.fontFamilyCode || "monospace" }}>{r.duration || formatDurationMs(ms)}</Text>
            <div className="rh-dur-bar">
              <div className={`rh-dur-bar-fill ${isFail ? "fail" : "ok"}`} style={{ width: `${pct}%` }} />
            </div>
          </div>
        );
      },
    },
    {
      title: "", key: "actions", width: 50,
      render: () => <Tooltip title="Retry run"><Button type="text" size="small" icon={<RedoOutlined />} onClick={(e) => { e.stopPropagation(); handleRetry(envInfo.id); }} /></Tooltip>,
    },
  ], [stats, envInfo.id]);

  /* ── RunDetail (expanded row) ── */
  const RunDetail = ({ run }) => {
    const isFailure = run.status === "FAILURE";
    const isSuccess = run.status === "SUCCESS";
    const passed = run.result?.passed || 0;
    const failed = run.result?.failed || 0;
    const total = run.result?.total || 0;
    const skipped = run.skipped_count || Math.max(0, total - passed - failed);
    const failedModels = run.failed_models || [];
    const dur = run.duration || formatDurationMs(run.duration_ms);
    const expected = stats?.expected_duration_ms ? `~${formatDurationMs(stats.expected_duration_ms)}` : null;
    const errorModelName = failedModels.length > 0 ? failedModels[0] : null;
    const models = run.result?.models || [];

    // Aggregate row-level changes (from result when available, otherwise —)
    const totalRowsProcessed = run.result?.rows_processed ?? null;
    const totalAdded = run.result?.rows_added ?? null;
    const totalModified = run.result?.rows_modified ?? null;
    const totalDeleted = run.result?.rows_deleted ?? null;

    // Parse error into message + stack
    const errorLines = (run.error_message || "").split("\n");
    const errorMsg = errorLines[0] || "";
    const errorStack = errorLines.slice(1).join("\n");

    // Per-model table columns for success view
    const modelColumns = [
      {
        title: "Model", dataIndex: "name", key: "name",
        render: (name, m) => (
          <Space size={6}>
            {(m.end_status === "OK" || m.status === "success") ? <CheckCircleFilled style={{ color: token.colorSuccess, fontSize: 13 }} /> : <CloseCircleFilled style={{ color: token.colorError, fontSize: 13 }} />}
            <Text>{name}</Text>
            {m.type && <Tag style={{ fontSize: 10, lineHeight: "16px", padding: "0 4px" }}>{m.type}</Tag>}
          </Space>
        ),
      },
      { title: "Rows in", dataIndex: "rows_in", key: "rows_in", width: 90, align: "right", render: (v) => <Text style={{ fontFamily: token.fontFamilyCode || "monospace", fontSize: 12 }}>{v != null ? v.toLocaleString() : "—"}</Text> },
      { title: "Rows out", dataIndex: "rows_out", key: "rows_out", width: 90, align: "right", render: (v) => <Text style={{ fontFamily: token.fontFamilyCode || "monospace", fontSize: 12 }}>{v != null ? v.toLocaleString() : "—"}</Text> },
      { title: "Added", dataIndex: "rows_added", key: "rows_added", width: 90, align: "right", render: (v) => v != null ? <Text style={{ color: token.colorSuccess, fontFamily: token.fontFamilyCode || "monospace", fontSize: 12 }}>+ +{v.toLocaleString()}</Text> : <Text type="secondary">—</Text> },
      { title: "Modified", dataIndex: "rows_modified", key: "rows_modified", width: 90, align: "right", render: (v) => v != null ? <Text style={{ color: token.colorPrimary, fontFamily: token.fontFamilyCode || "monospace", fontSize: 12 }}>✎ ~{v.toLocaleString()}</Text> : <Text type="secondary">—</Text> },
      { title: "Deleted", dataIndex: "rows_deleted", key: "rows_deleted", width: 90, align: "right", render: (v) => v != null ? <Text style={{ color: token.colorError, fontFamily: token.fontFamilyCode || "monospace", fontSize: 12 }}>⊟ −{v.toLocaleString()}</Text> : <Text type="secondary">—</Text> },
      { title: "Duration", dataIndex: "duration_ms", key: "duration_ms", width: 90, align: "right", render: (v) => <Text style={{ fontFamily: token.fontFamilyCode || "monospace", fontSize: 12 }}>{v != null ? formatDurationMs(v) : "—"}</Text> },
    ];

    return (
      <Card size="small" styles={{ body: { padding: 16 } }}>
        {/* Header */}
        <Row justify="space-between" align="middle" style={{ marginBottom: 14 }}>
          <Col>
            <Space>
              <Text strong>Run #{run.run_number} details</Text>
              <Text type="secondary" style={{ fontSize: 12 }}>
                {isFailure ? `failed after ${passed} of ${total} models` : `${total} models built successfully`}
                {dur !== "—" && ` · ${dur} total runtime`}
              </Text>
            </Space>
          </Col>
          <Col>
            <Space>
              {isSuccess && envInfo.project_id && (
                <Button size="small" icon={<EyeOutlined />} onClick={() => navigate(`/ide/project/${envInfo.project_id}`)}>View lineage</Button>
              )}
              <Button type="primary" icon={<RedoOutlined />} size="small" onClick={() => handleRetry(envInfo.id)}>Re-run</Button>
            </Space>
          </Col>
        </Row>

        {/* Success banner */}
        {isSuccess && (
          <div style={{ padding: "10px 14px", background: token.colorSuccessBg, borderRadius: 6, marginBottom: 14, display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <Space>
              <CheckCircleFilled style={{ color: token.colorSuccess, fontSize: 16 }} />
              <div>
                <Text strong style={{ color: token.colorSuccess }}>All {total} models built successfully</Text>
                <br />
                <Text type="secondary" style={{ fontSize: 12 }}>
                  {totalRowsProcessed != null ? `${totalRowsProcessed.toLocaleString()} rows processed · ` : ""}{dur} total runtime
                </Text>
              </div>
            </Space>
            {(totalAdded != null || totalModified != null || totalDeleted != null) && (
              <Space size={12}>
                {totalAdded != null && <Text style={{ color: token.colorSuccess, fontFamily: token.fontFamilyCode || "monospace", fontSize: 12 }}>+ +{totalAdded.toLocaleString()}</Text>}
                {totalModified != null && <Text style={{ color: token.colorPrimary, fontFamily: token.fontFamilyCode || "monospace", fontSize: 12 }}>✎ ~{totalModified.toLocaleString()}</Text>}
                {totalDeleted != null && <Text style={{ color: token.colorError, fontFamily: token.fontFamilyCode || "monospace", fontSize: 12 }}>⊟ −{totalDeleted.toLocaleString()}</Text>}
              </Space>
            )}
          </div>
        )}

        {/* Stats grid — different for success vs failure */}
        {isSuccess ? (
          <>
            {/* Row stats cards for success */}
            <Row gutter={8} style={{ marginBottom: 14 }}>
              <Col span={6}>
                <Card size="small" styles={{ body: { padding: "8px 12px" } }}>
                  <div style={{ fontSize: 11, fontWeight: 600, textTransform: "uppercase", letterSpacing: 0.3, color: token.colorTextSecondary, marginBottom: 4 }}>ROWS PROCESSED</div>
                  <div style={{ fontSize: 18, fontWeight: 700 }}>{totalRowsProcessed != null ? totalRowsProcessed.toLocaleString() : "—"}</div>
                </Card>
              </Col>
              <Col span={6}>
                <Card size="small" styles={{ body: { padding: "8px 12px" } }}>
                  <div style={{ fontSize: 11, fontWeight: 600, textTransform: "uppercase", letterSpacing: 0.3, color: token.colorTextSecondary, marginBottom: 4 }}>+ ADDED</div>
                  <div style={{ fontSize: 18, fontWeight: 700, color: token.colorSuccess }}>{totalAdded != null ? `+${totalAdded.toLocaleString()}` : "—"}</div>
                </Card>
              </Col>
              <Col span={6}>
                <Card size="small" styles={{ body: { padding: "8px 12px" } }}>
                  <div style={{ fontSize: 11, fontWeight: 600, textTransform: "uppercase", letterSpacing: 0.3, color: token.colorTextSecondary, marginBottom: 4 }}>✎ MODIFIED</div>
                  <div style={{ fontSize: 18, fontWeight: 700, color: token.colorPrimary }}>{totalModified != null ? `~${totalModified.toLocaleString()}` : "—"}</div>
                </Card>
              </Col>
              <Col span={6}>
                <Card size="small" styles={{ body: { padding: "8px 12px" } }}>
                  <div style={{ fontSize: 11, fontWeight: 600, textTransform: "uppercase", letterSpacing: 0.3, color: token.colorTextSecondary, marginBottom: 4 }}>⊟ DELETED</div>
                  <div style={{ fontSize: 18, fontWeight: 700, color: token.colorError }}>{totalDeleted != null ? `−${totalDeleted.toLocaleString()}` : "—"}</div>
                </Card>
              </Col>
            </Row>

            {/* Per-model changes table */}
            {models.length > 0 && (
              <>
                <Text strong style={{ fontSize: 11, textTransform: "uppercase", letterSpacing: 0.4, display: "block", marginBottom: 8 }}>Per-model changes</Text>
                <Table
                  size="small"
                  dataSource={models.map((m, i) => ({ ...m, key: i }))}
                  columns={modelColumns}
                  pagination={false}
                  showHeader
                />
              </>
            )}
          </>
        ) : (
          <>
            {/* Failure stats grid */}
            <Row gutter={8} style={{ marginBottom: 14 }}>
              <Col span={6}>
                <Card size="small" styles={{ body: { padding: "8px 12px" } }}>
                  <Space><div style={{ fontSize: 18, fontWeight: 700, color: token.colorSuccess }}>{passed}</div><div><div style={{ fontSize: 12, fontWeight: 500 }}>Succeeded</div><Text type="secondary" style={{ fontSize: 11 }}>of {total} total</Text></div></Space>
                </Card>
              </Col>
              <Col span={6}>
                <Card size="small" styles={{ body: { padding: "8px 12px" } }}>
                  <Space><div style={{ fontSize: 18, fontWeight: 700, color: failed > 0 ? token.colorError : undefined }}>{failed}</div><div><div style={{ fontSize: 12, fontWeight: 500 }}>Failed</div>{failedModels.length > 0 && <Text type="secondary" style={{ fontSize: 11 }}>{failedModels.join(", ")}</Text>}</div></Space>
                </Card>
              </Col>
              <Col span={6}>
                <Card size="small" styles={{ body: { padding: "8px 12px" } }}>
                  <Space><div style={{ fontSize: 18, fontWeight: 700, color: token.colorTextSecondary }}>{skipped}</div><div><div style={{ fontSize: 12, fontWeight: 500 }}>Skipped</div>{skipped > 0 && <Text type="secondary" style={{ fontSize: 11 }}>downstream of fail</Text>}</div></Space>
                </Card>
              </Col>
              <Col span={6}>
                <Card size="small" styles={{ body: { padding: "8px 12px" } }}>
                  <Space><div style={{ fontSize: 18, fontWeight: 700 }}>{dur}</div><div><div style={{ fontSize: 12, fontWeight: 500 }}>Runtime</div>{expected && <Text type="secondary" style={{ fontSize: 11 }}>expected {expected}</Text>}</div></Space>
                </Card>
              </Col>
            </Row>

            {/* Error box */}
            {run.error_message && (
              <div className="rh-error-box">
                <div className="rh-error-box-title">
                  <span><CloseCircleFilled style={{ color: token.colorError, marginRight: 6 }} />Error in {errorModelName || "execution"}</span>
                  <Space size={4}>
                    <Button size="small" type="text" icon={<CopyOutlined />} onClick={() => handleCopyError(run.error_message)}>Copy</Button>
                    <Button size="small" type="text" icon={<EyeOutlined />} onClick={() => envInfo.project_id && navigate(`/ide/project/${envInfo.project_id}`)} disabled={!envInfo.project_id}>View model</Button>
                  </Space>
                </div>
                <div className="rh-error-msg">
                  {errorModelName && <><strong style={{ color: token.colorError }}>{errorModelName}</strong>{" · "}</>}
                  {errorMsg.replace(errorModelName ? `${errorModelName} · ` : "", "")}
                </div>
                {errorStack && <div className="rh-error-stack">{errorStack}</div>}
              </div>
            )}

            {/* Execution Timeline for failures */}
            <Text strong style={{ fontSize: 11, textTransform: "uppercase", letterSpacing: 0.4 }}>Execution timeline</Text>
            <Timeline style={{ marginTop: 12 }} items={[
              { dot: <CheckCircleFilled style={{ color: token.colorSuccess, fontSize: 14 }} />, children: (<Row justify="space-between"><Col><Text>Setup</Text></Col><Col><Text type="secondary" style={{ fontFamily: token.fontFamilyCode || "monospace" }}>—</Text></Col></Row>) },
              ...models.map((m, i) => {
                const isOk = m.end_status === "OK" || m.status === "success";
                const isFail = m.end_status === "FAIL" || m.status === "failure";
                return {
                  key: i,
                  dot: isOk ? <CheckCircleFilled style={{ color: token.colorSuccess, fontSize: 14 }} /> : isFail ? <CloseCircleFilled style={{ color: token.colorError, fontSize: 14 }} /> : <MinusOutlined />,
                  color: isFail ? "red" : isOk ? "green" : "gray",
                  children: (<Row justify="space-between"><Col><Text style={{ color: isFail ? token.colorError : undefined, fontWeight: isFail ? 600 : undefined }}>{m.name}</Text></Col><Col><Text type="secondary" style={{ fontFamily: token.fontFamilyCode || "monospace" }}>{m.duration_ms ? formatDurationMs(m.duration_ms) : "—"}</Text></Col></Row>),
                };
              }),
              ...(skipped > 0 ? [{ dot: <MinusOutlined />, color: "gray", children: (<Row justify="space-between"><Col><Text type="secondary">{skipped} downstream models</Text></Col><Col><Text type="secondary" style={{ fontFamily: token.fontFamilyCode || "monospace" }}>skipped</Text></Col></Row>) }] : []),
            ]} />
          </>
        )}
      </Card>
    );
  };

  /* ═══════════════ RENDER ═══════════════ */
  return (
    <div className="runhistory-container">
      {/* Header */}
      <div className="runhistory-header">
        <Title level={3} style={{ margin: 0 }}>Run History</Title>
        {filters.job && envInfo.job_name && (
          <Text type="secondary">All runs for job <strong>{envInfo.job_name}</strong>. Sorted by most recent.</Text>
        )}
      </div>

      {/* Stats Cards */}
      {filters.job && (
        <Row gutter={12} style={{ padding: "12px 24px" }}>
          <Col span={6}>
            <StatCard label="Success rate (7d)" icon={<CheckCircleFilled style={{ color: token.colorSuccess }} />}
              value={statsLoading ? "..." : stats?.success_rate_7d != null ? `${stats.success_rate_7d}%` : "— %"}
              subtext={!statsLoading && <Text style={{ color: token.colorError, fontSize: 11 }}>{stats?.success_count_7d || 0} of {stats?.total_count_7d || 0} succeeded</Text>}
            />
          </Col>
          <Col span={6}>
            <StatCard label="Avg duration"
              value={statsLoading ? "..." : stats?.avg_duration_ms != null ? formatDurationMs(stats.avg_duration_ms) : "—"}
              spark={<Sparkline color={token.colorPrimary} data={stats?.duration_trend} />}
            />
          </Col>
          <Col span={6}>
            <StatCard label="Failures (24h)"
              value={statsLoading ? "..." : stats?.failures_24h ?? 0}
              valueColor={stats?.failures_24h > 0 ? token.colorError : undefined}
              subtext={!statsLoading && <Text style={{ color: token.colorError, fontSize: 11 }}>
                {stats?.failures_change > 0 ? `↑ from ${stats.failures_24h - stats.failures_change} yesterday` : "↑ from 0 yesterday"}
              </Text>}
            />
          </Col>
          <Col span={6}>
            <StatCard label="Last successful run"
              value={statsLoading ? "..." : stats?.last_successful_run ? <span style={{ fontSize: 14, fontWeight: 600 }}>{getRelativeTime(stats.last_successful_run)}</span> : <span style={{ fontSize: 14, fontWeight: 600 }}>Never</span>}
              subtext={!stats?.last_successful_run && !statsLoading && <Text type="secondary" style={{ fontSize: 11 }}>Since job created</Text>}
            />
          </Col>
        </Row>
      )}

      {/* Filter bar */}
      <div style={{ padding: "0 24px 12px" }}>
        <Card size="small" styles={{ body: { padding: 10 } }}>
          <Row gutter={[8, 8]} align="middle" wrap>
            <Col flex="200px"><Input size="small" placeholder="Search runs..." prefix={<SearchOutlined />} value={filters.search} onChange={(e) => handleFilterChange("search", e.target.value)} allowClear /></Col>
            <Col>
              <Select size="small" style={{ width: 140 }} value={datePreset}
                onChange={(v) => { setDatePreset(v); setShowCustomDate(v === "custom"); if (v !== "custom") setCustomDateRange(null); }}
                options={[
                  { label: "Last 24 hours", value: "24h" },
                  { label: "Last 7 days", value: "7d" },
                  { label: "Last 30 days", value: "30d" },
                  { label: "All time", value: "all" },
                  { label: "Custom range", value: "custom" },
                ]}
                suffixIcon={<CalendarOutlined />}
              />
            </Col>
            {showCustomDate && (
              <Col><RangePicker size="small" value={customDateRange} onChange={(dates) => setCustomDateRange(dates)} /></Col>
            )}
            <Col><Select size="small" placeholder="Status" style={{ width: 120 }} allowClear value={filters.status || undefined} onChange={(v) => handleFilterChange("status", v)} options={STATUS_OPTIONS} /></Col>
            <Col><Select size="small" placeholder="Trigger" style={{ width: 120 }} allowClear value={filters.trigger || undefined} onChange={(v) => handleFilterChange("trigger", v)} options={[{ label: "Manual", value: "manual" }, { label: "Scheduled", value: "scheduled" }]} /></Col>
            <Col><Select size="small" placeholder="Environment" style={{ width: 140 }} allowClear
              value={envInfo.env_type || undefined}
              options={envInfo.env_type ? [{ label: envInfo.env_type, value: envInfo.env_type }] : []}
            /></Col>
            <Col flex="auto" style={{ textAlign: "right" }}>
              <Space size={8}>
                {activeFilterCount > 0 && (
                  <>
                    <Badge count={`${activeFilterCount} filter${activeFilterCount > 1 ? "s" : ""}`} style={{ backgroundColor: token.colorPrimary }} />
                    <Button type="link" size="small" onClick={() => { setFilters((p) => ({ ...p, status: "", trigger: "", search: "" })); setDatePreset("24h"); setCustomDateRange(null); setShowCustomDate(false); }}>Clear</Button>
                  </>
                )}
                <Tooltip title="Refresh"><Button icon={<ReloadOutlined spin={loading} />} size="small" onClick={handleRefresh} /></Tooltip>
              </Space>
            </Col>
          </Row>
        </Card>
      </div>

      {/* Job header bar */}
      {filters.job && envInfo.job_name && (
        <Row justify="space-between" align="middle" style={{ padding: "4px 24px 12px" }}>
          <Col><Space><Text strong style={{ fontSize: 14 }}>{envInfo.job_name}</Text><Text type="secondary" style={{ fontSize: 13 }}>· {totalCount} runs</Text></Space></Col>
          <Col>
            <Space>
              {envInfo.env_type && <Tag color="red" style={{ fontWeight: 600 }}>● {envInfo.env_type?.toUpperCase()}</Tag>}
              {stats?.schedule_enabled && <Tag color="purple" icon={<ClockCircleOutlined />} style={{ fontWeight: 600 }}>SCHEDULED {stats.schedule_label?.toUpperCase() || "DAILY"}</Tag>}
              <Button type="link" size="small" onClick={() => navigate(`/project/job/list?task=${envInfo.id}`)}>View job →</Button>
            </Space>
          </Col>
        </Row>
      )}

      {/* Table */}
      <div className="runhistory-table-container">
        <Table
          rowKey="id"
          columns={columns}
          dataSource={jobHistoryData}
          loading={loading}
          size="middle"
          expandable={{
            expandedRowKeys,
            onExpandedRowsChange: (keys) => setExpandedRowKeys([...keys]),
            expandedRowRender: (record) => <RunDetail run={record} />,
            expandRowByClick: false,
            expandIcon: ({ expanded, onExpand, record }) => (
              ["SUCCESS", "FAILURE", "RETRY", "REVOKED"].includes(record.status) ? (
                <Button type="text" size="small" icon={expanded ? <UpOutlined /> : <DownOutlined />} onClick={(e) => onExpand(record, e)} />
              ) : null
            ),
          }}
          locale={{ emptyText: <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description={!jobListItems.length ? "No jobs created yet" : !filters.job ? "Select a job" : "No matching runs"} /> }}
          pagination={{
            current: currentPage,
            pageSize,
            total: Math.min(totalCount, 1000),
            showTotal: (t, r) => `Showing ${r[0]}–${r[1]} of ${t} runs`,
            showSizeChanger: true,
            pageSizeOptions: ["10", "20", "50"],
            onChange: (p, s) => { setCurrentPage(p); setPageSize(s); fetchHistory(filters.job, p, s, filters); },
          }}
        />
      </div>
    </div>
  );
};

export { Runhistory };
