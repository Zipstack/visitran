/* eslint-disable eqeqeq, react/prop-types */
import { useEffect, useState, useCallback, useMemo } from "react";
import {
  Alert,
  Button,
  Space,
  Typography,
  Modal,
  Pagination,
  Card,
  Row,
  Col,
  theme,
} from "antd";
import {
  CheckCircleFilled,
  CloseCircleFilled,
  ThunderboltOutlined,
  PlusOutlined,
} from "@ant-design/icons";
import { useNavigate, useSearchParams } from "react-router-dom";

import { checkPermission } from "../../common/helpers";
import { useNotificationService } from "../../service/notification-service";
import { useJobService } from "./service";
import { usePagination } from "../../widgets/hooks/usePagination";
import { JobListFilters } from "./JobListFilters.jsx";
import { JobListTable } from "./JobListTable.jsx";
import { JobDeploy } from "./JobDeploy.jsx";

import "./JobDeploy.css";

const { Text, Title } = Typography;

let useSubscriptionDetailsStoreSafe;
try {
  useSubscriptionDetailsStoreSafe =
    require("../../plugins/store/subscription-details-store").useSubscriptionDetailsStore;
} catch {
  useSubscriptionDetailsStoreSafe = null;
}

/* ── StatCard ── */
const StatCard = ({ label, icon, value, valueColor, subtext }) => (
  <Card
    size="small"
    style={{ height: "100%" }}
    styles={{ body: { padding: 14, height: "100%" } }}
  >
    <Space size={4} direction="vertical" style={{ width: "100%" }}>
      <Text
        type="secondary"
        style={{
          fontSize: 11,
          fontWeight: 600,
          letterSpacing: 0.4,
          textTransform: "uppercase",
        }}
      >
        {icon}
        {icon ? " " : ""}
        {label}
      </Text>
      <div
        style={{
          fontSize: 22,
          fontWeight: 700,
          lineHeight: 1,
          color: valueColor,
          fontVariantNumeric: "tabular-nums",
        }}
      >
        {value}
      </div>
      {subtext && <div style={{ fontSize: 11 }}>{subtext}</div>}
    </Space>
  </Card>
);

const JobList = () => {
  const navigate = useNavigate();
  const { listPeriodicTasks, getProjects, deleteTask } = useJobService();
  const { notify } = useNotificationService();
  const { token } = theme.useToken();
  const [delTaskDetail, setDelTaskDetail] = useState({
    projectId: "",
    taskId: "",
  });
  const [jobList, setJobList] = useState([]);
  const [isDeleteModalOpen, setIsDeleteModalOpen] = useState(false);
  const [isJobListModified, setIsJobListModified] = useState(false);
  const [backup, setBackup] = useState([]);
  const [projects, setProjects] = useState([]);
  const [tableLoading, setTableLoading] = useState(false);
  const [openJobDeploy, setOpenJobDeploy] = useState(false);
  const [selectedJobId, setSelectedJobId] = useState(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [searchParams, setSearchParams] = useSearchParams();
  const [prefillModel, setPrefillModel] = useState(null);
  const [prefillProject, setPrefillProject] = useState(null);
  const [filters, setFilters] = useState({
    proj: "all",
    env: "all",
    status: "",
    lastRun: "",
    schedule: "",
  });
  const {
    currentPage,
    pageSize,
    totalCount,
    setTotalCount,
    setCurrentPage,
    setPageSize,
  } = usePagination();
  const canWrite = checkPermission("JOB_DEPLOYMENT", "can_write");

  const usage =
    typeof useSubscriptionDetailsStoreSafe === "function"
      ? useSubscriptionDetailsStoreSafe((s) => s.usage)
      : null;
  const isJobLimitReached =
    usage?.jobs && usage.jobs.used >= usage.jobs.allowed;

  const fetchJobs = async ({
    page = currentPage,
    limit = pageSize,
    showLoader = true,
  } = {}) => {
    if (showLoader) setTableLoading(true);
    try {
      const tasks = await listPeriodicTasks(page, limit);
      const { page_items, total_items, current_page } = tasks.data;
      setJobList(page_items);
      setBackup(page_items);
      setTotalCount(total_items);
      setCurrentPage(current_page);
      setPageSize(limit);
    } catch (error) {
      notify({ error });
    } finally {
      if (showLoader) setTableLoading(false);
    }
  };

  const fetchProjects = useCallback(async () => {
    try {
      const data = await getProjects();
      setProjects(
        data.map((el) => ({ label: el.project_name, value: el.project_name }))
      );
    } catch (error) {
      notify({ error });
    }
  }, [getProjects]);

  useEffect(() => {
    fetchJobs({ showLoader: true });
    fetchProjects();
  }, []);
  useEffect(() => {
    if (!isJobListModified) return;
    fetchJobs();
    setIsJobListModified(false);
  }, [isJobListModified]);

  const onSearchChange = (e) => setSearchQuery(e.target.value);

  // Client-side filtering
  useEffect(() => {
    let filtered = backup;
    const { env, proj, status, schedule, lastRun } = filters;
    if (proj !== "all")
      filtered = filtered.filter((el) => el.project?.name === proj);
    if (env !== "all")
      filtered = filtered.filter((el) => el.environment?.type === env);
    if (status) {
      filtered = filtered.filter((el) => {
        if (status === "FAILED")
          return ["FAILED", "FAILURE", "FAILED PERMANENTLY"].includes(
            el.task_status
          );
        if (status === "RUNNING")
          return ["RUNNING", "STARTED", "PENDING"].includes(el.task_status);
        return el.task_status === status;
      });
    }
    if (schedule) filtered = filtered.filter((el) => el.task_type === schedule);
    if (lastRun) {
      const windowMs = { "24h": 86400000, "7d": 604800000, "30d": 2592000000 }[
        lastRun
      ];
      if (windowMs) {
        const cutoff = Date.now() - windowMs;
        filtered = filtered.filter(
          (el) =>
            el.task_completion_time &&
            new Date(el.task_completion_time) >= cutoff
        );
      }
    }
    if (searchQuery) {
      const term = searchQuery.toLowerCase();
      filtered = filtered.filter(
        ({ task_name, project }) =>
          task_name?.toLowerCase().includes(term) ||
          project?.name?.toLowerCase().includes(term)
      );
    }
    setJobList(filtered);
  }, [filters, backup, searchQuery]);

  const handleRowClick = useCallback((id) => {
    setOpenJobDeploy(true);
    setSelectedJobId(id);
  }, []);

  useEffect(() => {
    if (!openJobDeploy) {
      setSelectedJobId(null);
      setPrefillModel(null);
      setPrefillProject(null);
    }
  }, [openJobDeploy]);

  useEffect(() => {
    if (searchParams.get("create") === "1") {
      setPrefillModel(searchParams.get("model") || null);
      setPrefillProject(searchParams.get("project") || null);
      setOpenJobDeploy(true);
      setSearchParams({}, { replace: true });
    }
  }, [searchParams, setSearchParams]);

  const onDelete = async () => {
    try {
      await deleteTask(delTaskDetail.projectId, delTaskDetail.taskId);
      setIsDeleteModalOpen(false);
      notify({ type: "success", message: "Job deleted successfully" });
      setJobList(
        jobList.filter(
          (el) => el.periodic_task_details.id !== delTaskDetail.taskId
        )
      );
    } catch (error) {
      notify({ error });
    }
  };

  const handlePagination = (newPage, newPageSize) => {
    if (currentPage !== newPage || pageSize !== newPageSize) {
      setCurrentPage(newPage);
      setPageSize(newPageSize);
      fetchJobs({ page: newPage, limit: newPageSize });
    }
  };

  // Compute stats from current data
  const stats = useMemo(() => {
    const activeJobs = backup.filter(
      (j) => j.periodic_task_details?.enabled
    ).length;
    const pausedJobs = backup.length - activeJobs;

    // Filter to jobs that ran in the last 24 hours for rate/failure stats
    const cutoff24h = Date.now() - 86400000;
    const recentJobs = backup.filter(
      (j) =>
        j.task_completion_time && new Date(j.task_completion_time) >= cutoff24h
    );
    const failedJobs = recentJobs.filter((j) =>
      ["FAILED", "FAILURE", "FAILED PERMANENTLY"].includes(j.task_status)
    ).length;
    const successJobs = recentJobs.filter(
      (j) => j.task_status === "SUCCESS"
    ).length;
    const successRate =
      recentJobs.length > 0
        ? Math.round((successJobs / recentJobs.length) * 100)
        : null;

    // Next upcoming run
    const upcomingRuns = backup
      .filter((j) => j.next_run_time && j.periodic_task_details?.enabled)
      .sort((a, b) => new Date(a.next_run_time) - new Date(b.next_run_time));
    const nextRun = upcomingRuns.length > 0 ? upcomingRuns[0] : null;
    let nextRunCountdown = null;
    if (nextRun?.next_run_time) {
      const diff = new Date(nextRun.next_run_time) - new Date();
      if (diff > 0) {
        const mins = Math.floor(diff / 60000);
        if (mins < 60) nextRunCountdown = `in ${mins}m`;
        else if (mins < 1440)
          nextRunCountdown = `in ${Math.floor(mins / 60)}h ${mins % 60}m`;
        else nextRunCountdown = `in ${Math.floor(mins / 1440)}d`;
      }
    }

    return {
      activeJobs,
      pausedJobs,
      failedJobs,
      successRate,
      nextRun,
      nextRunCountdown,
    };
  }, [backup]);

  return (
    <div
      style={{
        height: "100%",
        display: "flex",
        flexDirection: "column",
        overflow: "auto",
      }}
    >
      <div style={{ flex: 1, padding: "20px 24px" }}>
        {/* Header */}
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "flex-start",
            marginBottom: 16,
          }}
        >
          <div>
            <Title level={3} style={{ margin: 0 }}>
              Jobs
            </Title>
            <Text type="secondary">
              Scheduled data pipelines across all your projects.
            </Text>
          </div>
          <Button
            type="primary"
            icon={<PlusOutlined />}
            onClick={() => setOpenJobDeploy(true)}
            disabled={!canWrite || isJobLimitReached}
          >
            Create Job
          </Button>
        </div>

        {isJobLimitReached && (
          <Alert
            message={`You've reached the maximum number of jobs (${usage.jobs.used}/${usage.jobs.allowed}). Upgrade to create more.`}
            type="warning"
            showIcon
            style={{ marginBottom: 12 }}
            action={
              <Button
                size="small"
                type="link"
                onClick={() => navigate("/project/setting/subscriptions")}
              >
                Upgrade
              </Button>
            }
          />
        )}

        {/* Stats Cards */}
        <Row gutter={12} style={{ marginBottom: 16 }}>
          <Col span={6}>
            <StatCard
              label="Active jobs"
              icon={
                <ThunderboltOutlined style={{ color: token.colorPrimary }} />
              }
              value={stats.activeJobs}
              subtext={
                stats.pausedJobs > 0 && (
                  <Text type="secondary">{stats.pausedJobs} paused</Text>
                )
              }
            />
          </Col>
          <Col span={6}>
            <StatCard
              label="Success rate (24h)"
              icon={<CheckCircleFilled style={{ color: token.colorSuccess }} />}
              value={
                stats.successRate != null ? `${stats.successRate}%` : "— %"
              }
              valueColor={
                stats.successRate === 100
                  ? token.colorSuccess
                  : stats.successRate > 0
                  ? token.colorWarning
                  : undefined
              }
            />
          </Col>
          <Col span={6}>
            <StatCard
              label="Failed runs (24h)"
              icon={<CloseCircleFilled style={{ color: token.colorError }} />}
              value={stats.failedJobs}
              valueColor={stats.failedJobs > 0 ? token.colorError : undefined}
              subtext={
                stats.failedJobs > 0 && (
                  <Text style={{ color: token.colorError, fontSize: 11 }}>
                    Needs attention
                  </Text>
                )
              }
            />
          </Col>
          <Col span={6}>
            <StatCard
              label="Next run"
              icon={<ThunderboltOutlined />}
              value={stats.nextRunCountdown || "—"}
              subtext={
                stats.nextRun && (
                  <Text type="secondary" style={{ fontSize: 11 }}>
                    {stats.nextRun.task_name} ·{" "}
                    {stats.nextRun.next_run_time
                      ? new Date(
                          stats.nextRun.next_run_time
                        ).toLocaleTimeString([], {
                          hour: "2-digit",
                          minute: "2-digit",
                        })
                      : ""}
                  </Text>
                )
              }
            />
          </Col>
        </Row>

        {/* Filters */}
        <JobListFilters
          searchQuery={searchQuery}
          onSearchChange={onSearchChange}
          filters={filters}
          setFilters={setFilters}
          projects={projects}
          canWrite={canWrite}
          onCreateJob={() => setOpenJobDeploy(true)}
          onRefresh={() => fetchJobs({ page: currentPage, limit: pageSize })}
          loading={tableLoading}
          isJobLimitReached={isJobLimitReached}
          totalJobs={jobList.length}
        />

        {/* Table */}
        <JobListTable
          data={jobList}
          onRowClick={handleRowClick}
          setIsDeleteModalOpen={setIsDeleteModalOpen}
          setDelTaskDetail={setDelTaskDetail}
          tableLoading={tableLoading}
          onToggleSuccess={() => fetchJobs({ showLoader: false })}
        />

        {jobList?.length > 0 && (
          <div
            style={{
              display: "flex",
              justifyContent: "flex-end",
              padding: "12px 0",
            }}
          >
            <Pagination
              current={currentPage}
              pageSize={pageSize}
              total={Math.min(totalCount, 1000)}
              showTotal={(total, range) =>
                `Showing ${range[0]}–${range[1]} of ${total} jobs`
              }
              showSizeChanger
              onChange={handlePagination}
            />
          </div>
        )}
      </div>

      {/* Job Deploy Modal */}
      <JobDeploy
        open={openJobDeploy}
        setOpen={setOpenJobDeploy}
        selectedJobDeployId={selectedJobId}
        setIsJobListModified={setIsJobListModified}
        prefillModel={prefillModel}
        prefillProject={prefillProject}
      />

      <Modal
        title="Delete Job"
        open={isDeleteModalOpen}
        onOk={onDelete}
        onCancel={() => setIsDeleteModalOpen(false)}
        okText="Delete"
        okButtonProps={{ danger: true }}
      >
        <Text>Are you sure you want to delete this Job?</Text>
      </Modal>
    </div>
  );
};

export { JobList };
