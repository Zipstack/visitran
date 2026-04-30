import { useEffect, useState, useCallback, useMemo } from "react";
import { Alert, Button, Space, Typography, Modal, Pagination } from "antd";
import debounce from "lodash/debounce";
import { useNavigate, useSearchParams } from "react-router-dom";

import { checkPermission } from "../../common/helpers";
import { useNotificationService } from "../../service/notification-service";
import { useJobService } from "./service";
import { usePagination } from "../../widgets/hooks/usePagination";
import { JobListFilters } from "./JobListFilters.jsx";
import { JobListTable } from "./JobListTable.jsx";
import { JobDeploy } from "./JobDeploy.jsx";

import "./JobDeploy.css";

let useSubscriptionDetailsStoreSafe;
try {
  useSubscriptionDetailsStoreSafe =
    require("../../plugins/store/subscription-details-store").useSubscriptionDetailsStore;
} catch {
  useSubscriptionDetailsStoreSafe = null;
}

const JobList = () => {
  const navigate = useNavigate();
  const { listPeriodicTasks, getProjects, deleteTask } = useJobService();
  const { notify } = useNotificationService();
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
  const [filters, setFilters] = useState({ proj: "all", env: "all" });
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
    if (showLoader) {
      setTableLoading(true);
    }
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
      if (showLoader) {
        setTableLoading(false);
      }
    }
  };

  const fetchProjects = useCallback(async () => {
    try {
      const data = await getProjects();
      setProjects(
        data.map((el) => ({
          label: el.project_name,
          value: el.project_name,
        }))
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

  const runSearch = useMemo(
    () =>
      debounce((text) => {
        const term = text.toLowerCase();
        setJobList(
          backup.filter(
            ({ task_name, project }) =>
              task_name?.toLowerCase().startsWith(term) ||
              project?.name?.toLowerCase().startsWith(term)
          )
        );
      }, 300),
    [backup]
  );

  useEffect(() => () => runSearch.cancel(), [runSearch]); // cancel debounce on unmount

  const onSearchChange = (e) => {
    const value = e.target.value;
    setSearchQuery(value);
    runSearch(value);
  };

  const filterBy = useCallback((query, data, type) => {
    if (query === "all") return data;
    return data.filter((el) =>
      type === "env"
        ? el.environment?.type === query
        : el.project?.name === query
    );
  }, []);

  useEffect(() => {
    const { env, proj } = filters;
    let filtered = backup;

    filtered = filterBy(proj, filtered, "proj");
    filtered = filterBy(env, filtered, "env");

    setJobList(filtered);
  }, [filters, backup, filterBy]);

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
    } else if (searchParams.get("task")) {
      const taskId = Number(searchParams.get("task"));
      if (!Number.isNaN(taskId)) {
        setSelectedJobId(taskId);
        setOpenJobDeploy(true);
        setSearchParams({}, { replace: true });
      }
    }
  }, [searchParams, setSearchParams]);

  const onDelete = async () => {
    try {
      await deleteTask(delTaskDetail.projectId, delTaskDetail.taskId);
      setIsDeleteModalOpen(false);
      notify({
        type: "success",
        message: `Job Deleted Successfully`,
      });
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

  return (
    <div className="flex-direction-column width-100 height-100 overflow-y-auto">
      <div className="flex-1 pad-12">
        <Space direction="vertical" className="height-100 width-100">
          <Typography.Text className="font-size-16 job-deploy-title">
            Jobs
          </Typography.Text>

          {isJobLimitReached && (
            <Alert
              message={`You've reached the maximum number of jobs (${usage.jobs.used}/${usage.jobs.allowed}) allowed in your current plan. Upgrade to create more.`}
              type="warning"
              showIcon
              style={{ marginBottom: 8 }}
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
            <Space className="flex-justify-right pad-10-top">
              <Pagination
                className="custom-pagination"
                current={currentPage}
                pageSize={pageSize}
                total={Math.min(totalCount, 1000)}
                showTotal={(total, range) =>
                  `Showing ${range[0]} to ${range[1]} of ${Math.min(
                    totalCount,
                    1000
                  )} entries`
                }
                showSizeChanger
                onChange={handlePagination}
              />
            </Space>
          )}
        </Space>
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
        <Typography>Are you sure you want to delete this Job?</Typography>
      </Modal>
    </div>
  );
};

export { JobList };
