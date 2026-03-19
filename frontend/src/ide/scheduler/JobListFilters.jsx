import { Input, Select, Button, Space } from "antd";
import { PlusOutlined, ReloadOutlined } from "@ant-design/icons";
import { memo } from "react";
import PropTypes from "prop-types";

const JobListFilters = memo(
  ({
    searchQuery,
    onSearchChange,
    filters,
    setFilters,
    projects,
    canWrite,
    onCreateJob,
    onRefresh,
    loading,
    isJobLimitReached = false,
  }) => (
    <div className="flex-space-between job-deploy-filters">
      <Space>
        <Input
          placeholder="Search by job name"
          onChange={onSearchChange}
          value={searchQuery}
          className="job-deploy-header-field"
        />

        <Select
          placeholder="Environment"
          value={filters.env}
          onChange={(value) => setFilters({ ...filters, env: value })}
          options={[
            { label: "All", value: "all" },
            { label: "STAGING", value: "STG" },
            { label: "DEV", value: "DEV" },
            { label: "PROD", value: "PROD" },
          ]}
          className="job-deploy-header-field"
        />

        <Select
          placeholder="Project"
          options={[...projects, { label: "All", value: "all" }]}
          value={filters.proj}
          onChange={(value) => setFilters({ ...filters, proj: value })}
          className="job-deploy-header-field"
        />
      </Space>

      <Space>
        <Button
          type="primary"
          icon={<PlusOutlined />}
          onClick={onCreateJob}
          disabled={!canWrite || isJobLimitReached}
          className="primary_button_style"
        >
          Create Job
        </Button>
        <Button
          icon={<ReloadOutlined spin={loading} />}
          onClick={onRefresh}
          disabled={loading}
        />
      </Space>
    </div>
  )
);

JobListFilters.propTypes = {
  searchQuery: PropTypes.string,
  onSearchChange: PropTypes.func,
  filters: PropTypes.object,
  setFilters: PropTypes.func,
  projects: PropTypes.array,
  canWrite: PropTypes.bool,
  onCreateJob: PropTypes.func,
  onRefresh: PropTypes.func,
  loading: PropTypes.bool,
  isJobLimitReached: PropTypes.bool,
};

JobListFilters.displayName = "JobListFilters";

export { JobListFilters };
