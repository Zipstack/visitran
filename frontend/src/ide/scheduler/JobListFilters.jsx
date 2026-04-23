import { Input, Select, Button, Space, Badge, Card, Row, Col } from "antd";
import { PlusOutlined, ReloadOutlined, SearchOutlined } from "@ant-design/icons";
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
    totalJobs = 0,
  }) => {
    const activeFilterCount = [
      filters.env !== "all" ? filters.env : null,
      filters.proj !== "all" ? filters.proj : null,
      filters.status || null,
      filters.lastRun || null,
      filters.schedule || null,
      searchQuery || null,
    ].filter(Boolean).length;

    const handleClearFilters = () => {
      setFilters({ env: "all", proj: "all", status: "", lastRun: "", schedule: "" });
      if (onSearchChange) onSearchChange({ target: { value: "" } });
    };

    return (
      <Card size="small" styles={{ body: { padding: 10 } }} style={{ marginBottom: 12 }}>
        <Row gutter={[8, 8]} align="middle" wrap>
          <Col flex="220px">
            <Input
              size="small"
              placeholder="Search by job name..."
              prefix={<SearchOutlined />}
              onChange={onSearchChange}
              value={searchQuery}
              allowClear
            />
          </Col>
          <Col>
            <Select
              size="small"
              placeholder="Status"
              style={{ width: 120 }}
              allowClear
              value={filters.status || undefined}
              onChange={(v) => setFilters({ ...filters, status: v || "" })}
              options={[
                { label: "Success", value: "SUCCESS" },
                { label: "Failed", value: "FAILED" },
                { label: "Running", value: "RUNNING" },
              ]}
            />
          </Col>
          <Col>
            <Select
              size="small"
              placeholder="Last run"
              style={{ width: 120 }}
              allowClear
              value={filters.lastRun || undefined}
              onChange={(v) => setFilters({ ...filters, lastRun: v || "" })}
              options={[
                { label: "Last 24h", value: "24h" },
                { label: "Last 7 days", value: "7d" },
                { label: "Last 30 days", value: "30d" },
              ]}
            />
          </Col>
          <Col>
            <Select
              size="small"
              placeholder="Environment"
              style={{ width: 140 }}
              allowClear
              value={filters.env !== "all" ? filters.env : undefined}
              onChange={(v) => setFilters({ ...filters, env: v || "all" })}
              options={[
                { label: "Production", value: "PROD" },
                { label: "Staging", value: "STG" },
                { label: "Development", value: "DEV" },
              ]}
            />
          </Col>
          <Col>
            <Select
              size="small"
              placeholder="Schedule"
              style={{ width: 120 }}
              allowClear
              value={filters.schedule || undefined}
              onChange={(v) => setFilters({ ...filters, schedule: v || "" })}
              options={[
                { label: "Cron", value: "cron" },
                { label: "Interval", value: "interval" },
              ]}
            />
          </Col>
          <Col flex="auto" style={{ textAlign: "right" }}>
            <Space size={8}>
              <span style={{ fontSize: 12, color: "var(--font-color-3, #8c8c8c)" }}>
                {totalJobs} job{totalJobs !== 1 ? "s" : ""}
              </span>
              {activeFilterCount > 0 && (
                <Button type="link" size="small" onClick={handleClearFilters}>
                  Clear filters
                </Button>
              )}
              <Button
                size="small"
                icon={<ReloadOutlined spin={loading} />}
                onClick={onRefresh}
                disabled={loading}
              />
            </Space>
          </Col>
        </Row>
      </Card>
    );
  }
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
  totalJobs: PropTypes.number,
};

JobListFilters.displayName = "JobListFilters";

export { JobListFilters };
