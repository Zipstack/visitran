import { useState, useEffect, useCallback } from "react";
import PropTypes from "prop-types";
import { Table, Select, DatePicker, Button, Tag } from "antd";
import { DownloadOutlined } from "@ant-design/icons";

import { useAxiosPrivate } from "../../service/axios-service";
import { useNotificationService } from "../../service/notification-service";
import { useProjectStore } from "../../store/project-store";
import { orgStore } from "../../store/org-store";
import { usePagination } from "../../widgets/hooks/usePagination";
import { fetchAuditEvents, exportAuditCsv } from "./services";

const { RangePicker } = DatePicker;

const EVENT_TYPE_OPTIONS = [
  { label: "All", value: "" },
  { label: "Committed", value: "version_committed" },
  { label: "Rolled Back", value: "version_rolled_back" },
  { label: "Draft Saved", value: "draft_saved" },
  { label: "Draft Discarded", value: "draft_discarded" },
  { label: "Conflict Resolved", value: "conflict_resolved" },
];

const EVENT_TAG_COLORS = {
  version_committed: "blue",
  version_rolled_back: "orange",
  draft_saved: "cyan",
  draft_discarded: "default",
  conflict_resolved: "purple",
  conflict_finalized: "green",
};

const columns = [
  { title: "Time", dataIndex: "created_at", key: "created_at", width: 180, render: (val) => val ? new Date(val).toLocaleString() : "\u2014" },
  { title: "Event", dataIndex: "event_type", key: "event_type", width: 160, render: (val) => <Tag color={EVENT_TAG_COLORS[val] || "default"}>{val}</Tag> },
  { title: "User", dataIndex: "user", key: "user", width: 140, render: (val) => val?.name || "system" },
  { title: "Message", dataIndex: "commit_message", key: "commit_message", ellipsis: true },
  { title: "Version", dataIndex: "version_number", key: "version_number", width: 80, render: (val) => val ? `v${val}` : "\u2014" },
];

function AuditTrail() {
  const [events, setEvents] = useState([]);
  const [loading, setLoading] = useState(false);
  const [eventType, setEventType] = useState("");
  const [dateRange, setDateRange] = useState(null);
  const [exporting, setExporting] = useState(false);

  const axiosRef = useAxiosPrivate();
  const { notify } = useNotificationService();
  const projectId = useProjectStore((state) => state.projectId);
  const orgId = orgStore.getState().selectedOrgId;
  const { currentPage, pageSize, totalCount, setTotalCount, onPaginationChange } = usePagination();

  const getFilters = useCallback(() => {
    const filters = {};
    if (eventType) filters.event_type = eventType;
    if (dateRange && dateRange[0]) filters.start_date = dateRange[0].format("YYYY-MM-DD");
    if (dateRange && dateRange[1]) filters.end_date = dateRange[1].format("YYYY-MM-DD");
    return filters;
  }, [eventType, dateRange]);

  const loadEvents = useCallback(async (page, limit) => {
    if (!projectId) return;
    setLoading(true);
    try {
      const data = await fetchAuditEvents(axiosRef, orgId, projectId, page || currentPage, limit || pageSize, getFilters());
      setEvents(data.page_items || []);
      setTotalCount(data.total_count || 0);
    } catch (error) { notify({ error }); }
    finally { setLoading(false); }
  }, [axiosRef, orgId, projectId, currentPage, pageSize, getFilters, setTotalCount, notify]);

  useEffect(() => { loadEvents(1, pageSize); }, [eventType, dateRange]); // eslint-disable-line react-hooks/exhaustive-deps

  const handlePageChange = (page, size) => { onPaginationChange(page, size); loadEvents(page, size); };

  const handleExport = async () => {
    setExporting(true);
    try {
      const blob = await exportAuditCsv(axiosRef, orgId, projectId, getFilters());
      const url = window.URL.createObjectURL(new Blob([blob]));
      const link = document.createElement("a"); link.href = url; link.setAttribute("download", "audit_trail.csv"); document.body.appendChild(link); link.click(); link.remove(); window.URL.revokeObjectURL(url);
    } catch (error) { notify({ error }); }
    finally { setExporting(false); }
  };

  return (
    <div>
      <div className="audit-trail-filters">
        <Select value={eventType} onChange={setEventType} options={EVENT_TYPE_OPTIONS} style={{ width: 180 }} placeholder="Event type" />
        <RangePicker onChange={setDateRange} />
        <Button className="audit-export-btn" icon={<DownloadOutlined />} onClick={handleExport} loading={exporting} size="small">Export CSV</Button>
      </div>
      <Table dataSource={events} columns={columns} rowKey={(row) => row.event_id || row.id} loading={loading} size="small" pagination={{ current: currentPage, pageSize, total: totalCount, onChange: handlePageChange, showSizeChanger: false }} expandable={{ expandedRowRender: (record) => <pre style={{ fontSize: 11, margin: 0 }}>{JSON.stringify(record.metadata || record, null, 2)}</pre> }} />
    </div>
  );
}

AuditTrail.propTypes = {};

export { AuditTrail };
