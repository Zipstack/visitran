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
} from "antd";
import {
  DatabaseOutlined,
  CalendarOutlined,
  EditOutlined,
  DeleteOutlined,
  PlayCircleOutlined,
  LoadingOutlined,
  HistoryOutlined,
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

    const goToRunHistory = (userTaskId) =>
      navigate(`/project/job/history?task=${userTaskId}`);

    const renderDateCell = (text) => {
      if (!text) {
        return (
          <Typography.Text type="secondary">Not started yet.</Typography.Text>
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
    };
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
        if (res.status) {
          // Re-fetch the job list to get updated data from server
          if (onToggleSuccess) onToggleSuccess();
        }
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
        notify({
          type: "success",
          message: "Job Scheduled Successfully",
        });
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
          render: (text, record) => (
            <Tooltip title="Open Run History for this job">
              <Button
                type="link"
                size="small"
                style={{ padding: 0, height: "auto" }}
                icon={<HistoryOutlined />}
                onClick={() => goToRunHistory(record.user_task_id)}
              >
                <span className="job-deploy-bold">{text}</span>
              </Button>
            </Tooltip>
          ),
        },
        {
          title: "Project",
          dataIndex: "project",
          key: "project",
          render: (project) => (
            <Typography.Text>{project?.name}</Typography.Text>
          ),
        },
        {
          title: "Environment",
          dataIndex: "environment",
          key: "environment",
          render: (environment) => (
            <Tooltip title={environment?.name}>
              <Tag color="geekblue" icon={<DatabaseOutlined />}>
                {environment?.name}
              </Tag>
            </Tooltip>
          ),
        },
        {
          title: "Schedule",
          key: "schedule",
          render: (_, record) => {
            const scheduleText = getTooltipText(
              record.periodic_task_details?.[record.task_type] ?? {},
              record.task_type
            );
            return (
              <Tooltip title={scheduleText}>
                <Tag color="geekblue" icon={<CalendarOutlined />}>
                  {record.task_type === "interval" ? "Interval" : "Cron"}
                </Tag>
                <Typography.Text
                  type="secondary"
                  style={{ fontSize: 12, marginLeft: 4 }}
                >
                  {scheduleText}
                </Typography.Text>
              </Tooltip>
            );
          },
        },
        {
          title: "Last Run Status",
          key: "last_run_status",
          render: (_, record) => {
            if (!record.task_status) {
              return <Typography.Text type="secondary">—</Typography.Text>;
            }
            const isFailed = [
              "FAILED",
              "FAILED PERMANENTLY",
              "FAILURE",
            ].includes(record.task_status);
            return (
              <Tooltip
                title={
                  isFailed
                    ? "Logs available in Run History"
                    : record.task_status
                }
              >
                <Tag
                  color={
                    isFailed
                      ? "red"
                      : record.task_status === "SUCCESS"
                      ? "green"
                      : "default"
                  }
                >
                  {record.task_status === "FAILURE"
                    ? "FAILED"
                    : record.task_status}
                </Tag>
              </Tooltip>
            );
          },
        },
        {
          title: "Last Run",
          dataIndex: "task_completion_time",
          key: "last_run",
          render: renderDateCell,
        },
        {
          title: "Next Run",
          dataIndex: "next_run_time",
          key: "next_run",
          render: renderDateCell,
        },
        {
          title: "Status",
          key: "status",
          render: (_, record) => (
            <Space>
              <Switch
                checked={record.periodic_task_details?.enabled}
                checkedChildren="Enabled"
                unCheckedChildren="Disabled"
                onChange={(checked) => {
                  handleSwitchSchedular(record, checked);
                }}
              />
              <Typography.Text
                type={
                  record.periodic_task_details?.enabled ? "success" : "warning"
                }
              ></Typography.Text>
            </Space>
          ),
        },
        {
          title: "Actions",
          key: "actions",
          render: (_, record) => (
            <Space>
              <Tooltip title="Deploy Job">
                <Button
                  type="link"
                  icon={
                    loading[record.user_task_id] ? (
                      <LoadingOutlined spin={true} />
                    ) : (
                      <PlayCircleOutlined />
                    )
                  }
                  onClick={() =>
                    handleRun(record.project.id, record.user_task_id)
                  }
                />
              </Tooltip>
              <Tooltip title="edit">
                <Button
                  type="link"
                  icon={<EditOutlined />}
                  onClick={() => onRowClick(record.user_task_id)}
                />
              </Tooltip>
              <Tooltip title="delete">
                <Button
                  type="link"
                  danger
                  icon={<DeleteOutlined />}
                  onClick={() => handleDelete(record)}
                />
              </Tooltip>
            </Space>
          ),
        },
      ],
      [getTooltipText, onRowClick, loading]
    );
    return (
      <Table
        columns={columns}
        dataSource={data}
        rowKey="user_task_id"
        loading={tableLoading}
        locale={{
          emptyText: (
            <Empty
              image={Empty.PRESENTED_IMAGE_SIMPLE}
              description="No jobs created yet"
            />
          ),
        }}
        bordered
        className="job-deploy-table"
        pagination={{ pageSize: 10 }}
      />
    );
  }
);
JobListTable.propTypes = {
  data: PropTypes.array,
  onRowClick: PropTypes.func,
  setIsDeleteModalOpen: PropTypes.func,
  setDelTaskDetail: PropTypes.func,
  tableLoading: PropTypes.bool,
  onToggleSuccess: PropTypes.func,
};
JobListTable.displayName = "JobListTable";
export { JobListTable };
