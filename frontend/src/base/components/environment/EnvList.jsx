/* eslint-disable react/prop-types */
import { useState, useEffect, useCallback, useMemo, useRef } from "react";
import {
  Button,
  Table,
  Space,
  Typography,
  Tooltip,
  Tag,
  Popconfirm,
  Pagination,
  Input,
  Select,
  Card,
  Row,
  Col,
  Empty,
  theme,
} from "antd";
import {
  EditOutlined,
  DeleteOutlined,
  PlusOutlined,
  ReloadOutlined,
  SearchOutlined,
  CheckCircleFilled,
  ExclamationCircleFilled,
  ThunderboltOutlined,
  FireFilled,
  ExperimentOutlined,
  CodeOutlined,
} from "@ant-design/icons";
import Cookies from "js-cookie";

import { orgStore } from "../../../store/org-store";
import { useAxiosPrivate } from "../../../service/axios-service";
import { checkPermission } from "../../../common/helpers";
import { usePagination } from "../../../widgets/hooks/usePagination";
import { EnvironmentDrawer } from "./EnvironmentDrawer";
import {
  fetchAllEnvironments,
  deleteEnvironmentApi,
} from "./environment-api-service";
import { useNotificationService } from "../../../service/notification-service";
import "../connection/ConnectionList.css";

const { Text, Title } = Typography;

/* ── Env type tag ── */
const EnvTypeTag = ({ type }) => {
  const config = {
    PROD: { color: "error", label: "PROD", icon: <FireFilled /> },
    STG: { color: "warning", label: "STG", icon: <ExperimentOutlined /> },
    DEV: { color: "blue", label: "DEV", icon: <CodeOutlined /> },
  };
  const c = config[type] || config.DEV;
  return (
    <Tag
      color={c.color}
      icon={c.icon}
      style={{ fontWeight: 700, fontSize: 10, letterSpacing: 0.4 }}
    >
      {c.label}
    </Tag>
  );
};

/* ── Status tag ── */
const StatusTag = ({ tested }) => {
  if (tested) {
    return (
      <Tag icon={<CheckCircleFilled />} color="success">
        Healthy
      </Tag>
    );
  }
  return (
    <Tag icon={<ExclamationCircleFilled />} color="warning">
      Untested
    </Tag>
  );
};

const EnvList = () => {
  const csrfToken = Cookies.get("csrftoken");
  const { selectedOrgId } = orgStore();
  const axiosRef = useAxiosPrivate();
  const { token } = theme.useToken();
  const { notify } = useNotificationService();
  const pageRef = useRef(null);

  const canWrite = checkPermission("ENVIRONMENT", "can_write");
  const canDelete = checkPermission("ENVIRONMENT", "can_delete");

  const {
    currentPage,
    pageSize,
    totalCount,
    setTotalCount,
    setCurrentPage,
    setPageSize,
  } = usePagination();

  const [envDataList, setEnvDataList] = useState([]);
  const [loading, setLoading] = useState(false);
  const [isDrawerOpen, setIsDrawerOpen] = useState(false);
  const [envId, setEnvId] = useState("");
  const [searchQuery, setSearchQuery] = useState("");
  const [filterType, setFilterType] = useState(null);
  const [testingIds, setTestingIds] = useState({});

  /* ── Test environment's connection ── */
  const handleTestEnv = async (envRecord) => {
    const connId = envRecord.connection?.id;
    if (!connId) return;
    setTestingIds((prev) => ({ ...prev, [envRecord.id]: true }));
    try {
      await axiosRef({
        method: "GET",
        url: `/api/v1/visitran/${
          selectedOrgId || "default_org"
        }/connection/${connId}/test`,
      });
      notify({ type: "success", message: "Connection test passed" });
      getEnvData();
    } catch (error) {
      notify({ error });
    } finally {
      setTestingIds((prev) => {
        // eslint-disable-next-line no-unused-vars
        const { [envRecord.id]: _, ...rest } = prev;
        return rest;
      });
    }
  };

  /* ── Fetch environments ── */
  const getEnvData = useCallback(
    async (page = currentPage, limit = pageSize) => {
      setLoading(true);
      try {
        const data = await fetchAllEnvironments(
          axiosRef,
          selectedOrgId,
          page,
          limit
        );
        const { total_items, current_page, page_items } = data;
        setEnvDataList(page_items);
        setTotalCount(total_items);
        setCurrentPage(current_page);
      } catch (error) {
        notify({ error });
      } finally {
        setLoading(false);
      }
    },
    [selectedOrgId]
  );

  useEffect(() => {
    getEnvData();
  }, [getEnvData]);

  /* ── Client-side filtering ── */
  const filteredData = useMemo(() => {
    let data = envDataList;
    if (searchQuery) {
      const term = searchQuery.toLowerCase();
      data = data.filter(
        (e) =>
          e.name?.toLowerCase().includes(term) ||
          e.description?.toLowerCase().includes(term)
      );
    }
    if (filterType) {
      data = data.filter((e) => e.deployment_type === filterType);
    }
    return data;
  }, [envDataList, searchQuery, filterType]);

  /* ── Delete ── */
  const deleteEnv = async (id) => {
    try {
      const res = await deleteEnvironmentApi(
        axiosRef,
        selectedOrgId,
        csrfToken,
        id
      );
      if (res.status === "success") {
        notify({
          type: "success",
          message: "Environment deleted successfully",
        });
        getEnvData();
      } else {
        notify({ message: res.message });
      }
    } catch (error) {
      notify({ error });
    }
  };

  /* ── Pagination ── */
  const handlePagination = (newPage, newPageSize) => {
    if (currentPage !== newPage || pageSize !== newPageSize) {
      setCurrentPage(newPage);
      setPageSize(newPageSize);
      getEnvData(newPage, newPageSize);
    }
  };

  /* ── Table columns ── */
  const columns = useMemo(
    () => [
      {
        title: "Name",
        dataIndex: "name",
        key: "name",
        sorter: (a, b) => (a.name || "").localeCompare(b.name || ""),
        render: (name, record) => {
          const conn = record.connection;
          return (
            <Space size={8}>
              {conn?.db_icon && (
                <div
                  className="conn-db-icon-wrap"
                  style={{ width: 32, height: 32, borderRadius: 6 }}
                >
                  <img
                    src={conn.db_icon}
                    alt={conn.datasource_name}
                    width={20}
                    height={20}
                    style={{ objectFit: "contain" }}
                  />
                </div>
              )}
              <Space direction="vertical" size={1}>
                <Text style={{ fontWeight: 500 }}>{name}</Text>
                {record.description && (
                  <Text type="secondary" style={{ fontSize: 11 }}>
                    {record.description}
                  </Text>
                )}
              </Space>
            </Space>
          );
        },
      },
      {
        title: "Type",
        dataIndex: "deployment_type",
        key: "type",
        width: 110,
        render: (type) => <EnvTypeTag type={type} />,
      },
      {
        title: "Connection",
        key: "connection",
        width: 250,
        render: (_, record) => {
          const conn = record.connection;
          if (!conn) return <Text type="secondary">—</Text>;
          return (
            <Space size={6}>
              <div
                className="conn-db-icon-wrap"
                style={{ width: 28, height: 28, borderRadius: 4 }}
              >
                <img
                  src={conn.db_icon}
                  alt={conn.datasource_name}
                  width={18}
                  height={18}
                  style={{ objectFit: "contain" }}
                />
              </div>
              <Space direction="vertical" size={0}>
                <Text style={{ fontSize: 12, fontWeight: 500 }}>
                  {conn.name}
                </Text>
                {conn.host && (
                  <Text
                    type="secondary"
                    style={{ fontSize: 11, fontFamily: "monospace" }}
                  >
                    {conn.host}
                  </Text>
                )}
              </Space>
            </Space>
          );
        },
      },
      {
        title: "Status",
        key: "status",
        width: 110,
        render: (_, record) => <StatusTag tested={record.is_tested} />,
      },
      {
        title: "Used by",
        key: "usedBy",
        width: 140,
        render: (_, record) => {
          const jobs = record.job_count || 0;
          const projects = record.project_count || 0;
          if (jobs === 0 && projects === 0) {
            return (
              <Text
                type="secondary"
                style={{ fontSize: 12, fontStyle: "italic" }}
              >
                Not used
              </Text>
            );
          }
          return (
            <Text type="secondary" style={{ fontSize: 12 }}>
              {jobs > 0 && `${jobs} job${jobs !== 1 ? "s" : ""}`}
              {jobs > 0 && projects > 0 && " · "}
              {projects > 0 &&
                `${projects} project${projects !== 1 ? "s" : ""}`}
            </Text>
          );
        },
      },
      {
        title: "Actions",
        key: "actions",
        width: 140,
        align: "right",
        render: (_, record) => (
          <Space size={2}>
            <Tooltip title="Test connection">
              <Button
                type="text"
                size="small"
                icon={<ThunderboltOutlined />}
                style={{ color: "#f59e0b" }}
                onClick={() => handleTestEnv(record)}
                loading={testingIds[record.id]}
              />
            </Tooltip>
            <Tooltip title="Edit">
              <Button
                type="text"
                size="small"
                icon={<EditOutlined />}
                disabled={!canWrite}
                onClick={() => {
                  setEnvId(record.id);
                  setIsDrawerOpen(true);
                }}
              />
            </Tooltip>
            <Popconfirm
              title="Delete environment?"
              description={
                (record.job_count || 0) > 0
                  ? `"${record.name}" is used by ${record.job_count} job(s)${
                      record.project_count
                        ? ` and ${record.project_count} project(s)`
                        : ""
                    }. Deleting may break scheduled runs.`
                  : `"${record.name}" will be permanently removed.`
              }
              okText="Delete"
              okButtonProps={{ danger: true }}
              onConfirm={() => deleteEnv(record.id)}
              disabled={!canDelete}
            >
              <Tooltip title="Delete">
                <Button
                  type="text"
                  size="small"
                  icon={<DeleteOutlined />}
                  danger
                  disabled={!canDelete}
                />
              </Tooltip>
            </Popconfirm>
          </Space>
        ),
      },
    ],
    [canWrite, canDelete, token, testingIds]
  );

  const hasActiveFilters = searchQuery || filterType;

  return (
    <div className="conn-page" ref={pageRef}>
      <div className="conn-page-inner">
        {/* Header */}
        <Row justify="space-between" align="top" style={{ marginBottom: 20 }}>
          <Col>
            <Title level={3} style={{ margin: 0 }}>
              Environments
            </Title>
            <Text type="secondary">
              Where your jobs run. Each environment wraps a connection and a
              deployment mode.
            </Text>
          </Col>
          <Col>
            <Button
              type="primary"
              icon={<PlusOutlined />}
              onClick={() => {
                setEnvId("");
                setIsDrawerOpen(true);
              }}
              disabled={!canWrite}
            >
              New Environment
            </Button>
          </Col>
        </Row>

        {/* Filter bar */}
        <Card
          size="small"
          className="conn-filter-bar"
          styles={{ body: { padding: 10 } }}
        >
          <Row gutter={[8, 8]} align="middle" wrap>
            <Col flex="280px">
              <Input
                size="small"
                placeholder="Search environments..."
                prefix={<SearchOutlined />}
                allowClear
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
              />
            </Col>
            <Col>
              <Select
                size="small"
                placeholder="Type"
                style={{ width: 120 }}
                allowClear
                value={filterType}
                onChange={setFilterType}
                options={[
                  { value: "PROD", label: "Production" },
                  { value: "STG", label: "Staging" },
                  { value: "DEV", label: "Development" },
                ]}
              />
            </Col>
            {hasActiveFilters && (
              <Col>
                <Button
                  type="link"
                  size="small"
                  onClick={() => {
                    setSearchQuery("");
                    setFilterType(null);
                  }}
                >
                  Clear filters
                </Button>
              </Col>
            )}
            <Col flex="auto" style={{ textAlign: "right" }}>
              <Space size={8}>
                <Text type="secondary" style={{ fontSize: 12 }}>
                  {filteredData.length} environment
                  {filteredData.length !== 1 ? "s" : ""}
                </Text>
                <Tooltip title="Refresh">
                  <Button
                    type="text"
                    size="small"
                    icon={<ReloadOutlined spin={loading} />}
                    onClick={() => getEnvData(currentPage, pageSize)}
                    disabled={loading}
                  />
                </Tooltip>
              </Space>
            </Col>
          </Row>
        </Card>

        {/* Table */}
        <Table
          columns={columns}
          dataSource={filteredData}
          loading={loading}
          rowKey="id"
          size="middle"
          pagination={false}
          locale={{
            emptyText: (
              <Empty
                image={Empty.PRESENTED_IMAGE_SIMPLE}
                description="No environments found"
              />
            ),
          }}
        />

        {filteredData.length > 0 && (
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
                `Showing ${range[0]}\u2013${range[1]} of ${total}`
              }
              showSizeChanger
              onChange={handlePagination}
            />
          </div>
        )}
      </div>

      {/* Environment Drawer */}
      <EnvironmentDrawer
        open={isDrawerOpen}
        onClose={() => {
          setIsDrawerOpen(false);
          setEnvId("");
        }}
        envId={envId}
        getContainer={() => pageRef.current || document.body}
        onSaved={() => getEnvData(currentPage, pageSize)}
      />
    </div>
  );
};

EnvList.displayName = "EnvList";

export default EnvList;
