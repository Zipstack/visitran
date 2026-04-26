/* eslint-disable react/prop-types */
import Cookies from "js-cookie";
import { useState, useEffect, useMemo, useCallback, useRef } from "react";
import {
  Button,
  Table,
  Space,
  Tooltip,
  Typography,
  Modal,
  Pagination,
  Tag,
  Input,
  Select,
  Card,
  Row,
  Col,
  Popconfirm,
  Empty,
  theme,
} from "antd";
import { useLocation } from "react-router-dom";
import {
  EditOutlined,
  PlusOutlined,
  DeleteOutlined,
  ReloadOutlined,
  SearchOutlined,
  CheckCircleFilled,
  ExclamationCircleFilled,
  CloseCircleFilled,
  ThunderboltOutlined,
  DatabaseOutlined,
  AppstoreOutlined,
} from "@ant-design/icons";

import { orgStore } from "../../../store/org-store";
import { useAxiosPrivate } from "../../../service/axios-service";
import { ConnectionDrawer } from "./ConnectionDrawer";
import { checkPermission } from "../../../common/helpers";
import { deleteConnection } from "../environment/environment-api-service";
import { useNotificationService } from "../../../service/notification-service";
import { usePagination } from "../../../widgets/hooks/usePagination";
import "./ConnectionList.css";

const { Text, Title } = Typography;

/* ── Status tag component ── */
const StatusTag = ({ flag }) => {
  if (flag === "GREEN")
    return (
      <Tag icon={<CheckCircleFilled />} color="success">
        Healthy
      </Tag>
    );
  if (flag === "YELLOW")
    return (
      <Tag icon={<ExclamationCircleFilled />} color="warning">
        Stale
      </Tag>
    );
  if (flag === "RED")
    return (
      <Tag icon={<CloseCircleFilled />} color="error">
        Error
      </Tag>
    );
  return <Tag color="default">Unknown</Tag>;
};

const ConnectionList = () => {
  const { selectedOrgId } = orgStore();
  const csrfToken = Cookies.get("csrftoken");
  const axios = useAxiosPrivate();
  const location = useLocation();
  const { cId } = location.state || {};
  const { token } = theme.useToken();
  const { notify } = useNotificationService();

  const canWrite = checkPermission("CONNECTION", "can_write");
  const canDelete = checkPermission("CONNECTION", "can_delete");

  const [connectionDataList, setConnectionDataList] = useState([]);
  const [loading, setLoading] = useState(false);
  const [isDrawerOpen, setIsDrawerOpen] = useState(false);
  const [connectionId, setConnectionId] = useState("");
  const [isDeleteModalOpen, setIsDeleteModalOpen] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [filterDb, setFilterDb] = useState(null);
  const [filterStatus, setFilterStatus] = useState(null);
  const [testingIds, setTestingIds] = useState({});

  const {
    currentPage,
    pageSize,
    totalCount,
    setTotalCount,
    setCurrentPage,
    setPageSize,
  } = usePagination();

  /* ── Fetch connections ── */
  const getConnectionData = useCallback(
    async (page = currentPage, limit = pageSize) => {
      setLoading(true);
      try {
        const res = await axios({
          method: "GET",
          url: `/api/v1/visitran/${selectedOrgId || "default_org"}/connections`,
          params: { page, limit },
        });
        const { total_items, current_page, page_items } = res.data.data;
        setConnectionDataList(page_items);
        setTotalCount(total_items);
        setCurrentPage(current_page);
        setPageSize(limit);
      } catch (error) {
        notify({ error });
      } finally {
        setLoading(false);
      }
    },
    [selectedOrgId, currentPage, pageSize]
  );

  useEffect(() => {
    getConnectionData();
  }, [selectedOrgId]);

  useEffect(() => {
    if (cId) {
      setConnectionId(cId);
      setIsDrawerOpen(true);
    }
  }, [cId]);

  /* ── Client-side filtering ── */
  const filteredData = useMemo(() => {
    let data = connectionDataList;
    if (searchQuery) {
      const term = searchQuery.toLowerCase();
      data = data.filter(
        (c) =>
          c.name?.toLowerCase().includes(term) ||
          c.description?.toLowerCase().includes(term)
      );
    }
    if (filterDb) {
      data = data.filter((c) => c.datasource_name === filterDb);
    }
    if (filterStatus) {
      data = data.filter((c) => c.connection_flag === filterStatus);
    }
    return data;
  }, [connectionDataList, searchQuery, filterDb, filterStatus]);

  /* ── Unique DB types for filter dropdown ── */
  const dbOptions = useMemo(() => {
    const types = [
      ...new Set(connectionDataList.map((c) => c.datasource_name)),
    ];
    return types.map((t) => ({
      label: t.charAt(0).toUpperCase() + t.slice(1),
      value: t,
    }));
  }, [connectionDataList]);

  /* ── Delete connection ── */
  const deleteConn = async (id) => {
    setLoading(true);
    try {
      const res = await deleteConnection(axios, selectedOrgId, csrfToken, id);
      if (res.status === "success") {
        notify({ type: "success", message: "Connection deleted successfully" });
        const isLastItem = connectionDataList.length === 1;
        const targetPage =
          isLastItem && currentPage > 1 ? currentPage - 1 : currentPage;
        getConnectionData(targetPage, pageSize);
      } else {
        notify({ message: res.message });
      }
    } catch (error) {
      notify({ error });
    } finally {
      setLoading(false);
      setIsDeleteModalOpen(false);
    }
  };

  /* ── Test connection inline ── */
  const handleTestConnection = async (connId) => {
    setTestingIds((prev) => ({ ...prev, [connId]: true }));
    try {
      await axios({
        method: "GET",
        url: `/api/v1/visitran/${
          selectedOrgId || "default_org"
        }/connection/${connId}/test`,
      });
      notify({ type: "success", message: "Connection test passed" });
      getConnectionData(currentPage, pageSize);
    } catch (error) {
      notify({ error });
      getConnectionData(currentPage, pageSize);
    } finally {
      setTestingIds((prev) => {
        // eslint-disable-next-line no-unused-vars
        const { [connId]: _, ...rest } = prev;
        return rest;
      });
    }
  };

  /* ── Handle drawer open/close ── */
  const openCreate = () => {
    setConnectionId("");
    setIsDrawerOpen(true);
  };

  const openEdit = (record) => {
    setConnectionId(record.id);
    setIsDrawerOpen(true);
  };

  const handleDrawerClose = () => {
    setIsDrawerOpen(false);
    setConnectionId("");
  };

  /* ── Pagination ── */
  const handlePagination = (newPage, newPageSize) => {
    if (currentPage !== newPage || pageSize !== newPageSize) {
      setCurrentPage(newPage);
      setPageSize(newPageSize);
      getConnectionData(newPage, newPageSize);
    }
  };

  /* ── Table columns ── */
  const columns = useMemo(
    () => [
      {
        title: "Connection",
        dataIndex: "name",
        key: "name",
        sorter: (a, b) => (a.name || "").localeCompare(b.name || ""),
        render: (name, record) => (
          <div className="conn-name-cell">
            <Tooltip title={record.datasource_name}>
              <div className="conn-db-icon-wrap">
                <img
                  src={record.db_icon}
                  alt={record.datasource_name}
                  className="conn-db-icon"
                />
              </div>
            </Tooltip>
            <Space direction="vertical" size={1}>
              <Space size={6}>
                <Text className="conn-name-text">{name}</Text>
                <Tag
                  style={{
                    fontSize: 10,
                    margin: 0,
                    fontWeight: 500,
                  }}
                >
                  {record.datasource_name?.charAt(0).toUpperCase() +
                    record.datasource_name?.slice(1)}
                </Tag>
              </Space>
              {record.description && (
                <Text type="secondary" style={{ fontSize: 11 }}>
                  {record.description}
                </Text>
              )}
              {record.host && (
                <Text
                  type="secondary"
                  style={{ fontSize: 11, fontFamily: "monospace" }}
                >
                  {record.host}
                </Text>
              )}
            </Space>
          </div>
        ),
      },
      {
        title: "Status",
        dataIndex: "connection_flag",
        key: "status",
        width: 120,
        render: (flag) => <StatusTag flag={flag} />,
      },
      {
        title: "Used by",
        key: "usedBy",
        width: 180,
        render: (_, record) => {
          const envCount = record.env_count || 0;
          const projCount = record.project_count || 0;
          if (envCount === 0 && projCount === 0) {
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
            <Space direction="vertical" size={1}>
              <Space size={4}>
                <DatabaseOutlined
                  style={{ color: token.colorPrimary, fontSize: 12 }}
                />
                <Text style={{ fontSize: 12 }}>
                  <strong>{envCount}</strong> environment
                  {envCount !== 1 ? "s" : ""}
                </Text>
              </Space>
              <Space size={4}>
                <AppstoreOutlined style={{ color: "#8b5cf6", fontSize: 12 }} />
                <Text style={{ fontSize: 12 }}>
                  <strong>{projCount}</strong> project
                  {projCount !== 1 ? "s" : ""}
                </Text>
              </Space>
            </Space>
          );
        },
      },
      {
        title: "Last modified",
        key: "last_modified",
        width: 150,
        render: (_, record) => {
          const modBy = record.last_modified_by?.name || "—";
          return (
            <Space direction="vertical" size={0}>
              <Text style={{ fontSize: 12 }}>{modBy}</Text>
            </Space>
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
                loading={testingIds[record.id]}
                onClick={() => handleTestConnection(record.id)}
                disabled={record.is_sample_project}
              />
            </Tooltip>
            <Tooltip title="Edit">
              <Button
                type="text"
                size="small"
                icon={<EditOutlined />}
                onClick={() => openEdit(record)}
                disabled={!canWrite || record.is_sample_project}
              />
            </Tooltip>
            <Popconfirm
              title="Delete connection?"
              description="This action cannot be undone."
              okText="Delete"
              okButtonProps={{ danger: true }}
              onConfirm={() => deleteConn(record.id)}
              disabled={!canDelete || record.is_sample_project}
            >
              <Tooltip title="Delete">
                <Button
                  type="text"
                  size="small"
                  icon={<DeleteOutlined />}
                  danger
                  disabled={!canDelete || record.is_sample_project}
                />
              </Tooltip>
            </Popconfirm>
          </Space>
        ),
      },
    ],
    [token, testingIds, canWrite, canDelete]
  );

  const hasActiveFilters = searchQuery || filterDb || filterStatus;

  const pageRef = useRef(null);

  return (
    <div className="conn-page" ref={pageRef}>
      <div className="conn-page-inner">
        {/* Header */}
        <Row justify="space-between" align="top" style={{ marginBottom: 20 }}>
          <Col>
            <Title level={3} style={{ margin: 0 }}>
              Connections
            </Title>
            <Text type="secondary">
              Database credentials used by environments. Store once, reuse
              across projects.
            </Text>
          </Col>
          <Col>
            <Space>
              <Button
                type="primary"
                icon={<PlusOutlined />}
                onClick={openCreate}
                disabled={!canWrite}
              >
                New Connection
              </Button>
            </Space>
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
                placeholder="Search connections..."
                prefix={<SearchOutlined />}
                allowClear
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
              />
            </Col>
            <Col>
              <Select
                size="small"
                placeholder="Database"
                style={{ width: 140 }}
                allowClear
                value={filterDb}
                onChange={setFilterDb}
                options={dbOptions}
              />
            </Col>
            <Col>
              <Select
                size="small"
                placeholder="Status"
                style={{ width: 120 }}
                allowClear
                value={filterStatus}
                onChange={setFilterStatus}
                options={[
                  { value: "GREEN", label: "Healthy" },
                  { value: "YELLOW", label: "Stale" },
                  { value: "RED", label: "Error" },
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
                    setFilterDb(null);
                    setFilterStatus(null);
                  }}
                >
                  Clear filters
                </Button>
              </Col>
            )}
            <Col flex="auto" style={{ textAlign: "right" }}>
              <Space size={8}>
                <Text type="secondary" style={{ fontSize: 12 }}>
                  {filteredData.length} connection
                  {filteredData.length !== 1 ? "s" : ""}
                </Text>
                <Tooltip title="Refresh">
                  <Button
                    type="text"
                    size="small"
                    icon={<ReloadOutlined spin={loading} />}
                    onClick={() => getConnectionData(currentPage, pageSize)}
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
          rowKey="id"
          loading={loading}
          size="middle"
          pagination={false}
          locale={{
            emptyText: (
              <Empty
                image={Empty.PRESENTED_IMAGE_SIMPLE}
                description="No connections found"
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

      {/* Connection Drawer */}
      <ConnectionDrawer
        open={isDrawerOpen}
        onClose={handleDrawerClose}
        connectionId={connectionId}
        getContainer={() => pageRef.current || document.body}
        onSaved={() => getConnectionData(currentPage, pageSize)}
      />

      {/* Delete confirmation */}
      <Modal
        title="Delete Connection"
        open={isDeleteModalOpen}
        onOk={() => deleteConn(isDeleteModalOpen)}
        onCancel={() => setIsDeleteModalOpen(false)}
        okText="Delete"
        centered
        okButtonProps={{ danger: true, loading }}
        maskClosable={false}
      >
        <Text>
          Are you sure you want to delete this connection? This action cannot be
          undone.
        </Text>
      </Modal>
    </div>
  );
};

export default ConnectionList;
