import Cookies from "js-cookie";
import { useState, useEffect } from "react";
import {
  Button,
  Table,
  Space,
  Tooltip,
  Typography,
  Modal,
  Pagination,
} from "antd";
import { useLocation } from "react-router-dom";
import {
  EditOutlined,
  PlusOutlined,
  DeleteOutlined,
  ReloadOutlined,
} from "@ant-design/icons";

import { orgStore } from "../../../store/org-store";
import { useAxiosPrivate } from "../../../service/axios-service";
import { CreateConnection } from "../environment/CreateConnection";
import { checkPermission } from "../../../common/helpers";
import {
  deleteConnection,
  deleteAllConnectionsApi,
} from "../environment/environment-api-service";
import { useNotificationService } from "../../../service/notification-service";
import { usePagination } from "../../../widgets/hooks/usePagination";
import "../environment/environment.css";
const ConnectionList = () => {
  const { selectedOrgId } = orgStore();
  const csrfToken = Cookies.get("csrftoken");

  const axios = useAxiosPrivate();
  const location = useLocation();
  const { cId } = location.state || {};
  const [connectionId, setConnectionId] = useState("");
  const [isDeleteModalOpen, setIsDeleteModalOpen] = useState(false);
  const [isDeleteAllModalOpen, setIsDeleteAllModalOpen] = useState(false);
  const [connectionDataList, setConnectionDataList] = useState([]);
  const [loading, setLoading] = useState(false);
  const [deleteAllLoading, setDeleteAllLoading] = useState(false);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const { notify } = useNotificationService();
  const can_write = checkPermission("CONNECTION", "can_write");
  const can_delete = checkPermission("CONNECTION", "can_delete");

  const {
    currentPage,
    pageSize,
    totalCount,
    setTotalCount,
    setCurrentPage,
    setPageSize,
  } = usePagination();

  const deleteConn = async (id) => {
    setLoading(true);
    try {
      const res = await deleteConnection(axios, selectedOrgId, csrfToken, id);
      if (res.status === "success") {
        notify({
          type: "success",
          message: "Connection deleted successfully",
        });
        // If last item on current page, go to previous page
        const isLastItemOnPage = connectionDataList.length === 1;
        const targetPage =
          isLastItemOnPage && currentPage > 1 ? currentPage - 1 : currentPage;
        getConnectionData(targetPage, pageSize);
      } else {
        notify({
          message: res.message,
        });
      }
    } catch (error) {
      console.error(error);
      notify({ error });
    } finally {
      setLoading(false);
      setIsDeleteModalOpen(false);
    }
  };

  const deleteAllConns = async () => {
    setDeleteAllLoading(true);
    try {
      const res = await deleteAllConnectionsApi(
        axios,
        selectedOrgId,
        csrfToken
      );
      if (res.status === "success") {
        const { deleted_count, skipped } = res.data;
        if (skipped.length > 0) {
          notify({
            type: "warning",
            message: `Deleted ${deleted_count} connection(s). ${
              skipped.length
            } skipped due to project dependencies: ${skipped.join(", ")}`,
          });
        } else {
          notify({
            type: "success",
            message: `All ${deleted_count} connection(s) deleted successfully`,
          });
        }
        getConnectionData(1, pageSize);
      } else {
        notify({ message: res.message });
      }
    } catch (error) {
      console.error(error);
      notify({ error });
    } finally {
      setDeleteAllLoading(false);
      setIsDeleteAllModalOpen(false);
    }
  };

  const columns = [
    {
      title: "Name ",
      dataIndex: "name",
      key: "name",
      render: (_, record) => (
        <Typography style={{ display: "flex" }}>
          <Tooltip title={record.datasource_name} key={record.datasource_name}>
            <img
              src={record.db_icon}
              alt={record.datasource_name}
              height={20}
              width={20}
              style={{ marginRight: "5px" }}
            />
          </Tooltip>
          <Typography>{record.name}</Typography>
        </Typography>
      ),
    },

    {
      title: "Description",
      dataIndex: "description",
      key: "description",
    },
    // {
    //   title: "Connection",
    //   dataIndex: "created_by.name",
    //   key: "created_by.name",
    //   render: (_, { last_modified_by }) => (
    //     <Typography>{last_modified_by.name || "N/A"}</Typography>
    //   ),
    // },

    {
      title: "Action",
      key: "action",
      render: (_, record) => (
        <Space size="middle">
          <Tooltip title={record.is_sample_project ? "" : "Edit"} key="Edit">
            <Button
              icon={<EditOutlined />}
              type="text"
              disabled={!can_write || record.is_sample_project}
              onClick={() => {
                if (!record.is_sample_project) {
                  setConnectionId(record.id);
                  setIsModalOpen(true);
                }
              }}
            />
          </Tooltip>
          <Tooltip title="Delete" key="Delete">
            <Button
              icon={<DeleteOutlined />}
              type="text"
              danger
              disabled={!can_delete || record.is_sample_project}
              onClick={() => setIsDeleteModalOpen(record.id)}
            />
          </Tooltip>
        </Space>
      ),
    },
  ];

  const getConnectionData = async (page = currentPage, limit = pageSize) => {
    setLoading(true);
    try {
      const requestOptions = {
        method: "GET",
        url: `/api/v1/visitran/${selectedOrgId || "default_org"}/connections`,
        params: {
          page,
          limit,
        },
      };
      const res = await axios(requestOptions);
      const { total_items, current_page } = res.data.data;
      setConnectionDataList(res.data.data.page_items);
      setTotalCount(total_items); // Save the total count for pagination
      setCurrentPage(current_page);
    } catch (error) {
      console.error(error);
      notify({ error });
    } finally {
      setLoading(false);
    }
  };
  useEffect(() => {
    getConnectionData();
  }, [selectedOrgId]);

  useEffect(() => {
    if (cId) {
      setIsModalOpen(true);
    }
  }, [cId]);

  const handlePagination = (newPage, newPageSize) => {
    if (currentPage !== newPage || pageSize !== newPageSize) {
      setCurrentPage(newPage);
      setPageSize(newPageSize);
      getConnectionData(newPage, newPageSize);
    }
  };
  return (
    <div className="envListContainer height-100 overflow-y-auto">
      <div className="listPageTopbar envTable">
        <Typography className="myHeading">Connections</Typography>
        {!cId && (
          <Space size="middle">
            {connectionDataList.length > 0 && (
              <Button
                danger
                onClick={() => setIsDeleteAllModalOpen(true)}
                icon={<DeleteOutlined />}
                disabled={!can_delete || loading}
              >
                Delete All
              </Button>
            )}
            <Button
              type="primary"
              onClick={() => {
                setConnectionId("");
                setIsModalOpen(true);
              }}
              icon={<PlusOutlined />}
              className="primary_button_style"
              disabled={!can_write}
            >
              Create Connection
            </Button>
            <Tooltip title="Refresh">
              <Button
                icon={<ReloadOutlined spin={loading} />}
                onClick={() => getConnectionData(currentPage, pageSize)}
                disabled={loading}
              />
            </Tooltip>
          </Space>
        )}
      </div>

      <Table
        loading={loading}
        columns={columns}
        dataSource={connectionDataList}
        className="envTable"
        bordered
        rowKey="id"
        pagination={false}
      />
      {connectionDataList.length > 0 && (
        <Space className="envTable flex-justify-right pad-10-top">
          <Pagination
            className="custom-pagination"
            current={currentPage}
            pageSize={pageSize}
            total={Math.min(totalCount, 1000)}
            showTotal={(total, range) =>
              `Showing ${range[0]} to ${range[1]} of ${Math.min(
                totalCount,
                1000
              )}entries`
            }
            showSizeChanger
            onChange={handlePagination}
          />
        </Space>
      )}
      <Modal
        title="Delete Connection"
        open={isDeleteModalOpen}
        onOk={() => deleteConn(isDeleteModalOpen)}
        onCancel={() => setIsDeleteModalOpen(false)}
        okText="Delete"
        centered
        okButtonProps={{ danger: true, loading: loading }}
        maskClosable={false}
      >
        <Typography.Paragraph>
          Are you sure you want to delete this connection?
        </Typography.Paragraph>
      </Modal>

      <Modal
        title="Delete All Connections"
        open={isDeleteAllModalOpen}
        onOk={deleteAllConns}
        onCancel={() => setIsDeleteAllModalOpen(false)}
        okText="Delete All"
        centered
        okButtonProps={{ danger: true, loading: deleteAllLoading }}
        maskClosable={false}
      >
        <Typography.Paragraph>
          Are you sure you want to delete all connections? Connections with
          active project dependencies will be skipped. This action cannot be
          undone.
        </Typography.Paragraph>
      </Modal>

      <Modal
        title={cId || connectionId ? "Edit Connection" : "Create Connection"}
        open={isModalOpen}
        onCancel={() => {
          setIsModalOpen(false);
          setConnectionId("");
        }}
        footer={null}
        centered
        width={1000}
        maskClosable={false}
      >
        <CreateConnection
          key={`${connectionId || cId}-${Date.now()}`} // Force remount for fresh API calls
          connectionId={connectionId || cId}
          setIsModalOpen={setIsModalOpen}
          setConnectionId={setConnectionId}
          getAllConnection={getConnectionData}
        />
      </Modal>
    </div>
  );
};

export default ConnectionList;
