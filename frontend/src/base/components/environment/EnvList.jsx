import { useState, useEffect, useCallback } from "react";
import {
  Button,
  Table,
  Space,
  Typography,
  Tooltip,
  Tag,
  Modal,
  Pagination,
} from "antd";
import {
  EditOutlined,
  DeleteOutlined,
  PlusOutlined,
  ReloadOutlined,
} from "@ant-design/icons";
import Cookies from "js-cookie";

import { orgStore } from "../../../store/org-store";
import { useAxiosPrivate } from "../../../service/axios-service";
import { checkPermission } from "../../../common/helpers";
import { usePagination } from "../../../widgets/hooks/usePagination";
import NewEnv from "./NewEnv";
import {
  fetchAllEnvironments,
  deleteEnvironmentApi,
} from "./environment-api-service";
import "./environment.css";
import { useNotificationService } from "../../../service/notification-service";

const EnvList = () => {
  const csrfToken = Cookies.get("csrftoken");
  const { selectedOrgId } = orgStore();
  const axiosRef = useAxiosPrivate();

  const {
    currentPage,
    pageSize,
    totalCount,
    setTotalCount,
    setCurrentPage,
    setPageSize,
  } = usePagination();

  const [envDataList, setEnvDataList] = useState([]);
  const [isDeleteModalOpen, setIsDeleteModalOpen] = useState(false);
  const [isNewEnvModalOpen, setIsNewEnvModalOpen] = useState(false);
  const [envId, setEnvId] = useState("");
  const [loading, setLoading] = useState(false);
  const { notify } = useNotificationService();

  const can_delete = checkPermission("ENVIRONMENT", "can_delete");
  const can_write = checkPermission("ENVIRONMENT", "can_write");

  const getColor = (type) => {
    switch (type.toLowerCase()) {
      case "prod":
        return "green";
      case "stg":
        return "blue";
      default:
        return "yellow";
    }
  };

  const columns = [
    {
      title: "Name",
      dataIndex: "name",
      key: "name",
      render: (_, { connection, name }) => (
        <div className="dFlex">
          <Tooltip
            title={connection.datasource_name}
            key={connection.datasource_name}
          >
            <img
              src={connection.db_icon}
              alt={connection.datasource_name}
              height={20}
              width={20}
              className="mr5"
            />
          </Tooltip>
          <Typography>{name}</Typography>
        </div>
      ),
    },
    {
      title: "Description",
      dataIndex: "description",
      key: "description",
    },
    {
      title: "Deployment type",
      dataIndex: "deployment_type",
      key: "deployment_type",
      render: (_, record) => (
        <Tag
          color={getColor(record.deployment_type)}
          key={record.deployment_type}
        >
          {record.deployment_type}
        </Tag>
      ),
    },
    {
      title: "Connection",
      dataIndex: "connection.name",
      key: "connection.name",
      render: (_, { connection }) => <Typography>{connection.name}</Typography>,
    },
    {
      title: "Action",
      key: "action",
      render: (_, record) => (
        <Space size="middle">
          <Tooltip title="Edit" key="Edit">
            <Button
              icon={<EditOutlined />}
              type="text"
              disabled={!can_write}
              onClick={() => {
                setEnvId(record.id);
                setIsNewEnvModalOpen(true);
              }}
            />
          </Tooltip>

          <Tooltip title="Delete" key="Delete">
            <Button
              icon={<DeleteOutlined />}
              type="text"
              danger
              disabled={!can_delete}
              onClick={() => setIsDeleteModalOpen(record.id)}
            />
          </Tooltip>
        </Space>
      ),
    },
  ];

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
        console.error(error);
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

  const deleteEnv = async (id) => {
    try {
      const res = await deleteEnvironmentApi(
        axiosRef,
        selectedOrgId,
        csrfToken,
        id
      );
      if (res.status === "success") {
        getEnvData();
      } else {
        notify({
          message: res.message,
        });
      }
    } catch (error) {
      console.error(error);
      notify({ error });
    } finally {
      setIsDeleteModalOpen(false);
    }
  };

  const handlePagination = (newPage, newPageSize) => {
    if (currentPage !== newPage || pageSize !== newPageSize) {
      setCurrentPage(newPage);
      setPageSize(newPageSize);
      getEnvData(newPage, newPageSize);
    }
  };

  return (
    <div className="envListContainer height-100 overflow-y-auto">
      <div className="listPageTopbar envTable">
        <Typography className="myHeading">Environments</Typography>
        <Space size="middle">
          <Button
            className="primary_button_style"
            type="primary"
            onClick={() => {
              setEnvId("");
              setIsNewEnvModalOpen(true);
            }}
            icon={<PlusOutlined />}
            disabled={!can_write}
          >
            Create Environment
          </Button>
          <Tooltip title="Refresh">
            <Button
              icon={<ReloadOutlined spin={loading} />}
              onClick={() => getEnvData(currentPage, pageSize)}
              disabled={loading}
            />
          </Tooltip>
        </Space>
      </div>

      <Table
        columns={columns}
        dataSource={envDataList}
        className="envTable"
        loading={loading}
        bordered
        rowKey="id"
        pagination={false}
      />
      {envDataList.length > 0 && (
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
              )} entries`
            }
            showSizeChanger
            onChange={handlePagination}
          />
        </Space>
      )}

      <Modal
        title="Delete Environment"
        open={isDeleteModalOpen}
        onOk={() => deleteEnv(isDeleteModalOpen)}
        onCancel={() => setIsDeleteModalOpen(false)}
        okText="Delete"
        centered
        okButtonProps={{ danger: true }}
        maskClosable={false}
      >
        <p>Are you sure you want to delete this environment?</p>
      </Modal>

      <Modal
        title="New Environment"
        open={isNewEnvModalOpen}
        onCancel={() => {
          setIsNewEnvModalOpen(false);
          setEnvId("");
        }}
        footer={null}
        centered
        width={600}
        maskClosable={false}
        destroyOnClose={true}
      >
        <NewEnv
          id={envId}
          setIsEnvModalOpen={setIsNewEnvModalOpen}
          getAllEnvironments={getEnvData}
        />
      </Modal>
    </div>
  );
};

EnvList.displayName = "EnvList";

export default EnvList;
