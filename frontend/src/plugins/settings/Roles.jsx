import { useState, useEffect } from "react";
import Cookies from "js-cookie";
import { Table, Typography, Space, Button, Modal, Input } from "antd";
import {
  PlusOutlined,
  DeleteOutlined,
  SyncOutlined,
  EditOutlined,
} from "@ant-design/icons";
import PermissionRestricted from "./PermissionRestricted";

import { orgStore } from "../../store/org-store";
import { useAxiosPrivate } from "../../service/axios-service";
import { useNotificationService } from "../../service/notification-service";
const Roles = () => {
  const { Text } = Typography;
  const axios = useAxiosPrivate();
  const csrfToken = Cookies.get("csrftoken");
  const [isRestricted, setIsRestricted] = useState(false);
  const [roles, setRoles] = useState([]);
  const { selectedOrgId } = orgStore();
  const [isDeleteModalOpen, setIsDeleteModalOpen] = useState("");
  const [roleForm, setRoleForm] = useState({ name: "", description: "" });
  const [isRoleModalOpen, setIsRoleModalOpen] = useState(false);
  const [editingRole, setEditingRole] = useState(null);
  const { notify } = useNotificationService();

  const [isLoading, setIsLoading] = useState(false);
  const [isSyncing, setIsSyncing] = useState(false);

  const roleColumns = [
    {
      dataIndex: "name",
      key: "name",
      render: (_, record) => (
        <div>
          <Text strong>{record.name}</Text>
          <br />
          <Text type="secondary">{record.description}</Text>
        </div>
      ),
    },
    {
      dataIndex: "",
      key: "action",
      render: (_, record) => (
        <Space>
          <Button
            icon={<EditOutlined />}
            onClick={() => {
              setEditingRole(record);
              setRoleForm({
                name: record.name,
                description: record.description,
              });
              setIsRoleModalOpen(true);
            }}
          />
          <Button
            icon={<DeleteOutlined />}
            onClick={() => setIsDeleteModalOpen(record.id)}
          />
        </Space>
      ),
      width: "150px",
    },
  ];

  const handleDeleteUser = (id) => {
    setIsLoading(true);
    axios({
      method: "DELETE",
      url: `/api/v1/visitran/${selectedOrgId}/uac/role/${id}/delete`,
      headers: { "X-CSRFToken": csrfToken },
    })
      .then((res) => {
        getAllRoles();
      })
      .catch((error) => {
        console.error(error);
        notify({ error });
      })
      .finally(() => {
        setIsLoading(false);
      });
  };

  const updateRole = () => {
    setIsLoading(true);
    axios({
      method: "PUT",
      url: `/api/v1/visitran/${selectedOrgId}/uac/role/${editingRole.id}/update`,
      headers: { "X-CSRFToken": csrfToken },
      data: roleForm,
    })
      .then(() => {
        notify({
          type: "success",
          message: "Success",
          description: "Role updated successfully",
        });
        getAllRoles();
      })
      .catch((error) => {
        console.error(error);
        notify({ error });
      })
      .finally(() => {
        setIsLoading(false);
        setIsRoleModalOpen(false);
        setEditingRole(null);
        setRoleForm({ name: "", description: "" });
      });
  };

  const createRole = () => {
    setIsLoading(true);
    axios({
      method: "POST",
      url: `/api/v1/visitran/${selectedOrgId}/uac/role/create`,
      headers: { "X-CSRFToken": csrfToken },
      data: roleForm,
    })
      .then(() => {
        notify({
          type: "success",
          message: "Success",
          description: "Role created Successfully",
        });
        getAllRoles();
      })
      .catch((error) => {
        console.error(error);
        notify({ error });
      })
      .finally(() => {
        setIsLoading(false);
        setIsRoleModalOpen(false);
        setRoleForm({ name: "", description: "" });
      });
  };

  const getAllRoles = () => {
    setIsLoading(true);
    axios({
      method: "GET",
      url: `/api/v1/visitran/${selectedOrgId}/uac/roles`,
    })
      .then((res) => {
        const data = res?.data?.data;
        setRoles(data);
      })
      .catch((error) => {
        if (error?.response?.status === 403) {
          setIsRestricted(true);
          return;
        }
        notify({ error });
      })
      .finally(() => {
        setIsLoading(false);
      });
  };

  useEffect(() => {
    getAllRoles();
  }, []);

  const handleRoleChange = (value, name) => {
    setRoleForm({ ...roleForm, [name]: value });
  };

  const syncFromScalekit = () => {
    setIsSyncing(true);
    axios({
      method: "POST",
      url: `/api/v1/visitran/${selectedOrgId}/uac/role/sync/scalekit/`,
      headers: { "X-CSRFToken": csrfToken },
    })
      .then(() => {
        notify({
          type: "success",
          message: "Success",
          description: "Roles synced from Scalekit successfully",
        });
        getAllRoles();
      })
      .catch((error) => {
        console.error(error);
        notify({ error });
      })
      .finally(() => {
        setIsSyncing(false);
      });
  };

  if (isRestricted) return <PermissionRestricted />;

  return (
    <div className="roles_wrapper">
      <div
        style={{
          marginBottom: 16,
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
        }}
      >
        <Typography.Title level={3}>Roles</Typography.Title>
        <Space>
          <Button
            icon={<SyncOutlined spin={isSyncing} />}
            onClick={syncFromScalekit}
            loading={isSyncing}
          >
            Sync from Scalekit
          </Button>
          <Button
            type="primary"
            icon={<PlusOutlined />}
            className="primary_button_style"
            onClick={() => {
              setEditingRole(null);
              setRoleForm({ name: "", description: "" });
              setIsRoleModalOpen(true);
            }}
          >
            Add Role
          </Button>
        </Space>
      </div>
      <Table
        dataSource={roles}
        columns={roleColumns}
        pagination={false}
        showHeader={false}
        rowClassName={() => "role-table-row"}
        loading={isLoading}
      />
      <Modal
        title={editingRole ? "Edit Role" : "Add Role"}
        open={isRoleModalOpen}
        onCancel={() => {
          setIsRoleModalOpen(false);
          setEditingRole(null);
          setRoleForm({ name: "", description: "" });
        }}
        footer={null}
        maskClosable={false}
      >
        <div>
          <div className="invite_field_wrap">
            <Typography className="fieldName categoryLabel">Role</Typography>
            <Input
              className="invite_inputfield"
              name="name"
              value={roleForm.name}
              onChange={(e) => handleRoleChange(e.target.value, "name")}
            ></Input>
          </div>

          <div className="invite_field_wrap">
            <Typography className="fieldName categoryLabel">
              Description
            </Typography>
            <Input
              className="invite_inputfield"
              name="description"
              value={roleForm.description}
              onChange={(e) => handleRoleChange(e.target.value, "description")}
            ></Input>
          </div>

          <div className="invite_field_wrap btn_wrap">
            <Button onClick={() => setIsRoleModalOpen(false)}>Cancel</Button>

            <Button
              onClick={editingRole ? updateRole : createRole}
              type="primary"
              style={{ marginLeft: "10px" }}
              className="primary_button_style"
              disabled={!roleForm.description || !roleForm.name}
              loading={isLoading}
            >
              {editingRole ? "Update" : "Add"}
            </Button>
          </div>
        </div>
      </Modal>

      <Modal
        title="Delete User"
        open={isDeleteModalOpen}
        onOk={() => {
          handleDeleteUser(isDeleteModalOpen);
          setIsDeleteModalOpen("");
        }}
        onCancel={() => setIsDeleteModalOpen("")}
        okText="Remove"
        okButtonProps={{ danger: true }}
        maskClosable={false}
        centered
      >
        <p>Are you sure you want to delete this user?</p>
      </Modal>
    </div>
  );
};

export default Roles;
