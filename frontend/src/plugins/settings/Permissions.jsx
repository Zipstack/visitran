import { useState, useEffect } from "react";
import Cookies from "js-cookie";
import {
  Table,
  Typography,
  Space,
  Button,
  Modal,
  Select,
  Checkbox,
} from "antd";
import {
  PlusOutlined,
  DeleteOutlined,
  EditOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined,
} from "@ant-design/icons";
import PermissionRestricted from "./PermissionRestricted";

import { orgStore } from "../../store/org-store";
import { useAxiosPrivate } from "../../service/axios-service";
import { useNotificationService } from "../../service/notification-service";

const Permissions = () => {
  const axios = useAxiosPrivate();
  const csrfToken = Cookies.get("csrftoken");
  const { selectedOrgId } = orgStore();

  const [isRestricted, setIsRestricted] = useState(false);

  const [permissions, setPermissions] = useState([]);
  const [roles, setRoles] = useState([]);
  const [resources, setResources] = useState([]);
  const [isDeleteModalOpen, setIsDeleteModalOpen] = useState("");
  const [isPermissionModalOpen, setIsPermissionModalOpen] = useState(false);
  const [permissionForm, setPermissionForm] = useState({
    role_id: "",
    resource_id: "",
    can_read: "",
    can_write: "",
    can_delete: "",
  });
  const [editingPermission, setEditingPermission] = useState(null);
  const { notify } = useNotificationService();
  const [isLoading, setIsLoading] = useState(false);

  const onChange = (checkedValues) => {
    setPermissionForm((prevForm) => ({
      ...prevForm,
      can_read: checkedValues.includes("can_read"),
      can_write: checkedValues.includes("can_write"),
      can_delete: checkedValues.includes("can_delete"),
    }));
  };

  const plainOptions = [
    { label: "Delete", value: "can_delete" },
    { label: "Read", value: "can_read" },
    { label: "Write", value: "can_write" },
  ];

  const renderPermissionIcon = (value) => {
    if (value === true) {
      return <CheckCircleOutlined style={{ color: "green" }} />;
    } else {
      return <CloseCircleOutlined style={{ color: "red" }} />;
    }
  };

  const resColumns = [
    {
      title: "Role",
      dataIndex: "role",
      key: "role",
      filters: [
        ...roles.map((el) => {
          return { text: el.name, value: el.name };
        }),
      ],
      onFilter: (value, record) => record.role === value,
    },
    {
      title: "Resource",
      dataIndex: "resource",
      key: "resource",
      filters: [
        ...resources.map((el) => {
          return {
            text: el.resource_display_name,
            value: el.resource_display_name,
          };
        }),
      ],
      onFilter: (value, record) => record.resource === value,
    },
    {
      title: "Read",
      dataIndex: "can_read",
      key: "can_read",
      render: (_, record) => renderPermissionIcon(record.can_read),
    },
    {
      title: "Write",
      dataIndex: "can_write",
      key: "can_write",
      render: (_, record) => renderPermissionIcon(record.can_write),
    },
    {
      title: "Delete",
      dataIndex: "can_delete",
      key: "can_delete",
      render: (_, record) => renderPermissionIcon(record.can_delete),
    },
    {
      title: "Action",
      key: "action",
      render: (_, record) => (
        <Space>
          <Button
            icon={<EditOutlined />}
            onClick={() => {
              setEditingPermission(record);
              const matchedRole = roles.find(
                (r) => r.name === record.role
              );
              const matchedResource = resources.find(
                (r) => r.resource_name === record.resource
              );
              setPermissionForm({
                role_id: matchedRole?.id || "",
                resource_id: matchedResource?.id || "",
                can_read: record.can_read,
                can_write: record.can_write,
                can_delete: record.can_delete,
              });
              setIsPermissionModalOpen(true);
            }}
          />
          <Button
            icon={<DeleteOutlined />}
            onClick={() => setIsDeleteModalOpen(record.id)}
          />
        </Space>
      ),
    },
  ];

  const handleDeletePermission = (id) => {
    setIsLoading(true);
    axios({
      method: "DELETE",
      url: `/api/v1/visitran/${selectedOrgId}/uac/permissions/${id}`,
      headers: { "X-CSRFToken": csrfToken },
    })
      .then((res) => {
        getAllPermissions();
      })
      .catch((error) => {
        console.error(error);
        notify({ error });
      })
      .finally(() => {
        setIsLoading(false);
      });
  };

  const updatePermission = () => {
    setIsLoading(true);
    axios({
      method: "PUT",
      url: `/api/v1/visitran/${selectedOrgId}/uac/permissions/${editingPermission.id}`,
      headers: { "X-CSRFToken": csrfToken },
      data: permissionForm,
    })
      .then(() => {
        notify({
          type: "success",
          message: "Success",
          description: "Permission updated successfully",
        });
        getAllPermissions();
      })
      .catch((error) => {
        console.error(error);
        notify({ error });
      })
      .finally(() => {
        setIsLoading(false);
        setIsPermissionModalOpen(false);
        setEditingPermission(null);
        setPermissionForm({
          role_id: "",
          resource_id: "",
          can_read: "",
          can_write: "",
          can_delete: "",
        });
      });
  };

  const createPermission = () => {
    setIsLoading(true);
    axios({
      method: "POST",
      url: `/api/v1/visitran/${selectedOrgId}/uac/permissions`,
      headers: { "X-CSRFToken": csrfToken },
      data: permissionForm,
    })
      .then(() => {
        notify({
          type: "success",
          message: "Success",
          description: "Permission created Successfully",
        });
        getAllPermissions();
      })
      .catch((error) => {
        console.error(error);
        notify({ error });
      })
      .finally(() => {
        setIsLoading(false);
        setIsPermissionModalOpen(false);
        setPermissionForm({
          role_id: "",
          resource_id: "",
          can_read: "",
          can_write: "",
          can_delete: "",
        });
      });
  };

  const getAllPermissions = () => {
    axios({
      method: "GET",
      url: `/api/v1/visitran/${selectedOrgId}/uac/permissions/list`,
    })
      .then((res) => {
        const data = res.data;
        setPermissions(data);
      })
      .catch((error) => {
        if (error?.response?.status === 403) {
          setIsRestricted(true);
          return;
        }
        notify({ error });
      });
  };

  const getAllResources = () => {
    axios({
      method: "GET",
      url: `/api/v1/visitran/${selectedOrgId}/uac/resources`,
    })
      .then((res) => {
        const data = res.data.data;
        setResources(data);
      })
      .catch((error) => {
        if (error?.response?.status === 403) {
          setIsRestricted(true);
          return;
        }
        console.error(error);
        notify({ error });
      });
  };

  const getAllRoles = () => {
    axios({
      method: "GET",
      url: `/api/v1/visitran/${selectedOrgId}/uac/roles`,
    })
      .then((res) => {
        const data = res.data.data;
        setRoles(data);
      })
      .catch((error) => {
        if (error?.response?.status === 403) {
          setIsRestricted(true);
          return;
        }
        console.error(error);
        notify({ error });
      });
  };

  useEffect(() => {
    getAllPermissions();
    getAllRoles();
    getAllResources();
  }, []);

  const handlePermissionChange = (value, name) => {
    setPermissionForm({ ...permissionForm, [name]: value });
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
        <Typography.Title level={3}>Permissions</Typography.Title>
        <Space>
          <Button
            type="primary"
            icon={<PlusOutlined />}
            className="primary_button_style"
            onClick={() => {
              setEditingPermission(null);
              setPermissionForm({
                role_id: "",
                resource_id: "",
                can_read: "",
                can_write: "",
                can_delete: "",
              });
              setIsPermissionModalOpen(true);
            }}
          >
            Add Permission
          </Button>
        </Space>
      </div>
      <Table
        dataSource={permissions}
        columns={resColumns}
        rowKey="id"
        pagination={{ pageSize: 10 }}
      />

      <Modal
        title={editingPermission ? "Edit Permission" : "Add Permission"}
        open={isPermissionModalOpen}
        onCancel={() => {
          setIsPermissionModalOpen(false);
          setEditingPermission(null);
          setPermissionForm({
            role_id: "",
            resource_id: "",
            can_read: "",
            can_write: "",
            can_delete: "",
          });
        }}
        footer={null}
        maskClosable={false}
      >
        <div>
          <div className="invite_field_wrap">
            <Typography className="fieldName categoryLabel">
              Resource Name
            </Typography>
            <Select
              className="invite_inputfield"
              name="resource_id"
              style={{ width: "100%" }}
              value={permissionForm.resource_id}
              onChange={(value) => handlePermissionChange(value, "resource_id")}
              options={resources.map((el) => {
                return { value: el.id, label: el.resource_name };
              })}
            ></Select>
          </div>

          <div className="invite_field_wrap">
            <Typography className="fieldName categoryLabel">Role</Typography>
            <Select
              className="invite_inputfield"
              name="role_id"
              style={{ width: "100%" }}
              value={permissionForm.role_id}
              onChange={(value) => handlePermissionChange(value, "role_id")}
              options={roles.map((el) => {
                return { value: el.id, label: el.name };
              })}
            ></Select>
          </div>

          <div className="invite_field_wrap">
            <Typography className="fieldName categoryLabel">
              Permissions
            </Typography>
            <Checkbox.Group
              options={plainOptions}
              value={Object.keys(permissionForm).filter(
                (key) => permissionForm[key] === true
              )}
              onChange={onChange}
            />
          </div>

          <div className="invite_field_wrap btn_wrap">
            <Button onClick={() => setIsPermissionModalOpen(false)}>
              Cancel
            </Button>

            <Button
              onClick={
                editingPermission ? updatePermission : createPermission
              }
              type="primary"
              style={{ marginLeft: "10px" }}
              className="primary_button_style"
              disabled={
                !permissionForm.resource_id || !permissionForm.role_id
              }
              loading={isLoading}
            >
              {editingPermission ? "Update" : "Add"}
            </Button>
          </div>
        </div>
      </Modal>

      <Modal
        title="Delete User"
        open={isDeleteModalOpen}
        onOk={() => {
          handleDeletePermission(isDeleteModalOpen);
          setIsDeleteModalOpen("");
        }}
        onCancel={() => setIsDeleteModalOpen("")}
        okText="Remove"
        okButtonProps={{ danger: true }}
        maskClosable={false}
        centered
      >
        <p>Are you sure you want to delete this permission?</p>
      </Modal>
    </div>
  );
};

export default Permissions;
