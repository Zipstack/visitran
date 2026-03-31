import { useState, useEffect } from "react";
import Cookies from "js-cookie";
import { Table, Typography, Space, Button, Modal, Input, Select } from "antd";
import { PlusOutlined, DeleteOutlined, EditOutlined } from "@ant-design/icons";
import PermissionRestricted from "./PermissionRestricted";

import { orgStore } from "../../store/org-store";
import { useAxiosPrivate } from "../../service/axios-service";
import { useNotificationService } from "../../service/notification-service";

const Resources = () => {
  const { Text } = Typography;
  const axios = useAxiosPrivate();
  const csrfToken = Cookies.get("csrftoken");

  const [isRestricted, setIsRestricted] = useState(false);
  const [resources, setResources] = useState([]);
  const [resourcesType, setResourcesType] = useState([]);
  const [allResourcesNames, setAllResourcesNames] = useState([]);
  const { selectedOrgId } = orgStore();
  const [isDeleteModalOpen, setIsDeleteModalOpen] = useState("");
  const [resForm, setResForm] = useState({
    resource_name: "",
    description: "",
    content_type: "",
  });
  const [isResourceModalOpen, setIsResourceModalOpen] = useState(false);
  const [editingResource, setEditingResource] = useState(null);
  const { notify } = useNotificationService();

  const [isLoading, setIsLoading] = useState(false);

  const resColumns = [
    {
      dataIndex: "resource_display_name",
      key: "resource_display_name",
      render: (_, record) => (
        <div>
          <Text strong>{record.resource_display_name}</Text>
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
              setEditingResource(record);
              setResForm({
                resource_name: record.resource_name,
                description: record.description,
                content_type: record.content_type,
              });
              setIsResourceModalOpen(true);
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
      url: `/api/v1/visitran/${selectedOrgId}/uac/resource/${id}/delete`,
      headers: { "X-CSRFToken": csrfToken },
    })
      .then((res) => {
        getAllResources();
      })
      .catch((error) => {
        console.error(error);
        notify({ error });
      })
      .finally(() => {
        setIsLoading(false);
      });
  };

  const updateResource = () => {
    setIsLoading(true);
    axios({
      method: "PUT",
      url: `/api/v1/visitran/${selectedOrgId}/uac/resource/${editingResource.id}/update`,
      headers: { "X-CSRFToken": csrfToken },
      data: resForm,
    })
      .then(() => {
        notify({
          type: "success",
          message: "Success",
          description: "Resource updated successfully",
        });
        getAllResources();
      })
      .catch((error) => {
        console.error(error);
        notify({ error });
      })
      .finally(() => {
        setIsLoading(false);
        setIsResourceModalOpen(false);
        setEditingResource(null);
        setResForm({ content_type: "", description: "", resource_name: "" });
      });
  };

  const createResource = () => {
    setIsLoading(true);
    axios({
      method: "POST",
      url: `/api/v1/visitran/${selectedOrgId}/uac/resource/create`,
      headers: { "X-CSRFToken": csrfToken },
      data: resForm,
    })
      .then(() => {
        notify({
          type: "success",
          message: "Success",
          description: "Resource created Successfully",
        });
        getAllResources();
      })
      .catch((error) => {
        console.error(error);
        notify({ error });
      })
      .finally(() => {
        setIsLoading(false);
        setIsResourceModalOpen(false);
        setResForm({ content_type: "", description: "", resource_name: "" });
      });
  };

  const getAllResources = () => {
    setIsLoading(true);
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
        notify({ error });
      })
      .finally(() => {
        setIsLoading(false);
      });
  };

  const getResourcesType = () => {
    axios({
      method: "GET",
      url: `/api/v1/visitran/${selectedOrgId}/uac/resource/model_content_type`,
    })
      .then((res) => {
        const data = res.data.data;
        setResourcesType(
          data.map((el) => {
            return { value: el.id, label: el.model };
          })
        );
      })
      .catch((error) => {
        console.error(error);
        notify({ error });
      });
  };

  const getAllResourcesNames = () => {
    axios({
      method: "GET",
      url: `/api/v1/visitran/${selectedOrgId}/uac/resource/visitran_resources`,
    })
      .then((res) => {
        const data = res.data;
        setAllResourcesNames(
          data.map((el) => {
            return { value: el.resource_name, label: el.resource_display_name };
          })
        );
      })
      .catch((error) => {
        console.error(error);
        notify({ error });
      });
  };

  useEffect(() => {
    getAllResources();
  }, []);

  useEffect(() => {
    if (isResourceModalOpen) {
      getResourcesType();
      getAllResourcesNames();
    }
  }, [isResourceModalOpen]);

  const handleResourceChange = (value, name) => {
    setResForm({ ...resForm, [name]: value });
  };

  if (isRestricted) return <PermissionRestricted />;

  return (
    <div className="roles_wrapper flex-direction-column">
      <div
        style={{
          marginBottom: 16,
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
        }}
      >
        <Typography.Title level={3}>Resources</Typography.Title>
        <Space>
          <Button
            type="primary"
            icon={<PlusOutlined />}
            className="primary_button_style"
            onClick={() => {
              setEditingResource(null);
              setResForm({
                content_type: "",
                description: "",
                resource_name: "",
              });
              setIsResourceModalOpen(true);
            }}
          >
            Add Resource
          </Button>
        </Space>
      </div>
      <Table
        dataSource={resources}
        columns={resColumns}
        pagination={false}
        showHeader={false}
        rowKey="id"
        loading={isLoading}
      />
      <Modal
        title={editingResource ? "Edit Resource" : "Add Resource"}
        open={isResourceModalOpen}
        onCancel={() => {
          setIsResourceModalOpen(false);
          setEditingResource(null);
          setResForm({
            content_type: "",
            description: "",
            resource_name: "",
          });
        }}
        footer={null}
        maskClosable={false}
      >
        <div>
          <div className="invite_field_wrap">
            <div className="invite_field_wrap">
              <Typography className="fieldName categoryLabel">
                Resource Name
              </Typography>
              <Select
                className="invite_inputfield"
                name="resource_name"
                value={resForm.resource_name}
                options={allResourcesNames}
                onChange={(value) =>
                  handleResourceChange(value, "resource_name")
                }
              ></Select>
            </div>
            <Typography className="fieldName categoryLabel">
              Resource Type
            </Typography>
            <Select
              className="invite_inputfield"
              name="content_type"
              style={{ width: "100%" }}
              value={resForm.content_type}
              onChange={(value) => handleResourceChange(value, "content_type")}
              options={resourcesType}
            ></Select>
          </div>

          <div className="invite_field_wrap">
            <Typography className="fieldName categoryLabel">
              Description
            </Typography>
            <Input
              className="invite_inputfield"
              name="description"
              value={resForm.description}
              onChange={(e) =>
                handleResourceChange(e.target.value, "description")
              }
            ></Input>
          </div>

          <div className="invite_field_wrap btn_wrap">
            <Button onClick={() => setIsResourceModalOpen(false)}>
              Cancel
            </Button>

            <Button
              onClick={editingResource ? updateResource : createResource}
              type="primary"
              style={{ marginLeft: "10px" }}
              className="primary_button_style"
              disabled={!resForm.description || !resForm.resource_name}
              loading={isLoading}
            >
              {editingResource ? "Update" : "Add"}
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
        <p>Are you sure you want to delete this resource?</p>
      </Modal>
    </div>
  );
};

export default Resources;
