import { useState, useEffect, useCallback } from "react";
import { Modal, Form, Button, Divider } from "antd";
import PropTypes from "prop-types";
import isEqual from "lodash/isEqual";
import cloneDeep from "lodash/cloneDeep";
import { useNavigate } from "react-router-dom";

import { useAxiosPrivate } from "../../service/axios-service";
import { orgStore } from "../../store/org-store";
import { useProjectStore } from "../../store/project-store";
import { CreateConnection } from "../components/environment/CreateConnection";
import NewEnv from "../components/environment/NewEnv";
import {
  getAllConnectionsApi,
  getAllEnvironmentsApi,
  getSingleProjectDetailsApi,
  createProjectApi,
  updateProjectApi,
} from "./new-project-service";
import "./newproject.css";
import { ProjectInfoSection } from "./ProjectInfoSection";
import { ConnectionEnvSection } from "./ConnectionEnvSection";
import { useNotificationService } from "../../service/notification-service";

const DEFAULT_PREFILL_DATA = {
  project_name: "",
  description: "",
  connection: "",
  environment: "",
  repository_type: "managed",
  repository_name: "",
};

function NewProject({ open, setOpen, getAllProject, id }) {
  const axiosPrivate = useAxiosPrivate();
  const { selectedOrgId } = orgStore();
  const {
    setOpenedTabs,
    projectDetails = {},
    makeActiveTab,
    setProjectName,
  } = useProjectStore();

  const [connectionList, setConnectionList] = useState([]);
  const [envDataList, setEnvDataList] = useState([]);

  const [connection, setConnection] = useState({ id: "" });
  const [connectionDbType, setConnectionDbType] = useState("");

  const [isModalOpen, setIsModalOpen] = useState(false);
  const [isEnvModalOpen, setIsEnvModalOpen] = useState(false);

  const [initialPrefillData, setInitialPrefillData] =
    useState(DEFAULT_PREFILL_DATA);
  const [isModified, setIsModified] = useState(false);
  const { notify } = useNotificationService();

  const [form] = Form.useForm();
  const navigate = useNavigate();

  // Close main modal and reset details
  const handleModalClose = useCallback(() => {
    form.resetFields();
    setInitialPrefillData(DEFAULT_PREFILL_DATA);
    setIsModified(false);
    setOpen(false);
  }, [form, setOpen]);

  const getAllConnections = useCallback(
    async (connectionData) => {
      if (connectionData) {
        setConnectionList((prev) => {
          const exists = prev.some((c) => c.id === connectionData.id);
          if (exists) {
            return prev.map((c) =>
              c.id === connectionData.id ? connectionData : c
            );
          }
          return [...prev, connectionData];
        });
        return;
      }
      try {
        const data = await getAllConnectionsApi(axiosPrivate, selectedOrgId);
        setConnectionList(data);
      } catch (error) {
        console.error(error);
        notify({ error });
      }
    },
    [selectedOrgId]
  );

  const getAllEnvironments = useCallback(async () => {
    try {
      const data = await getAllEnvironmentsApi(axiosPrivate, selectedOrgId);
      setEnvDataList(data);
    } catch (error) {
      console.error(error);
      notify({ error });
    }
  }, [selectedOrgId]);

  const getSingleProjectDetails = useCallback(
    async (projId) => {
      try {
        const data = await getSingleProjectDetailsApi(
          axiosPrivate,
          selectedOrgId,
          projId
        );
        const {
          project_name,
          description,
          environment: envObj,
          connection: connObj,
          db_name,
        } = data;

        form.setFieldsValue({
          project_name,
          description,
          connection: connObj.id,
          environment: envObj.id,
          repository_type: "managed",
          repository_name: "",
        });

        setConnection({ id: connObj.id });
        setConnectionDbType(db_name);

        setInitialPrefillData({
          project_name,
          description,
          connection: connObj.id,
          environment: envObj.id,
          repository_type: "managed",
          repository_name: "",
        });
      } catch (error) {
        console.error(error);
        notify({ error });
      }
    },
    [form, selectedOrgId]
  );

  const updateProjectNameInTabs = useCallback(
    (newName) => {
      if (newName !== initialPrefillData.project_name) {
        const { openedTabs = [], focussedTab = {} } = cloneDeep(
          projectDetails?.[id] || {}
        );

        if (openedTabs.length) {
          const modified = openedTabs.map((tab) => {
            const parts = tab.key.split("/");
            parts[0] = newName;
            return { ...tab, key: parts.join("/") };
          });
          setOpenedTabs(modified, id);
        }

        if (Object.keys(focussedTab).length) {
          const newKey = [newName, ...focussedTab.key.split("/").slice(1)].join(
            "/"
          );
          makeActiveTab({ ...focussedTab, key: newKey }, id);
        }
      }
    },
    [
      id,
      initialPrefillData?.project_name,
      makeActiveTab,
      projectDetails,
      setOpenedTabs,
    ]
  );

  const handleCreate = async (formValues) => {
    const response = await createProjectApi(
      axiosPrivate,
      selectedOrgId,
      formValues
    );
    const projectId = response?.data?.project_id;

    notify({
      type: "success",
      message: "Success",
      description: "Project has been created successfully.",
    });

    if (projectId) {
      setProjectName(formValues.project_name);
      navigate(`/ide/project/${projectId}`);
    }
  };

  const handleUpdate = async (formValues) => {
    await updateProjectApi(axiosPrivate, selectedOrgId, id, formValues);
    updateProjectNameInTabs(formValues.project_name);
    notify({
      type: "success",
      message: "Success",
      description: "Project updated successfully.",
    });
  };

  const handleCreateOrUpdateProject = useCallback(
    async (formValues, isUpdate) => {
      try {
        if (isUpdate) {
          await handleUpdate(formValues);
        } else {
          await handleCreate(formValues);
        }
        getAllProject();
        setOpen(false);
      } catch (error) {
        console.error(error);
        notify({ error });
      }
    },
    [getAllProject, id, selectedOrgId, setOpen, updateProjectNameInTabs]
  );

  const createProject = useCallback(
    async (formValues) => {
      await handleCreateOrUpdateProject(formValues, false);
    },
    [handleCreateOrUpdateProject]
  );

  const updateProject = useCallback(
    async (formValues) => {
      await handleCreateOrUpdateProject(formValues, true);
    },
    [handleCreateOrUpdateProject]
  );

  const handleValuesChange = useCallback(
    (changedValues, allValues) => {
      const base = {
        project_name: allValues.project_name || "",
        description: allValues.description || "",
        connection: allValues.connection || "",
        environment: allValues.environment || "",
        repository_type: allValues.repository_type || "managed",
        repository_name: allValues.repository_name || "",
      };
      setIsModified(!isEqual(base, initialPrefillData));
    },
    [initialPrefillData]
  );

  const onFinish = useCallback(
    (values) => {
      if (id) {
        updateProject(values);
      } else {
        createProject(values);
      }
    },
    [createProject, id, updateProject]
  );

  useEffect(() => {
    getAllConnections();
    getAllEnvironments();
  }, [getAllConnections, getAllEnvironments]);

  useEffect(() => {
    if (id) {
      getSingleProjectDetails(id);
    }
  }, [id, getSingleProjectDetails]);

  return (
    <>
      <Modal
        open={open}
        onCancel={handleModalClose}
        centered
        footer={null}
        title={id ? "Edit Project" : "Create Project"}
        destroyOnClose
        maskClosable={false}
        className="newProjectModal"
      >
        <div className="newProjectLayout">
          <Form
            form={form}
            layout="vertical"
            initialValues={{
              repository_type: "managed",
              repository_name: "",
            }}
            onValuesChange={handleValuesChange}
            onFinish={onFinish}
            className="newProjectForm"
          >
            <ProjectInfoSection />

            <Divider className="divider-modal" />

            <ConnectionEnvSection
              connectionList={connectionList}
              envDataList={envDataList}
              setIsModalOpen={setIsModalOpen}
              setIsEnvModalOpen={setIsEnvModalOpen}
              id={id}
            />

            <Divider className="divider-modal" />

            <div className="projectFormFooter">
              <Button onClick={handleModalClose}>Cancel</Button>
              <Button
                className="primary_button_style"
                type="primary"
                htmlType="submit"
                disabled={id ? !isModified : false}
              >
                {id ? "Update" : "Create"} Project
              </Button>
            </div>
          </Form>
        </div>
      </Modal>

      <Modal
        title="New Connection"
        open={isModalOpen}
        onCancel={() => setIsModalOpen(false)}
        footer={null}
        width={1000}
        destroyOnClose
        className="newConnectionModal"
        centered
        maskClosable={false}
      >
        <CreateConnection
          setIsModalOpen={setIsModalOpen}
          getAllConnection={getAllConnections}
          connectionId={connection.id}
          setConnectionId={(connObj) => {
            form.setFieldValue("connection", connObj?.id);
            setConnection(connObj);
          }}
          setConnectionDbType={setConnectionDbType}
        />
      </Modal>

      <Modal
        title="New Environment"
        open={isEnvModalOpen}
        onCancel={() => setIsEnvModalOpen(false)}
        footer={null}
        width={600}
        destroyOnClose
        className="newEnvModal"
        centered
        maskClosable={false}
      >
        <NewEnv
          setIsEnvModalOpen={setIsEnvModalOpen}
          getAllEnvironments={getAllEnvironments}
          connectionId={connection.id}
          connectionDb={connectionDbType}
          setEnvironment={(envObj) => {
            form.setFieldValue("environment", envObj?.id);
          }}
        />
      </Modal>
    </>
  );
}

NewProject.propTypes = {
  open: PropTypes.bool.isRequired,
  setOpen: PropTypes.func.isRequired,
  getAllProject: PropTypes.func.isRequired,
  id: PropTypes.string,
};

NewProject.defaultProps = {
  id: null,
};

export { NewProject };
