import { useState, useEffect } from "react";
import { Typography, Badge, Tag, Tooltip, Modal, Button, Space } from "antd";
import {
  DatabaseOutlined,
  SettingOutlined,
  LineChartOutlined,
} from "@ant-design/icons";
import { useNavigate } from "react-router-dom";

import { LazyLoadComponent } from "../../../widgets/lazy_loader";
import { dbConfigService } from "./db-config-service.js";
import { DB_NO_NEED_TO_TEST } from "./db-config-constants.js";
import { SpinnerLoader } from "../../../widgets/spinner_loader";
import { useProjectStore } from "../../../store/project-store.js";
import "./db-config.css";
import { useNotificationService } from "../../../service/notification-service.js";

const CONNECTION_STATUS = {
  ONLINE: "success",
  OFFLINE: "error",
  WARNING: "warning",
};
const DB_ICON_STYLE = { marginInlineEnd: "8px" };

function DbConfigPreview() {
  const dbConfService = dbConfigService();
  const {
    dbConfigDetails,
    setDbConfigDetails,
    projectId,
    setProjectId,
    makeActiveTab,
    setOpenedTabs,
  } = useProjectStore();
  const [openDbConfig, setOpenDbConfig] = useState(false);
  const [dbStatus, setDbStatus] = useState();
  const [errorMsg, setErrorMsg] = useState("");
  const [showDbVisualizerTooltip, setShowDbVisualizerTooltip] = useState(false);
  const { notify } = useNotificationService();
  const navigate = useNavigate();
  const dsName = dbConfigDetails.datasource_name || "";
  const connectionName = dbConfigDetails.connection_name || "";
  useEffect(() => {
    if (!projectId) return;
    getDbConfigDetails();
  }, [projectId]);
  function getDbConfigDetails() {
    checkDbconfig()
      .then(() => {})
      .catch(() => {});
  }

  async function checkDbconfig() {
    let dbConfigRes;
    let configuredDs;
    try {
      setDbConfigDetails({});
      setDbStatus();
      dbConfigRes = await dbConfService.getDbConfig(projectId);

      setDbConfigDetails(dbConfigRes.data); // updating store
      configuredDs = dbConfigRes.data.datasource_name;
      if (DB_NO_NEED_TO_TEST.includes(configuredDs)) {
        setDbStatus(CONNECTION_STATUS.ONLINE);
      } else if (configuredDs) {
        if (projectId) {
          const connectionRes = await dbConfService.testConnection(
            projectId,
            configuredDs
          );
          const { data } = connectionRes;
          setDbStatus(
            data.status === CONNECTION_STATUS.ONLINE
              ? CONNECTION_STATUS.ONLINE
              : CONNECTION_STATUS.OFFLINE
          );
        } else {
          setDbStatus(CONNECTION_STATUS.WARNING);
        }
      }
    } catch (error) {
      setErrorMsg(error.response.data);
      if (configuredDs) {
        setDbStatus(CONNECTION_STATUS.OFFLINE);
      } else {
        setDbStatus(CONNECTION_STATUS.WARNING);
        console.error(error);
        notify({ error });
        setProjectId("");
      }
    }
  }

  function getDbDetails() {
    if (!dbStatus) {
      return <SpinnerLoader />;
    }
    return (
      <div className="db-preview-wrap">
        <div className="flex-1 overflow-hidden">
          <Badge
            className="db-preview-wrap-name"
            dot
            color={dbStatus === "success" ? "green" : "red"}
          >
            <DatabaseOutlined style={DB_ICON_STYLE} />
            <Typography.Text
              className="db-preview-name-typography"
              ellipsis={{ tooltip: connectionName }}
            >
              {connectionName || "No Database configured"}
            </Typography.Text>
          </Badge>
          {dbStatus !== "success" && (
            <Tooltip title={errorMsg}>
              <Tag color="red" className="ml-10">
                Failed
              </Tag>
            </Tooltip>
          )}
        </div>
        <div className="db-config-icons">
          <div
            className="db-visualizer-icon"
            onMouseEnter={() => setShowDbVisualizerTooltip(true)}
            onMouseLeave={() => setShowDbVisualizerTooltip(false)}
          >
            <LineChartOutlined onClick={handleDatabaseVisualizerClick} />
            {showDbVisualizerTooltip && (
              <div className="db-visualizer-tooltip">Database Visualizer</div>
            )}
          </div>
          <SettingOutlined onClick={onOpen} />
        </div>
      </div>
    );
  }

  function onDbUpdate(updatedConfigDetails) {
    if (updatedConfigDetails.datasource_name !== dsName) {
      getDbConfigDetails();
    }
    setTimeout(() => {
      // delaying this for toast context holder
      setOpenDbConfig(false);
    }, 3000);
  }

  function onOpen() {
    setOpenDbConfig(true);
  }

  function onClose() {
    setOpenDbConfig(false);
  }

  function handleDatabaseVisualizerClick() {
    // We need to modify the editor component to recognize database tabs regardless of how they were opened
    // Let's create a patch for the editor component's addTab function

    // First, let's get the current state of the project tabs
    const projectTabs =
      useProjectStore.getState().projectDetails[projectId] || {};
    const openedTabs = projectTabs.openedTabs || [];

    // Define the database tab properties
    const dbTabType = "ROOT_DB";

    // Check if a database tab is already open by looking for the ROOT_DB type
    const existingDbTab = openedTabs.find((tab) => tab.type === dbTabType);

    if (existingDbTab) {
      // If a database tab is already open, just make it active
      makeActiveTab({ key: existingDbTab.key, type: existingDbTab.type });
    } else {
      // Create a new database tab
      // Use a special key that will be recognized by both the bottom icon and tree nodes
      // This key is important - we'll use a consistent key that both can recognize
      const dbTabKey = "database";

      // Create a database tab in the editor
      const dbTabData = {
        key: dbTabKey,
        node: {
          title: "Database", // Use the same title as the database nodes
          type: dbTabType,
        },
      };

      // We need to modify the editor's behavior to recognize database tabs
      // Let's create a custom patch for the project store

      // First, get all existing tabs
      const updatedTabs = [
        ...openedTabs,
        {
          key: dbTabData.key,
          label: dbTabData.node.title,
          type: dbTabData.node.type,
        },
      ];

      // Update the opened tabs
      setOpenedTabs(updatedTabs);

      // Make it the active tab
      makeActiveTab({ key: dbTabData.key, type: dbTabData.node.type });

      // This is done by using a consistent key and type for the database tab
    }
  }
  function handleUpdateConn() {
    navigate("/project/connection/list", {
      state: {
        cId: dbConfigDetails.connection_id,
      },
    });
  }

  function handleUpdateProj() {
    navigate("/project/list", {
      state: {
        projId: projectId,
      },
    });
  }

  return (
    <>
      <div className="dbConfigPreview">{getDbDetails()}</div>
      {openDbConfig && (
        <LazyLoadComponent
          component={() => import("./db-config.jsx")}
          componentName="DatabaseConfig"
          onUpdate={onDbUpdate}
          onClose={onClose}
          setOpenDbConfig={setOpenDbConfig}
        />
      )}
      <Modal
        title="Database Connection Failed"
        width={600}
        closable={{ "aria-label": "Custom Close Button" }}
        open={false}
        footer={null}
        centered
      >
        <Typography.Paragraph>
          The database connection for this project has failed, which prevents
          the project from functioning properly.
        </Typography.Paragraph>
        <Typography.Title level={5}>
          Host: {dbConfigDetails?.connection_details?.host || "N/A"}
        </Typography.Title>
        <Typography.Title level={5}>
          Database: {dbConfigDetails?.connection_name || "N/A"}
        </Typography.Title>
        <Typography>
          Please verify the connection settings or switch to a different
          connection from the project settings.
        </Typography>
        <Space className="flex-justify-right">
          <Button onClick={handleUpdateConn}>Update Connection</Button>
          <Button onClick={handleUpdateProj}>Edit Project Settings</Button>
        </Space>
      </Modal>
    </>
  );
}

export { DbConfigPreview };
