import { useState } from "react";
import { DownOutlined } from "@ant-design/icons";
import { Button, Dropdown, Space } from "antd";
import PropTypes from "prop-types";

import pythonLogo from "../../../base/icons/python-file.svg";
import sqlLogo from "../../../base/icons/sql.svg";
import {
  FlowsheetLightIcon,
  FlowsheetDarkIcon,
} from "../../../base/icons/index.js";
import "./button-styles.css";
import { LazyLoadComponent } from "../../../widgets/lazy_loader";
import { useProjectStore } from "../../../store/project-store";
import { useUserStore } from "../../../store/user-store";
import { DRAWER_TYPES, THEME } from "../../../common/constants.js";
import { useNoCodeModelDrawerStore } from "../../../store/no-code-model-drawer-store.js";

const NoCodeConfiguration = ({ modalData, handleModalOpen }) => {
  const { setDbConfigDetails } = useProjectStore();
  const userDetails = useUserStore((state) => state.userDetails);
  const isDarkTheme = userDetails.currentTheme === THEME.DARK;
  const [openDbConfig, setOpenDbConfig] = useState(false);
  const { handleRightDrawer } = useNoCodeModelDrawerStore();

  // No longer need hover state for Python button
  const items = [
    {
      label: "Database",
      key: "database",
    },
    {
      label: "Model Configuration",
      key: "sourceDestination",
    },
    {
      label: modalData.joins.title,
      key: "joins",
    },
  ];

  const onClick = ({ key }) => {
    if (key === "database") {
      setOpenDbConfig(true);
    } else {
      handleModalOpen(key);
    }
  };

  const onDbConfigUpdate = (updatedConfigDetails) => {
    setDbConfigDetails(updatedConfigDetails); // updating store
    setTimeout(() => {
      // delaying this for toast context holder
      setOpenDbConfig(false);
    }, 3000);
  };

  const onDbConfigClose = () => {
    setOpenDbConfig(false);
  };

  return (
    <>
      <div className="button-container">
        <Button
          type="text"
          className="icon-button sequence-button"
          onClick={() => {
            handleRightDrawer(DRAWER_TYPES.SEQUENCE);
          }}
        >
          {isDarkTheme ? (
            <FlowsheetDarkIcon
              className="sequence-icon icon-dark"
              style={{ width: "20px", height: "20px" }}
            />
          ) : (
            <FlowsheetLightIcon
              className="sequence-icon icon-light"
              style={{ width: "20px", height: "20px" }}
            />
          )}
        </Button>
        <div
          className={`button-divider ${
            isDarkTheme ? "button-divider-dark" : "button-divider-light"
          }`}
        />
        <Button
          type="text"
          className="icon-button sql-button"
          onClick={() => {
            handleRightDrawer(DRAWER_TYPES.SQL);
          }}
        >
          <img
            src={sqlLogo}
            alt="SQL"
            className={`sql-icon ${
              isDarkTheme ? "sql-icon-dark" : "sql-icon-light"
            }`}
          />
        </Button>
        <div
          className={`button-divider ${
            isDarkTheme ? "button-divider-dark" : "button-divider-light"
          }`}
        />
        <Button
          type="text"
          className="icon-button python-button"
          onClick={() => {
            handleRightDrawer(DRAWER_TYPES.PYTHON);
          }}
        >
          <img
            src={pythonLogo}
            alt="Python"
            className={`python-icon ${
              isDarkTheme ? "icon-dark" : "icon-light"
            }`}
          />
        </Button>
      </div>
      <Dropdown
        trigger={"click"}
        menu={{
          items,
          onClick,
        }}
      >
        <Button size="small">
          <Space>
            Configuration
            <DownOutlined />
          </Space>
        </Button>
      </Dropdown>
      {openDbConfig && (
        <LazyLoadComponent
          component={() => import("../../explorer/db_config/db-config.jsx")}
          componentName="DatabaseConfig"
          onClose={onDbConfigClose}
          onUpdate={onDbConfigUpdate}
        />
      )}
    </>
  );
};

NoCodeConfiguration.propTypes = {
  modalData: PropTypes.object.isRequired,
  handleModalOpen: PropTypes.func.isRequired,
};

export { NoCodeConfiguration };
