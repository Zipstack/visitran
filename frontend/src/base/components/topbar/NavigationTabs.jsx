import { memo } from "react";
import { Typography, Dropdown, Space } from "antd";
import {
  AppstoreOutlined,
  CloudServerOutlined,
  DatabaseOutlined,
  CaretDownOutlined,
  ApiOutlined,
} from "@ant-design/icons";
import PropTypes from "prop-types";
import { useNavigate } from "react-router-dom";

const NavigationTabs = memo(({ activeTab }) => {
  const navigate = useNavigate();
  const deployMenuItems = [
    {
      key: "jobs",
      label: (
        <Typography onClick={() => navigate("/project/job/list")}>
          Jobs
        </Typography>
      ),
    },
    {
      key: "history",
      label: (
        <Typography onClick={() => navigate("/project/job/history")}>
          Run History
        </Typography>
      ),
    },
  ];

  return (
    <div className="flex-space-between">
      <Space size={5}>
        {/* Dashboard */}
        <Typography
          className={
            activeTab === "project"
              ? "menu_items_active menu_items"
              : "menu_items"
          }
          onClick={() => navigate("/project/list")}
        >
          <AppstoreOutlined className="clr-white" />
          <Typography className="menu_label cursor-pointer">
            Dashboard
          </Typography>
        </Typography>

        {/* Deploy menu */}
        <Dropdown
          menu={{ items: deployMenuItems }}
          className={
            activeTab === "job" ? "menu_items_active menu_items" : "menu_items"
          }
        >
          <Typography className="menu_items">
            <CloudServerOutlined className="clr-white" />
            <Typography className="menu_label">Deploy</Typography>
            <CaretDownOutlined className="clr-white m5" />
          </Typography>
        </Dropdown>

        {/* Environments */}
        <Typography
          onClick={() => navigate("/project/env/list")}
          className={
            activeTab === "env" ? "menu_items_active menu_items" : "menu_items"
          }
        >
          <DatabaseOutlined className="clr-white" />
          <Typography className="menu_label">Environments</Typography>
        </Typography>

        {/* Connections */}
        <Typography
          onClick={() => navigate("/project/connection/list")}
          className={
            activeTab === "connection"
              ? "menu_items_active menu_items"
              : "menu_items"
          }
        >
          <ApiOutlined className="clr-white" />
          <Typography className="menu_label">Connections</Typography>
        </Typography>
      </Space>
    </div>
  );
});

NavigationTabs.displayName = "NavigationTabs";

NavigationTabs.propTypes = {
  activeTab: PropTypes.string,
};

export { NavigationTabs };
