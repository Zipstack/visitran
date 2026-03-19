import { Outlet } from "react-router-dom";

import MenuTree from "./menutree/MenuTree";
import "./Settings.css";
const Settings = () => {
  return (
    <div className="settings_layout">
      <div className="menu-tree-wrap">
        <MenuTree />
      </div>
      <div className="content_wrap">
        <Outlet />
      </div>
    </div>
  );
};

export default Settings;
