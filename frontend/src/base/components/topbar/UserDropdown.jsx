import { memo, useEffect } from "react";
import { Avatar, Dropdown, Typography } from "antd";
import PropTypes from "prop-types";
import { useNavigate } from "react-router-dom";

import { useSessionStore } from "../../../store/session-store";
const UserDropdown = memo(
  ({ orgDetails, fetchOrgDetails, onLogout, onSwitchOrg, isCloud }) => {
    const navigate = useNavigate();
    const { sessionDetails } = useSessionStore();
    const email = sessionDetails?.email || "";
    const avatarText = email ? email.slice(0, 2).toUpperCase() : "G";
    const defaultItems = [
      {
        key: "projects",
        label: <Typography>Projects</Typography>,
      },
      {
        key: "settings",
        label: <Typography>Settings</Typography>,
      },
    ];

    // Org switching items, displayed only if `orgDetails` is an array
    const orgItems =
      Array.isArray(orgDetails) &&
      orgDetails?.map((el, index) => ({
        key: `org-${index}`,
        label: (
          <Typography onClick={() => onSwitchOrg(el?.organization_id)}>
            {el?.display_name}
          </Typography>
        ),
      }));

    const handleMenuClick = ({ key }) => {
      switch (key) {
        case "settings":
          navigate("/project/setting/profile");
          break;
        case "projects":
          navigate("/project/list");
          break;
        case "logout":
          onLogout();
          break;
        default:
          break;
      }
    };

    // Always include logout; add Switch Org only in cloud mode
    const menuItems = [
      ...defaultItems,
      {
        key: "logout",
        label: <Typography>Logout</Typography>,
      },
      ...(isCloud
        ? [
            {
              key: "4",
              label: "Switch Org",
              children: orgItems,
            },
          ]
        : []),
    ];

    // Fetch org details only in cloud mode
    useEffect(() => {
      if (isCloud) {
        fetchOrgDetails();
      }
    }, [fetchOrgDetails, isCloud]);

    return (
      <Dropdown menu={{ items: menuItems, onClick: handleMenuClick }}>
        <Avatar size={30} className="avatar-style">
          {avatarText}
        </Avatar>
      </Dropdown>
    );
  }
);

UserDropdown.displayName = "UserDropdown";

UserDropdown.propTypes = {
  orgDetails: PropTypes.array,
  fetchOrgDetails: PropTypes.func,
  onLogout: PropTypes.func,
  onSwitchOrg: PropTypes.func,
  isCloud: PropTypes.bool,
};

export { UserDropdown };
