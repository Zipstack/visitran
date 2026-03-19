import { useMemo, useCallback } from "react";
import {
  SettingOutlined,
  TeamOutlined,
  ReconciliationOutlined,
  IdcardOutlined,
  KeyOutlined,
  MailOutlined,
} from "@ant-design/icons";
import { Menu } from "antd";
import { useNavigate, useLocation } from "react-router-dom";

import {
  Roles as RolesIcon,
  UAC,
  Resources as ResourcesIcon,
  Perm,
} from "../../../icons";
import { useSessionStore } from "../../../../store/session-store";

// Detect cloud-only components — absent in OSS
let hasUserManagement = false;
let hasSubscriptions = false;
let hasRoles = false;
let hasResources = false;
let hasPermissions = false;
let hasSubscriptionAdmin = false;
let hasKeyManagement = false;
let hasSlackNotification = false;
try {
  require("../../../../plugins/settings/keyManagement");
  hasKeyManagement = true;
} catch {
  /* plugin not available */
}
try {
  require("../../../../plugins/settings/UserManagement");
  hasUserManagement = true;
} catch {
  /* plugin not available */
}
try {
  require("../../../../plugins/settings/Subscriptions");
  hasSubscriptions = true;
} catch {
  /* plugin not available */
}
try {
  require("../../../../plugins/settings/Roles");
  hasRoles = true;
} catch {
  /* plugin not available */
}
try {
  require("../../../../plugins/settings/Resources");
  hasResources = true;
} catch {
  /* plugin not available */
}
try {
  require("../../../../plugins/settings/Permissions");
  hasPermissions = true;
} catch {
  /* plugin not available */
}
try {
  require("../../../../plugins/subscription/admin/SubscriptionAdminPage");
  hasSubscriptionAdmin = true;
} catch {
  /* plugin not available */
}
try {
  require("../../../../plugins/slack-integration/SlackNotification");
  hasSlackNotification = true;
} catch {
  /* plugin not available */
}

const MenuTree = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const { sessionDetails } = useSessionStore();

  const isOrgAdmin = sessionDetails?.is_org_admin;
  const userRole = sessionDetails?.user_role;

  // Build settings children dynamically
  const settingsChildren = useMemo(
    () =>
      [
        {
          key: "/project/setting/profile",
          icon: <IdcardOutlined />,
          label: "Profile",
        },
        hasUserManagement && {
          key: "/project/setting/usermanagement",
          icon: <TeamOutlined />,
          label: "Users",
        },
        hasSubscriptions && {
          key: "/project/setting/subscriptions",
          icon: <ReconciliationOutlined />,
          label: "Billing",
        },
        hasKeyManagement && {
          key: "/project/setting/keymanagement",
          icon: <KeyOutlined />,
          label: "API Tokens",
        },
      ].filter(Boolean),
    []
  );

  // Build UAC children dynamically
  const uacChildren = useMemo(
    () =>
      [
        hasRoles && {
          key: "/project/setting/roles",
          icon: <RolesIcon />,
          label: "Roles",
        },
        hasResources && {
          key: "/project/setting/resources",
          icon: <ResourcesIcon />,
          label: "Resources",
        },
        hasPermissions && {
          key: "/project/setting/permissions",
          icon: <Perm />,
          label: "Permissions",
        },
        hasSubscriptionAdmin && {
          key: "/project/setting/subscription-admin",
          icon: <ReconciliationOutlined />,
          label: "Subscription Manage",
        },
      ].filter(Boolean),
    []
  );

  // Build notification children dynamically
  const notificationsChildren = useMemo(
    () => [
      {
        key: "email-disabled",
        icon: <MailOutlined />,
        label: "Email Notification",
        disabled: true,
      },
      ...(hasSlackNotification && isOrgAdmin
        ? [
            {
              key: "/project/setting/notification/slack",
              icon: <KeyOutlined />,
              label: "Slack Notification",
            },
          ]
        : []),
    ],
    [isOrgAdmin]
  );

  const items = useMemo(
    () =>
      [
        {
          key: "settings",
          icon: <SettingOutlined />,
          label: "Settings",
          children: settingsChildren,
        },
        userRole === "visitran_super_admin" &&
          uacChildren.length > 0 && {
            key: "user_access_control",
            icon: <UAC />,
            label: "User Access Control",
            children: uacChildren,
          },
        notificationsChildren.some((c) => !c.disabled) && {
          key: "notifications",
          icon: <SettingOutlined />,
          label: "Notification Setting",
          children: notificationsChildren,
        },
      ].filter(Boolean),
    [settingsChildren, uacChildren, notificationsChildren, userRole]
  );

  const handleClick = useCallback(
    ({ key }) => {
      if (key.startsWith("/")) navigate(key);
    },
    [navigate]
  );

  const defaultOpenKeys = useMemo(
    () => [
      "settings",
      ...(notificationsChildren.some((c) => !c.disabled)
        ? ["notifications"]
        : []),
      ...(userRole === "visitran_super_admin" && uacChildren.length > 0
        ? ["user_access_control"]
        : []),
    ],
    [notificationsChildren, uacChildren, userRole]
  );

  return (
    <Menu
      className="menutree"
      mode="inline"
      items={items}
      onClick={handleClick}
      selectedKeys={[location.pathname]}
      defaultOpenKeys={defaultOpenKeys}
      expandIcon
    />
  );
};

export default MenuTree;
