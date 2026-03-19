import { useCallback, useEffect, useRef, useState } from "react";
import { Outlet, useNavigate, useLocation } from "react-router-dom";
import { Space, Typography, Button, Dropdown, Tooltip } from "antd";
import "./ProjectDropdown.css";
import { DownOutlined } from "@ant-design/icons";

import { useUserStore } from "../../../store/user-store";
import { DRAWER_TYPES, THEME } from "../../../common/constants";
import { orgStore } from "../../../store/org-store";
import { handleUserLogout } from "../../../common/helpers";
import { useAxiosPrivate } from "../../../service/axios-service";
import { VisitranAIDarkIcon, WhiteLogo } from "../../../base/icons";
import "./Topbar.css";
import { ThemeSwitcher } from "./ThemeSwitcher.jsx";
import { UserDropdown } from "./UserDropdown.jsx";
import { NavigationTabs } from "./NavigationTabs.jsx";
import { useProjectStore } from "../../../store/project-store";
import { useNotificationService } from "../../../service/notification-service.js";
import { useNoCodeModelDrawerStore } from "../../../store/no-code-model-drawer-store.js";
import { useSessionStore } from "../../../store/session-store.js";

function Topbar() {
  const navigate = useNavigate();
  const location = useLocation();
  const axios = useAxiosPrivate();
  // Access all organization store values at component top level
  const { selectedOrgId } = orgStore();
  const userDetails = useUserStore((state) => state?.userDetails);
  const updateUserDetails = useUserStore((state) => state?.updateUserDetails);
  const { projectId, projectName, setProjectId, setProjectName } =
    useProjectStore();
  const [projects, setProjects] = useState([]);
  const [loading, setLoading] = useState(false);
  const prevPathRef = useRef(null);

  const [orgDetails, setOrgDetails] = useState([]);
  const [currentTheme, setCurrentTheme] = useState();
  const [activeTab, setActiveTab] = useState("");
  const { notify } = useNotificationService();
  const { handleRightDrawer } = useNoCodeModelDrawerStore();

  const isCloud = useSessionStore((state) => state.sessionDetails?.is_cloud);

  const fetchOrgDetails = useCallback(() => {
    axios({ method: "GET", url: "/api/v1/organization" })
      .then((res) => {
        setOrgDetails(res?.data?.organizations);
      })
      .catch((error) => {
        console.error(error);
        notify({ error });
      });
  }, []);

  // Switch organization
  const handleSwitchOrg = useCallback((id) => {
    window.location.href = `/api/v1/organization/${id}/set`;
  }, []);

  const handleLogout = useCallback(() => {
    handleUserLogout();
    notify({
      type: "info",
      description: "You have been successfully logged out.",
    });
  }, []);

  const updateTheme = useCallback(
    (theme = THEME.LIGHT) => {
      setCurrentTheme(theme);
      updateUserDetails({ currentTheme: theme });
    },
    [updateUserDetails]
  );

  const changeTheme = useCallback(
    (checked) => {
      if (checked) {
        document.body.classList.add(THEME.DARK);
      } else {
        document.body.classList.remove(THEME.DARK);
      }
      updateTheme(checked ? THEME.DARK : THEME.LIGHT);
    },
    [updateTheme]
  );

  // Initialize theme
  useEffect(() => {
    if (userDetails?.currentTheme) {
      if (userDetails?.currentTheme === THEME.DARK) {
        changeTheme(true);
      }
    } else if (window.matchMedia("(prefers-color-scheme: dark)").matches) {
      changeTheme(true);
    }
  }, [userDetails?.currentTheme, changeTheme]);

  // Fetch projects for dropdown when entering IDE pages (or re-entering from non-IDE)
  useEffect(() => {
    const isIde = location?.pathname?.startsWith("/ide");
    const wasIde = prevPathRef.current?.startsWith("/ide");
    prevPathRef.current = location?.pathname;

    if (isIde && (projects.length === 0 || !wasIde)) {
      const fetchProjects = async () => {
        setLoading(true);
        try {
          const requestOptions = {
            method: "GET",
            url: `/api/v1/visitran/${selectedOrgId || "default_org"}/projects`,
          };
          const res = await axios(requestOptions);
          setProjects(res.data);
        } catch (error) {
          notify({ error });
        } finally {
          setLoading(false);
        }
      };
      fetchProjects();
    }
  }, [location?.pathname, selectedOrgId]);

  // Set the active navigation tab based on the current path
  useEffect(() => {
    const { pathname } = location;
    switch (pathname) {
      case "/project/connection/list":
        setActiveTab("connection");
        break;
      case "/project/env/list":
        setActiveTab("env");
        break;
      case "/project/list":
        setActiveTab("project");
        break;
      case "/project/job/list":
      case "/project/job/history":
        setActiveTab("job");
        break;
      default:
        setActiveTab("");
        break;
    }
  }, [location]);

  return (
    <div className="height-100vh flex-direction-column" style={{ flex: 1 }}>
      <div className="topbar_wrap align-items-center">
        <div className="top_wrap">
          <div className="nav-tab-wrap">
            <div>
              <WhiteLogo
                style={{ height: "40px", width: "auto" }}
                onClick={() => navigate("/project/list")}
              />
            </div>
            <NavigationTabs
              activeTab={activeTab}
              currentPath={location?.pathname}
            />
          </div>
          <Space size={15}>
            {location?.pathname?.startsWith("/ide") && (
              <>
                <Dropdown
                  menu={{
                    items: Array.isArray(projects?.page_items)
                      ? projects?.page_items?.map((project) => ({
                          key: project?.project_id || "unknown",
                          label: (
                            <div
                              className={`project-dropdown-item ${
                                project?.project_id === projectId
                                  ? "project-dropdown-item-current"
                                  : ""
                              }`}
                            >
                              {project?.project_name || "Unnamed Project"}
                              {project?.project_id === projectId &&
                                " (current)"}
                            </div>
                          ),
                          onClick: () => {
                            // Only proceed if we have valid project data
                            if (!project?.project_id) return;

                            // Only change if selecting a different project
                            if (project.project_id !== projectId) {
                              // Following the pattern from ProjectListCard.handleCardClick
                              setProjectName(project.project_name);
                              // Also set project ID to ensure state is fully updated
                              setProjectId(project.project_id);
                              // Navigate to project IDE view
                              navigate(`/ide/project/${project.project_id}`);
                            }
                          },
                        }))
                      : [],
                  }}
                  disabled={loading}
                  trigger={["click"]}
                  placement="bottomLeft"
                  arrow={{ pointAtCenter: true }}
                  dropdownRender={(menu) => (
                    <div>
                      {loading ? (
                        <div className="dropdown-message">
                          Loading projects...
                        </div>
                      ) : projects.length === 0 ? (
                        <div className="dropdown-message">
                          No projects found
                        </div>
                      ) : (
                        menu
                      )}
                    </div>
                  )}
                >
                  <Button type="primary" className="project-dropdown-button">
                    <div className="project-name-container">
                      <Tooltip title={projectName || "No project selected"}>
                        <Typography className="project-name-text">
                          {projectName || "Select Project"}
                        </Typography>
                      </Tooltip>
                    </div>
                    <DownOutlined className="dropdown-arrow-icon" />
                  </Button>
                </Dropdown>
                <Button
                  icon={
                    <VisitranAIDarkIcon
                      style={{ height: "20px", width: "auto" }}
                    />
                  }
                  onClick={() => handleRightDrawer(DRAWER_TYPES.CHAT_AI)}
                  className="visitran-ai-btn"
                >
                  Visitran AI
                </Button>
              </>
            )}
            <ThemeSwitcher
              currentTheme={currentTheme}
              onThemeChange={changeTheme}
            />
            <UserDropdown
              orgDetails={orgDetails}
              fetchOrgDetails={fetchOrgDetails}
              onLogout={handleLogout}
              onSwitchOrg={handleSwitchOrg}
              navigate={navigate}
              isCloud={isCloud}
            />
          </Space>
        </div>
      </div>
      <div className="flex-1 overflow-hidden">
        <Outlet />
      </div>
    </div>
  );
}

export { Topbar };
