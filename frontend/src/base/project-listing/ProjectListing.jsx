import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  ArrowRightOutlined,
  BarsOutlined,
  DatabaseOutlined,
  CloseOutlined,
  DeleteOutlined,
  DownloadOutlined,
  DownOutlined,
  ExperimentOutlined,
  MessageOutlined,
  PlayCircleOutlined,
  PlusOutlined,
  RocketOutlined,
  SearchOutlined,
  SortAscendingOutlined,
  TableOutlined,
  ThunderboltOutlined,
  ToolOutlined,
} from "@ant-design/icons";
import { useNavigate, useLocation } from "react-router-dom";
import {
  Alert,
  Button,
  Checkbox,
  Dropdown,
  Input,
  List,
  Menu,
  Modal,
  Pagination,
  Segmented,
  Select,
  Spin,
  Tooltip,
  Typography,
} from "antd";
import Cookies from "js-cookie";

import "./ProjectListing.css";
// TableIcon removed - no longer used in empty state
import { useAxiosPrivate } from "../../service/axios-service.js";
import { orgStore } from "../../store/org-store.js";
import { useProjectStore } from "../../store/project-store.js";
import ProjectListCard from "../components/project-list/ProjectListCard";
import { NewProject } from "../new-project/NewProject.jsx";
import { checkPermission, getRelativeTime } from "../../common/helpers";
import { useNotificationService } from "../../service/notification-service.js";
import { DocsFooter } from "../components/docs-footer/DocsFooter";
import { useSessionStore } from "../../store/session-store";

let useSubscriptionDetailsStoreSafe;
try {
  useSubscriptionDetailsStoreSafe =
    require("../../plugins/store/subscription-details-store").useSubscriptionDetailsStore;
} catch {
  useSubscriptionDetailsStoreSafe = null;
}

const ProjectListing = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const { projId } = location.state || {};
  const csrfToken = Cookies.get("csrftoken");
  const [type, setType] = useState("card");
  const [projects, setProjects] = useState([]);
  const [loading, setLoading] = useState(true); // initial full-page load only
  const [fetching, setFetching] = useState(false); // search / pagination fetch
  const axios = useAxiosPrivate();
  const { selectedOrgId } = orgStore();
  const isCloud = useSessionStore((state) => state.sessionDetails?.is_cloud);
  const { setProjectId, setProjectName } = useProjectStore();
  const [openNewProjModal, setOpenNewProjModal] = useState(false);
  const [editProjectId, setEditProjectId] = useState(null);
  const [aiSetupLoading, setAiSetupLoading] = useState(false);
  const [overlayText, setOverlayText] = useState({
    title: "Setting up your AI workspace...",
    sub: "Creating sample project \u00b7 Initializing Visitran AI",
  });
  const [deletingId, setDeletingId] = useState(null);
  const [sortBy, setSortBy] = useState("modified");
  const [selectedIds, setSelectedIds] = useState(new Set());
  const [bulkDeleteModal, setBulkDeleteModal] = useState(false);
  const [bulkDelText, setBulkDelText] = useState("");
  const [searchText, setSearchText] = useState("");
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize] = useState(20);
  const [totalProjects, setTotalProjects] = useState(0);
  const searchTimerRef = useRef(null);
  const hasLoadedOnce = useRef(false);
  const { notify } = useNotificationService();

  const usage =
    typeof useSubscriptionDetailsStoreSafe === "function"
      ? useSubscriptionDetailsStoreSafe((s) => s.usage)
      : null;
  const isProjectLimitReached =
    usage?.projects && usage.projects.used >= usage.projects.allowed;

  const selectionMode = selectedIds.size > 0;

  const handleToggleSelect = useCallback((id) => {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }, []);

  const handleSelectAll = () => {
    setSelectedIds(new Set(projects.map((p) => p.project_id)));
  };
  const handleDeselectAll = () => setSelectedIds(new Set());

  const handleSearchChange = useCallback(
    (e) => {
      const val = e.target.value;
      setSearchText(val);
      clearTimeout(searchTimerRef.current);
      searchTimerRef.current = setTimeout(() => {
        setCurrentPage(1);
        getAllProject(val, 1);
      }, 400);
    },
    [searchText]
  );

  const handlePageChange = useCallback(
    (page) => {
      setCurrentPage(page);
      getAllProject(searchText, page);
    },
    [searchText]
  );

  const resumeProject = useMemo(() => {
    if (!projects.length) return null;
    return [...projects].sort(
      (a, b) => new Date(b.modified_at) - new Date(a.modified_at)
    )[0];
  }, [projects]);

  // Sorting is handled server-side; projects are already in the correct order.

  const handleResume = (project) => {
    if (project?.status && project?.is_sample) {
      return;
    }
    setProjectName(project?.project_name);
    navigate(`/ide/project/${project?.project_id}`);
  };

  const getAllProject = async (
    search = searchText,
    page = currentPage,
    { initial = false, silent = false } = {}
  ) => {
    if (initial) setLoading(true);
    if (!silent) setFetching(true);
    try {
      const params = { page, page_size: pageSize, sort_by: sortBy };
      if (search) params.search = search;
      const requestOptions = {
        method: "GET",
        url: `/api/v1/visitran/${selectedOrgId || "default_org"}/projects`,
        params,
      };
      const res = await axios(requestOptions);
      const data = res.data;
      // Support both paginated envelope and plain array (backward compat)
      if (Array.isArray(data)) {
        setProjects(data);
        setTotalProjects(data.length);
        if (data.length > 0) hasLoadedOnce.current = true;
      } else {
        setProjects(data.page_items || []);
        setTotalProjects(data.total || 0);
        setCurrentPage(data.page || 1);
        if (data.total > 0) hasLoadedOnce.current = true;
      }
    } catch (error) {
      console.error(error);
      notify({ error });
    } finally {
      setLoading(false);
      setFetching(false);
    }
  };

  useEffect(() => {
    if (!isCloud || selectedOrgId) {
      getAllProject("", 1, { initial: true });
    }
  }, [selectedOrgId]);

  // Re-fetch when sort order changes
  useEffect(() => {
    if (!loading) {
      getAllProject(searchText, 1);
      setCurrentPage(1);
    }
  }, [sortBy]);

  useEffect(() => {
    setProjectId("");
  }, []);

  const handleLoadProject = (template) => {
    const [proj, type] = template.split("_");
    const projName = proj === "dvd" ? "DVD Rental Store" : "Jaffle Shop";
    const projType = type === "final" ? "Finalized" : "Starter";
    const projectLabel = `${projName} ${projType}`;
    setOverlayText({
      title: `Loading ${projectLabel}...`,
      sub: "Creating sample project \u00b7 Setting up environment",
    });
    setAiSetupLoading(true);
    const requestOptions = {
      method: "POST",
      url: `/api/v1/visitran/${
        selectedOrgId || "default_org"
      }/project/sample-create`,
      headers: {
        "X-CSRFToken": csrfToken,
      },
      data: {
        template: template,
      },
    };
    axios(requestOptions)
      .then((res) => {
        const data = res?.data?.data || {};
        setProjectName(data?.project_name);
        navigate(`/ide/project/${data?.project_id}`);
      })
      .catch((error) => {
        console.error(error);
        notify({ error });
        setAiSetupLoading(false);
      });
  };

  const handleCreateOrUpdateProject = (id = null) => {
    setOpenNewProjModal(true);
    setEditProjectId(id);
  };

  const handleAIGuidedSetup = () => {
    setOverlayText({
      title: "Setting up your AI workspace...",
      sub: "Creating sample project \u00b7 Initializing Visitran AI",
    });
    setAiSetupLoading(true);
    const requestOptions = {
      method: "POST",
      url: `/api/v1/visitran/${
        selectedOrgId || "default_org"
      }/project/sample-create`,
      headers: {
        "X-CSRFToken": csrfToken,
      },
      data: {
        template: "jaffleshop_starter",
      },
    };
    axios(requestOptions)
      .then((res) => {
        const data = res?.data?.data || {};
        setProjectName(data?.project_name);
        navigate(`/ide/project/${data?.project_id}`);
      })
      .catch((error) => {
        console.error(error);
        notify({ error });
        setAiSetupLoading(false);
      });
  };

  useEffect(() => {
    if (editProjectId && !openNewProjModal) {
      setEditProjectId(null);
    }
  }, [editProjectId, openNewProjModal]);

  useEffect(() => {
    if (projId) {
      setOpenNewProjModal(true);
      setEditProjectId(projId);
    }
  }, [projId]);

  if (loading) {
    return (
      <Spin
        style={{
          display: "flex",
          justifyContent: "center",
          height: "100vh",
          alignItems: "center",
        }}
      />
    );
  }

  const handleBulkDelete = async () => {
    setBulkDeleteModal(false);
    setBulkDelText("");
    const ids = [...selectedIds];
    for (const id of ids) {
      setDeletingId(id);
      try {
        await axios({
          method: "DELETE",
          url: `/api/v1/visitran/${
            selectedOrgId || "default_org"
          }/project/${id}/delete`,
          headers: { "X-CSRFToken": csrfToken },
        });
        // Wait for card zoom-out animation
        await new Promise((r) => setTimeout(r, 600));
      } catch (error) {
        console.error(error);
        notify({ error });
      }
    }
    setDeletingId(null);
    setSelectedIds(new Set());
    getAllProject(searchText, currentPage);
  };

  const deleteProject = async (id) => {
    setDeletingId(id);
    try {
      const requestOptions = {
        method: "DELETE",
        url: `/api/v1/visitran/${
          selectedOrgId || "default_org"
        }/project/${id}/delete`,
        headers: {
          "X-CSRFToken": csrfToken,
        },
      };
      const res = await axios(requestOptions);
      if (res.status === 200) {
        notify({
          type: "success",
          message: "Deleted Successfully",
          description: res.data.data,
        });
        // Wait for card zoom-out animation then re-fetch
        await new Promise((r) => setTimeout(r, 1000));
        setDeletingId(null);
        getAllProject(searchText, currentPage);
      } else {
        setDeletingId(null);
      }
    } catch (error) {
      console.error(error);
      notify({ error });
      setDeletingId(null);
    }
  };

  return (
    <div className="list_main_wrap">
      <div
        className="flex-direction-column height-100 overflow-hidden"
        style={{ backgroundColor: "var(--white)" }}
      >
        {isProjectLimitReached && (
          <Alert
            message={`You've reached the maximum number of projects (${usage.projects.used}/${usage.projects.allowed}) allowed in your current plan. Upgrade to create more.`}
            type="warning"
            showIcon
            style={{ margin: "12px 24px 0" }}
            action={
              <Button
                size="small"
                type="link"
                onClick={() => navigate("/project/setting/subscriptions")}
              >
                Upgrade
              </Button>
            }
          />
        )}
        {(projects.length > 0 || searchText) && (
          <div className="proj_list_topbar_wrap">
            <Typography className="myprojectHeading">My Projects</Typography>
            <div>
              {projects.length || searchText ? (
                <Tooltip
                  title={
                    isProjectLimitReached
                      ? "Project limit reached. Upgrade your plan."
                      : ""
                  }
                >
                  <Button
                    icon={<PlusOutlined />}
                    onClick={() => handleCreateOrUpdateProject(null)}
                    className="primary_button_style mr-10"
                    disabled={
                      !checkPermission("PROJECT_DASHBOARD", "can_write") ||
                      isProjectLimitReached
                    }
                  >
                    Create Project
                  </Button>
                </Tooltip>
              ) : null}
              <Dropdown
                overlay={
                  <Menu mode="vertical" className="sample-projects-menu">
                    <Menu.ItemGroup title="Jaffle Shop">
                      <Menu.Item
                        key="jaffle-shop-starter"
                        onClick={() => handleLoadProject("jaffleshop_starter")}
                      >
                        Starter
                      </Menu.Item>
                      <Menu.Item
                        key="jaffle-shop-finalized"
                        onClick={() => handleLoadProject("jaffleshop_final")}
                      >
                        Finalized
                      </Menu.Item>
                    </Menu.ItemGroup>
                    <Menu.ItemGroup title="DVD Rental Store">
                      <Menu.Item
                        key="dvd-rental-starter"
                        onClick={() => handleLoadProject("dvd_starter")}
                      >
                        Starter
                      </Menu.Item>
                      <Menu.Item
                        key="dvd-rental-finalized"
                        onClick={() => handleLoadProject("dvd_final")}
                      >
                        Finalized
                      </Menu.Item>
                    </Menu.ItemGroup>
                  </Menu>
                }
                trigger={["click"]}
                placement="bottomLeft"
              >
                <Button
                  icon={<DownloadOutlined />}
                  style={{
                    marginLeft: "10px",
                    backgroundColor: "var(--card-header-bg)",
                  }}
                >
                  Load Sample Project <DownOutlined />
                </Button>
              </Dropdown>
              <Input
                placeholder="Search projects..."
                prefix={<SearchOutlined className="proj-search-icon" />}
                value={searchText}
                onChange={handleSearchChange}
                allowClear
                className="proj-search-input"
              />
              <Select
                value={sortBy}
                onChange={setSortBy}
                style={{ width: 160, marginLeft: "10px" }}
                suffixIcon={<SortAscendingOutlined />}
                options={[
                  { value: "modified", label: "Last Updated" },
                  { value: "created", label: "Create Date" },
                  { value: "name", label: "Name A-Z" },
                ]}
              />
              <Segmented
                options={[
                  {
                    value: "card",
                    icon: <TableOutlined className="proj-view-toggle-icon" />,
                  },
                  {
                    value: "list",
                    icon: <BarsOutlined className="proj-view-toggle-icon" />,
                  },
                ]}
                onChange={(value) => {
                  setType(value);
                }}
                style={{
                  padding: "4px",
                  backgroundColor: "var(--segment-bg)",
                  marginLeft: "10px",
                }}
              />
            </div>
          </div>
        )}

        {selectionMode && (
          <div className="selection-bar">
            <Checkbox
              checked={selectedIds.size === projects.length}
              indeterminate={
                selectedIds.size > 0 && selectedIds.size < projects.length
              }
              onChange={(e) =>
                e.target.checked ? handleSelectAll() : handleDeselectAll()
              }
            />
            <Typography.Text strong>
              {selectedIds.size} selected
            </Typography.Text>
            <Button
              danger
              icon={<DeleteOutlined />}
              onClick={() => setBulkDeleteModal(true)}
              size="small"
              disabled={!checkPermission("PROJECT_DASHBOARD", "can_delete")}
            >
              Delete Selected
            </Button>
            <CloseOutlined
              className="selection-bar-close"
              onClick={handleDeselectAll}
            />
          </div>
        )}

        {resumeProject && (
          <div
            className="resume-banner"
            onClick={() => handleResume(resumeProject)}
            role="button"
            tabIndex={0}
            onKeyDown={(e) => e.key === "Enter" && handleResume(resumeProject)}
          >
            <PlayCircleOutlined className="resume-banner-icon" />
            <div className="resume-banner-text">
              <span className="resume-banner-label">Continue:</span>
              <span className="resume-banner-project">
                {resumeProject.project_name}
              </span>
              <span className="resume-banner-dot">&middot;</span>
              <span className="resume-banner-meta">
                {getRelativeTime(resumeProject.modified_at)}
              </span>
            </div>
            <Button
              type="primary"
              size="small"
              className="resume-banner-btn"
              icon={<ArrowRightOutlined />}
            >
              Resume
            </Button>
          </div>
        )}

        <div className="flex-1 overflow-y-auto projectlist_wrap">
          {projects.length ? (
            <>
              <List
                className={`projects-list${
                  fetching ? " projects-list--fetching" : ""
                }`}
                grid={
                  type === "card"
                    ? {
                        gutter: 16,
                        xs: 1,
                        sm: 1,
                        md: 2,
                        lg: 3,
                        xl: 4,
                        xxl: 4,
                      }
                    : undefined
                }
                dataSource={projects}
                renderItem={(project) => (
                  <List.Item key={project.project_id}>
                    <ProjectListCard
                      type={type}
                      details={project}
                      deleteProject={deleteProject}
                      handleCreateOrUpdateProject={handleCreateOrUpdateProject}
                      isDeleting={deletingId === project.project_id}
                      isSelected={selectedIds.has(project.project_id)}
                      onToggleSelect={handleToggleSelect}
                      selectionMode={selectionMode}
                    />
                  </List.Item>
                )}
              />
              {totalProjects > pageSize && (
                <div className="project-list-pagination">
                  <Pagination
                    current={currentPage}
                    total={totalProjects}
                    pageSize={pageSize}
                    onChange={handlePageChange}
                    showSizeChanger={false}
                    showTotal={(total) => `${total} projects`}
                  />
                </div>
              )}
            </>
          ) : searchText || hasLoadedOnce.current ? (
            <div className="no-results-container">
              <div className="no-results-ghost-grid">
                {[0, 1, 2].map((i) => (
                  <div key={i} className="no-results-ghost-card">
                    <div className="no-results-ghost-header">
                      <div className="no-results-ghost-dot" />
                      <div className="no-results-ghost-line no-results-ghost-line--title" />
                    </div>
                    <div className="no-results-ghost-body">
                      <div className="no-results-ghost-line no-results-ghost-line--full" />
                      <div className="no-results-ghost-line no-results-ghost-line--half" />
                    </div>
                    <div className="no-results-ghost-footer">
                      <div className="no-results-ghost-line no-results-ghost-line--small" />
                      <div className="no-results-ghost-line no-results-ghost-line--small" />
                    </div>
                  </div>
                ))}
              </div>
              <div className="no-results-overlay">
                <SearchOutlined className="no-results-overlay-icon" />
                <Typography.Text className="no-results-overlay-text">
                  {searchText
                    ? `No projects found for \u201c${searchText}\u201d`
                    : "No projects yet"}
                </Typography.Text>
                <div className="no-results-overlay-actions">
                  {searchText && (
                    <Button
                      onClick={() => {
                        setSearchText("");
                        clearTimeout(searchTimerRef.current);
                        setCurrentPage(1);
                        getAllProject("", 1, { silent: true });
                      }}
                    >
                      Clear Search
                    </Button>
                  )}
                  <Button
                    type="primary"
                    icon={<PlusOutlined />}
                    onClick={() => handleCreateOrUpdateProject(null)}
                    disabled={
                      !checkPermission("PROJECT_DASHBOARD", "can_write")
                    }
                  >
                    New Project
                  </Button>
                </div>
              </div>
            </div>
          ) : (
            <div className="welcome-container">
              {/* AI Setup Overlay - full screen */}

              {/* Hero Section */}
              <div className="welcome-hero">
                <h1 className="welcome-title">
                  Transform your data with{" "}
                  <span className="welcome-ai">AI</span>
                </h1>
                <p className="welcome-subtitle">
                  Connect your database, explore sample datasets, or let
                  Visitran AI guide you through your first project. Get started
                  in under 60 seconds.
                </p>
              </div>

              {/* Highlights Bar */}
              <div className="welcome-highlights">
                <span>10+ database connectors</span>
                <span className="highlight-dot" />
                <span>No-code &amp; Full-code</span>
                <span className="highlight-dot" />
                <span>Built-in AI assistant</span>
              </div>

              {/* Action Cards */}
              <div className="welcome-section-label">
                CHOOSE HOW TO GET STARTED
              </div>
              <div className="welcome-cards">
                <div
                  className="welcome-card"
                  onClick={() => handleCreateOrUpdateProject(null)}
                >
                  <div className="welcome-card-icon">
                    <DatabaseOutlined />
                  </div>
                  <h3 className="welcome-card-title">Connect Your Database</h3>
                  <p className="welcome-card-desc">
                    Link PostgreSQL, MySQL, Snowflake, BigQuery, or DuckDB and
                    create your first project
                  </p>
                  <Button
                    type="primary"
                    className="welcome-card-btn"
                    disabled={
                      !checkPermission("PROJECT_DASHBOARD", "can_write")
                    }
                  >
                    Connect Database &rarr;
                  </Button>
                  <span className="welcome-card-footnote">
                    Supports 10+ database types
                  </span>
                </div>

                <div className="welcome-card">
                  <div className="welcome-card-icon">
                    <DownloadOutlined />
                  </div>
                  <h3 className="welcome-card-title">Load Sample Project</h3>
                  <p className="welcome-card-desc">
                    Spin up a Jaffle Shop or DVD Rental demo project instantly
                    to explore Visitran features
                  </p>
                  <Dropdown
                    overlay={
                      <Menu mode="vertical" className="sample-projects-menu">
                        <Menu.ItemGroup title="Jaffle Shop">
                          <Menu.Item
                            key="jaffle-starter"
                            onClick={() =>
                              handleLoadProject("jaffleshop_starter")
                            }
                          >
                            Starter
                          </Menu.Item>
                          <Menu.Item
                            key="jaffle-final"
                            onClick={() =>
                              handleLoadProject("jaffleshop_final")
                            }
                          >
                            Finalized
                          </Menu.Item>
                        </Menu.ItemGroup>
                        <Menu.ItemGroup title="DVD Rental Store">
                          <Menu.Item
                            key="dvd-starter"
                            onClick={() => handleLoadProject("dvd_starter")}
                          >
                            Starter
                          </Menu.Item>
                          <Menu.Item
                            key="dvd-final"
                            onClick={() => handleLoadProject("dvd_final")}
                          >
                            Finalized
                          </Menu.Item>
                        </Menu.ItemGroup>
                      </Menu>
                    }
                    trigger={["click"]}
                    placement="bottomLeft"
                  >
                    <Button type="primary" className="welcome-card-btn">
                      Load Sample Project &rarr;
                    </Button>
                  </Dropdown>
                  <span className="welcome-card-footnote">
                    No setup needed &middot; Ready in seconds
                  </span>
                </div>

                <div
                  className="welcome-card welcome-card-accent"
                  onClick={() => {
                    handleAIGuidedSetup();
                  }}
                >
                  <div className="welcome-card-icon">
                    <RocketOutlined />
                  </div>
                  <h3 className="welcome-card-title">AI-Guided Setup</h3>
                  <p className="welcome-card-desc">
                    We&apos;ll create a sample project for you and launch
                    Visitran AI instantly — start transforming data with AI from
                    the first click
                  </p>
                  <Button type="primary" className="welcome-card-btn">
                    Start with AI &rarr;
                  </Button>
                  <span className="welcome-card-footnote">
                    Creates project + opens AI automatically
                  </span>
                </div>
              </div>

              {/* Feature Cards */}
              <div className="welcome-section-label">
                SEE WHAT VISITRAN AI CAN DO
              </div>
              <div className="welcome-powered">
                Powered by <span className="welcome-ai">Visitran AI</span>
              </div>
              <p className="welcome-powered-sub">
                AI features activate once you&apos;re inside a project —
                here&apos;s what awaits you
              </p>
              <div className="welcome-features">
                <div className="welcome-feature">
                  <div className="welcome-feature-icon">
                    <MessageOutlined />
                  </div>
                  <h4>Natural Language Transforms</h4>
                  <p>
                    Describe what you want in plain English — AI writes the
                    transformation logic
                  </p>
                </div>
                <div className="welcome-feature">
                  <div className="welcome-feature-icon">
                    <ToolOutlined />
                  </div>
                  <h4>Smart Suggestions</h4>
                  <p>
                    AI analyzes your schema and recommends joins, filters, and
                    aggregations
                  </p>
                </div>
                <div className="welcome-feature">
                  <div className="welcome-feature-icon">
                    <SearchOutlined />
                  </div>
                  <h4>Data Quality Checks</h4>
                  <p>
                    Automatically detect nulls, duplicates, and anomalies across
                    your tables
                  </p>
                </div>
                <div className="welcome-feature">
                  <div className="welcome-feature-icon">
                    <ExperimentOutlined />
                  </div>
                  <h4>Pipeline Builder</h4>
                  <p>
                    AI helps you chain transformations into end-to-end data
                    pipelines
                  </p>
                </div>
                <div className="welcome-feature">
                  <div className="welcome-feature-icon">
                    <ThunderboltOutlined />
                  </div>
                  <h4>Code &amp; No-Code</h4>
                  <p>
                    Switch between visual drag-and-drop and full Python code
                    anytime
                  </p>
                </div>
              </div>
            </div>
          )}
        </div>
        <DocsFooter />
      </div>
      {/* AI Setup Overlay - full screen */}
      {aiSetupLoading && (
        <div className="ai-setup-overlay">
          <div className="ai-setup-sparkle-group">
            <svg
              className="ai-setup-sparkle ai-setup-sparkle--main"
              viewBox="0 0 24 24"
              fill="none"
            >
              <path
                d="M12 2L13.5 8.5L20 10L13.5 11.5L12 18L10.5 11.5L4 10L10.5 8.5L12 2Z"
                fill="currentColor"
              />
            </svg>
            <svg
              className="ai-setup-sparkle ai-setup-sparkle--top-right"
              viewBox="0 0 24 24"
              fill="none"
            >
              <path
                d="M12 2L13.5 8.5L20 10L13.5 11.5L12 18L10.5 11.5L4 10L10.5 8.5L12 2Z"
                fill="currentColor"
              />
            </svg>
            <svg
              className="ai-setup-sparkle ai-setup-sparkle--bottom-left"
              viewBox="0 0 24 24"
              fill="none"
            >
              <path
                d="M12 2L13.5 8.5L20 10L13.5 11.5L12 18L10.5 11.5L4 10L10.5 8.5L12 2Z"
                fill="currentColor"
              />
            </svg>
          </div>
          <div className="ai-setup-overlay-text">{overlayText.title}</div>
          <div className="ai-setup-overlay-sub">{overlayText.sub}</div>
        </div>
      )}
      <NewProject
        open={openNewProjModal}
        setOpen={setOpenNewProjModal}
        getAllProject={() => {
          setSearchText("");
          clearTimeout(searchTimerRef.current);
          getAllProject("", 1, { silent: true });
        }}
        id={editProjectId}
      />
      <Modal
        open={bulkDeleteModal}
        centered
        okText="Delete All"
        onOk={handleBulkDelete}
        onCancel={() => {
          setBulkDeleteModal(false);
          setBulkDelText("");
        }}
        okButtonProps={{ disabled: bulkDelText !== "DELETE", danger: true }}
        maskClosable={false}
      >
        <Typography style={{ fontWeight: 700, textAlign: "left" }}>
          Delete {selectedIds.size} Project
          {selectedIds.size > 1 ? "s" : ""}?
        </Typography>
        <Typography style={{ textAlign: "left" }}>
          All seed files, models, and related jobs will be permanently deleted
          for each selected project.
        </Typography>
        <div style={{ margin: "8px 0" }}>
          Type <span className="bold">DELETE </span>to confirm
        </div>
        <Input
          placeholder="Enter"
          value={bulkDelText}
          onChange={(e) => setBulkDelText(e.target.value)}
        />
      </Modal>
    </div>
  );
};

export { ProjectListing };
