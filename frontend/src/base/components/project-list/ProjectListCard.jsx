import { memo, useState, useMemo, useCallback, useEffect } from "react";
import {
  ExclamationCircleFilled,
  InfoCircleFilled,
  CheckCircleFilled,
  CalendarOutlined,
  FieldTimeOutlined,
  FolderOutlined,
  EditOutlined,
  DeleteOutlined,
  CodeOutlined,
  MessageOutlined,
  ShareAltOutlined,
} from "@ant-design/icons";
import { useNavigate } from "react-router-dom";
import PropTypes from "prop-types";
import {
  Typography,
  message,
  Modal,
  Input,
  Avatar,
  Checkbox,
  Spin,
  Tooltip,
} from "antd";

import { getRelativeTime, checkPermission } from "../../../common/helpers";
import { useProjectStore } from "../../../store/project-store";

import "./ProjectListCard.css";

let ShareProjectModal;
try {
  ShareProjectModal =
    require("../../../plugins/project-sharing/ShareProjectModal").default;
} catch {
  // Plugin not available in OSS
}

function ProjectListCard({
  type,
  details,
  deleteProject,
  handleCreateOrUpdateProject,
  isDeleting,
  isSelected,
  onToggleSelect,
  selectionMode,
}) {
  const navigate = useNavigate();
  const { setProjectName } = useProjectStore();
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [delText, setDelText] = useState("");
  const [isShareModalOpen, setIsShareModalOpen] = useState(false);
  const [sharedUsers, setSharedUsers] = useState(details?.shared_users || []);

  useEffect(() => {
    setSharedUsers(details?.shared_users || []);
  }, [details?.shared_users]);

  /* ---------- derive status ---------- */
  const status = useMemo(() => {
    if (details?.is_sample) return "sample";
    if (details?.user_role && details.user_role !== "OWNER") return "shared";
    return "owned";
  }, [details?.is_sample, details?.user_role]);
  const hasFailed = details?.total_failed_job > 0;

  /* ---------- click handler ---------- */
  const handleCardClick = useCallback(() => {
    if (details?.status && details?.is_sample) {
      message.info("Jaffle Shop Project Still In Progress.", 3);
      return;
    }
    setProjectName(details?.project_name);
    navigate(`/ide/project/${details?.project_id}`);
  }, [details, navigate, setProjectName]);

  return (
    <>
      <div
        className={`project-list-card-wrapper status-${status} ${
          hasFailed ? "has-failed" : ""
        } ${
          type === "card"
            ? "project-list-card--compact"
            : "project-list-card--full"
        } ${isDeleting ? "project-list-card--deleting" : ""} ${
          selectionMode || isSelected ? "project-list-card--selection-mode" : ""
        }`}
      >
        {/* deleting overlay */}
        {isDeleting && (
          <div className="project-list-card-delete-overlay">
            <Spin size="small" />
            <Typography.Text className="project-list-card-delete-text">
              Deleting...
            </Typography.Text>
          </div>
        )}

        {/* ---------- header ---------- */}
        <div className="project-list-card-header">
          <div className="project-list-card-header-left">
            <img
              src={details?.db_icon}
              className="project-list-card-db-source-icon"
              alt="db-icon"
            />
            <div className="project-list-card-title-group">
              <Typography.Text
                className="project-list-card-title"
                ellipsis={{ tooltip: details?.project_name }}
              >
                {details?.project_name}
              </Typography.Text>
              <span className="project-list-card-modified-hint">
                <FieldTimeOutlined /> Modified{" "}
                {getRelativeTime(details?.modified_at)}
              </span>
            </div>
          </div>

          <div className="project-list-card-badges">
            {details?.project_type && (
              <span className="project-list-card-badge-type">
                {details.project_type}
              </span>
            )}
          </div>

          {/* shared user avatars */}
          {sharedUsers.length > 0 && (
            <div
              className="project-list-card-shared-avatars"
              role="button"
              tabIndex={0}
              onClick={(e) => {
                e.stopPropagation();
                if (ShareProjectModal) setIsShareModalOpen(true);
              }}
              onKeyDown={(e) => {
                if (e.key === "Enter" && ShareProjectModal)
                  setIsShareModalOpen(true);
              }}
            >
              <Avatar.Group maxCount={3} size={20}>
                {sharedUsers.map((user) => (
                  <Tooltip
                    key={user.user_id || user.username}
                    title={user.username || user.email}
                  >
                    <Avatar
                      size={20}
                      className="project-list-card-shared-avatar"
                      src={user.profile_picture_url}
                    >
                      {user.username
                        ? user.username.charAt(0).toUpperCase()
                        : "U"}
                    </Avatar>
                  </Tooltip>
                ))}
              </Avatar.Group>
            </div>
          )}
        </div>

        {/* ---------- body (clickable) ---------- */}
        <div
          className="project-list-card-clickable-content"
          role="button"
          tabIndex={0}
          onClick={handleCardClick}
          onKeyDown={(e) => e.key === "Enter" && handleCardClick()}
        >
          {/* description */}
          <div className="project-list-card-detail-section project-list-card-desc-section">
            <Typography.Paragraph
              className="project-list-card-desc"
              ellipsis={{
                rows: 2,
                tooltip: details?.description
                  ? {
                      title: details.description,
                      placement: "bottomLeft",
                      overlayStyle: { maxWidth: 400 },
                    }
                  : false,
              }}
            >
              {details?.description || "\u00A0"}
            </Typography.Paragraph>
          </div>

          {/* models & chats */}
          <div className="project-list-card-detail-section project-list-card-counts-section project-list-card-border-top">
            <span className="project-list-card-count-item">
              <CodeOutlined className="project-list-card-count-icon" />
              <Typography.Text className="project-list-card-count-text">
                {details?.total_models ?? 0} Models
              </Typography.Text>
            </span>
            <span className="project-list-card-count-item">
              <MessageOutlined className="project-list-card-count-icon" />
              <Typography.Text className="project-list-card-count-text">
                {details?.total_ai_chats ?? 0} AI Chats
              </Typography.Text>
            </span>
          </div>

          {/* job stats */}
          <div className="project-list-card-detail-section project-list-card-border-top">
            {!details?.total_scheduled_jobs &&
            !details?.total_active_jobs &&
            !details?.total_failed_job ? (
              <Typography.Text className="project-list-card-no-jobs">
                No jobs scheduled yet
              </Typography.Text>
            ) : (
              <>
                <Typography className="project-list-card-jobs-title">
                  Jobs
                </Typography>
                <div className="project-list-card-progress-section">
                  <Typography.Text>
                    <ExclamationCircleFilled className="project-list-card-status-icon warning" />
                    {details?.total_scheduled_jobs} Scheduled
                  </Typography.Text>
                  <Typography.Text>
                    <InfoCircleFilled className="project-list-card-status-icon info" />
                    {details?.total_active_jobs} In&nbsp;Progress
                  </Typography.Text>
                  <Typography.Text>
                    <CheckCircleFilled className="project-list-card-status-icon failed" />
                    {details?.total_failed_job} Failed
                  </Typography.Text>
                </div>
              </>
            )}
          </div>

          {/* timeline */}
          <div className="project-list-card-detail-section project-list-card-timeline-section project-list-card-border-top">
            <span className="project-list-card-timeline-item">
              <CalendarOutlined />
              <Typography.Text className="project-list-card-created-time">
                Created:&nbsp;
                {new Date(details?.created_at).toISOString().split("T")[0]}
              </Typography.Text>
            </span>
            <span className="project-list-card-timeline-item">
              <FieldTimeOutlined />
              <Typography.Text className="project-list-card-modified-time">
                Modified:&nbsp;
                {new Date(details?.modified_at).toISOString().split("T")[0]}
              </Typography.Text>
            </span>
          </div>

          {/* author + shared users */}
          <div className="project-list-card-detail-section project-list-card-author-section project-list-card-border-top">
            <div className="project-list-card-author-left">
              <Avatar
                size={20}
                className="project-list-card-avatar"
                src={details?.created_by?.image || details?.created_by?.avatar}
              >
                {details?.created_by?.username
                  ? details.created_by.username.charAt(0).toUpperCase()
                  : "U"}
              </Avatar>
              <span className="project-list-card-author-text">
                {details?.created_by?.username}
              </span>
            </div>
          </div>
        </div>

        {/* ---------- hover actions ---------- */}
        <div className="project-list-card-actions">
          <button
            className="project-list-card-action-btn"
            onClick={(e) => {
              e.stopPropagation();
              handleCardClick();
            }}
          >
            <FolderOutlined /> Open
          </button>
          {checkPermission("PROJECT_DASHBOARD", "can_write") && (
            <button
              className="project-list-card-action-btn"
              onClick={(e) => {
                e.stopPropagation();
                handleCreateOrUpdateProject(details?.project_id);
              }}
            >
              <EditOutlined /> Edit
            </button>
          )}
          {ShareProjectModal &&
            checkPermission("PROJECT_DASHBOARD", "can_write") && (
              <button
                className="project-list-card-action-btn"
                onClick={(e) => {
                  e.stopPropagation();
                  setIsShareModalOpen(true);
                }}
              >
                <ShareAltOutlined /> Share
              </button>
            )}
          {status !== "shared" &&
            checkPermission("PROJECT_DASHBOARD", "can_delete") && (
              <button
                className="project-list-card-action-btn"
                onClick={(e) => {
                  e.stopPropagation();
                  setIsModalOpen(true);
                }}
              >
                <DeleteOutlined /> Delete
              </button>
            )}
          {status !== "shared" && (
            <Checkbox
              className="project-list-card-action-checkbox"
              checked={isSelected}
              onClick={(e) => e.stopPropagation()}
              onChange={() => onToggleSelect(details?.project_id)}
            />
          )}
        </div>
      </div>

      {/* ---------- delete modal ---------- */}
      <Modal
        open={isModalOpen}
        centered
        okText="Delete"
        onOk={() => {
          setIsModalOpen(false);
          deleteProject(details?.project_id);
        }}
        onCancel={() => setIsModalOpen(false)}
        okButtonProps={{
          disabled: delText !== "DELETE",
          danger: true,
        }}
        maskClosable={false}
      >
        <Typography className="project-list-card-modal-title">
          Delete the Project ?
        </Typography>
        <Typography className="project-list-card-modal-text">
          All seed files and related jobs will be permanently deleted.
        </Typography>
        <div className="project-list-card-modal-confirm-text">
          Type <span className="bold">DELETE&nbsp;</span>to confirm
        </div>
        <Input
          placeholder="Enter"
          value={delText}
          onChange={(e) => setDelText(e.target.value)}
        />
      </Modal>

      {ShareProjectModal && (
        <ShareProjectModal
          open={isShareModalOpen}
          onClose={() => setIsShareModalOpen(false)}
          projectId={details?.project_id}
          projectName={details?.project_name}
          onShareUpdate={setSharedUsers}
        />
      )}
    </>
  );
}

ProjectListCard.propTypes = {
  type: PropTypes.oneOf(["card", "list"]).isRequired,
  details: PropTypes.shape({
    project_id: PropTypes.string,
    project_name: PropTypes.string,
    db_icon: PropTypes.string,
    db_name: PropTypes.string,
    description: PropTypes.string,
    status: PropTypes.bool,
    is_sample: PropTypes.bool,
    project_type: PropTypes.string,
    total_scheduled_jobs: PropTypes.number,
    total_active_jobs: PropTypes.number,
    total_failed_job: PropTypes.number,
    total_models: PropTypes.number,
    total_ai_chats: PropTypes.number,
    created_at: PropTypes.string,
    modified_at: PropTypes.string,
    user_role: PropTypes.string,
    created_by: PropTypes.shape({
      username: PropTypes.string,
      avatar: PropTypes.string,
      image: PropTypes.string,
    }),
    shared_users: PropTypes.arrayOf(
      PropTypes.shape({
        user_id: PropTypes.string,
        username: PropTypes.string,
        email: PropTypes.string,
        avatar: PropTypes.string,
        image: PropTypes.string,
      })
    ),
  }).isRequired,
  isDeleting: PropTypes.bool,
  isSelected: PropTypes.bool,
  selectionMode: PropTypes.bool,
  deleteProject: PropTypes.func.isRequired,
  handleCreateOrUpdateProject: PropTypes.func.isRequired,
  onToggleSelect: PropTypes.func,
};

export default memo(ProjectListCard);
