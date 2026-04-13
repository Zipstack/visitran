import { useState, useEffect, useCallback } from "react";
import PropTypes from "prop-types";
import {
  Timeline,
  Button,
  Tag,
  Dropdown,
  Typography,
  Empty,
  Tooltip,
} from "antd";
import {
  CheckCircleOutlined,
  ClockCircleOutlined,
  CloudSyncOutlined,
  CloseCircleOutlined,
  GithubOutlined,
  EyeOutlined,
  MoreOutlined,
  PlayCircleOutlined,
  ReloadOutlined,
  RollbackOutlined,
  SwapOutlined,
  SyncOutlined,
  UserOutlined,
} from "@ant-design/icons";
import Cookies from "js-cookie";

import { useAxiosPrivate } from "../../service/axios-service";
import { useNotificationService } from "../../service/notification-service";
import { useProjectStore } from "../../store/project-store";
import { orgStore } from "../../store/org-store";
import { useVersionHistoryStore } from "../../store/version-history-store";
import { SpinnerLoader } from "../../widgets/spinner_loader";
import { fetchVersionHistory, retryGitSync } from "./services";

const PAGE_SIZE = 10;

function GitSyncBadge({ status }) {
  if (!status || status === "not_applicable") return null;
  if (status === "synced") {
    return (
      <Tooltip title="Synced to git">
        <Tag
          icon={<CheckCircleOutlined />}
          color="success"
          style={{ fontSize: 10, margin: 0 }}
        >
          synced
        </Tag>
      </Tooltip>
    );
  }
  if (status === "pending") {
    return (
      <Tooltip title="Syncing to git...">
        <Tag
          icon={<SyncOutlined spin />}
          color="processing"
          style={{ fontSize: 10, margin: 0 }}
        >
          syncing
        </Tag>
      </Tooltip>
    );
  }
  if (status === "failed") {
    return (
      <Tooltip title="Git sync failed">
        <Tag
          icon={<CloseCircleOutlined />}
          color="error"
          style={{ fontSize: 10, margin: 0 }}
        >
          failed
        </Tag>
      </Tooltip>
    );
  }
  return null;
}

GitSyncBadge.propTypes = { status: PropTypes.string };

function VersionTimeline({
  onViewVersion,
  onCompareVersion,
  onRollbackVersion,
  onExecuteVersion,
  onSyncStatusChange,
  onCreatePR,
  creatingPR,
}) {
  const [versions, setVersions] = useState([]);
  const [page, setPage] = useState(1);
  const [totalCount, setTotalCount] = useState(0);
  const [loading, setLoading] = useState(false);
  const [retryingId, setRetryingId] = useState(null);

  const axiosRef = useAxiosPrivate();
  const { notify } = useNotificationService();
  const projectId = useProjectStore((state) => state.projectId);
  const orgId = orgStore.getState().selectedOrgId;
  const isVersioningEnabled = useVersionHistoryStore(
    (s) => s.isVersioningEnabled
  );
  const gitConfig = useVersionHistoryStore((s) => s.gitConfig);
  const prWorkflowEnabled = gitConfig?.pr_mode !== "disabled";
  const isGitLab = (gitConfig?.repo_url || "").includes("gitlab");
  const prLabel = isGitLab ? "MR" : "PR";

  const loadVersions = useCallback(
    async (pageNum, append = false) => {
      if (!projectId) return;
      setLoading(true);
      try {
        const data = await fetchVersionHistory(
          axiosRef,
          orgId,
          projectId,
          pageNum,
          PAGE_SIZE
        );
        const items = data.page_items || [];
        const updatedVersions = append ? [...versions, ...items] : items;
        setVersions(updatedVersions);
        setTotalCount(data.total_count || 0);
        setPage(pageNum);
        if (onSyncStatusChange) {
          const pending = updatedVersions.filter(
            (v) => v.git_sync_status === "pending"
          ).length;
          const failed = updatedVersions.filter(
            (v) => v.git_sync_status === "failed"
          ).length;
          onSyncStatusChange({ pending, failed });
        }
      } catch (error) {
        notify({ error });
      } finally {
        setLoading(false);
      }
    },
    [axiosRef, orgId, projectId, notify]
  ); // eslint-disable-line

  useEffect(() => {
    setVersions([]);
    setPage(1);
    loadVersions(1);
  }, [projectId]); // eslint-disable-line

  const handleRetrySync = useCallback(
    async (versionId) => {
      if (!projectId) return;
      setRetryingId(versionId);
      try {
        const csrfToken = Cookies.get("csrftoken");
        await retryGitSync(axiosRef, orgId, projectId, csrfToken, versionId);
        notify({ type: "success", message: "Git sync completed" });
        loadVersions(1);
      } catch (error) {
        notify({ error });
      } finally {
        setRetryingId(null);
      }
    },
    [axiosRef, orgId, projectId, notify, loadVersions]
  );

  const hasMore = versions.length < totalCount;

  const getActionItems = (version) => {
    const items = [
      {
        key: "view",
        label: "View details",
        icon: <EyeOutlined />,
        onClick: () => onViewVersion?.(version),
      },
      {
        key: "compare",
        label:
          version.version_number > 1
            ? "Compare with previous"
            : "Compare versions",
        icon: <SwapOutlined />,
        onClick: () => onCompareVersion?.(version),
      },
      {
        key: "execute",
        label: "Execute version",
        icon: <PlayCircleOutlined />,
        onClick: () => onExecuteVersion?.(version),
      },
    ];
    const latestVersion = versions.length > 0 ? versions[0].version_number : 0;
    if (
      version.version_number > 1 &&
      version.version_number === latestVersion
    ) {
      items.push({
        key: "rollback",
        label: "Rollback to previous version",
        icon: <RollbackOutlined />,
        onClick: () => onRollbackVersion?.(version),
      });
    }
    if (isVersioningEnabled && version.git_sync_status === "failed") {
      items.push({
        key: "retry-sync",
        label: "Retry git sync",
        icon: <CloudSyncOutlined />,
        onClick: () => handleRetrySync(version.version_id),
      });
    }
    if (
      prWorkflowEnabled &&
      version.git_sync_status === "synced" &&
      !version.pr_number
    ) {
      items.push({
        key: "create-pr",
        label: creatingPR ? `Creating ${prLabel}...` : `Create ${prLabel}`,
        icon: <GithubOutlined />,
        disabled: creatingPR,
        onClick: () => onCreatePR?.(version.version_number),
      });
    }
    return items;
  };

  const formatDate = (dateStr) =>
    dateStr ? new Date(dateStr).toLocaleString() : "";

  // Determine which version is current: prefer is_current flag, fall back to highest version_number
  const currentVersionNumber = (() => {
    const explicit = versions.find((v) => v.is_current);
    if (explicit) return explicit.version_number;
    return versions.length > 0 ? versions[0].version_number : null;
  })();

  const timelineItems = versions.map((version) => ({
    key: version.version_id || version.version_number,
    color: version.is_auto_commit ? "gray" : "blue",
    children: (
      <div className="version-card">
        <div className="version-card-header">
          <span className="version-card-number">v{version.version_number}</span>
          <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
            {version.version_number === currentVersionNumber && (
              <Tag color="green" style={{ fontSize: 10, margin: 0 }}>
                Current
              </Tag>
            )}
            {version.pr_url && (
              <Tag
                icon={<GithubOutlined />}
                color="purple"
                style={{ fontSize: 10, margin: 0, cursor: "pointer" }}
                onClick={() => window.open(version.pr_url, "_blank")}
              >
                PR #{version.pr_number}
              </Tag>
            )}
            {isVersioningEnabled && (
              <GitSyncBadge status={version.git_sync_status} />
            )}
            {isVersioningEnabled && version.git_sync_status === "failed" && (
              <Tooltip title="Retry git sync">
                <Button
                  type="text"
                  size="small"
                  icon={
                    <ReloadOutlined spin={retryingId === version.version_id} />
                  }
                  onClick={() => handleRetrySync(version.version_id)}
                  disabled={retryingId === version.version_id}
                />
              </Tooltip>
            )}
            {version.is_auto_commit && (
              <Tag color="default" style={{ fontSize: 10, margin: 0 }}>
                auto
              </Tag>
            )}
            <Dropdown
              menu={{ items: getActionItems(version) }}
              trigger={["click"]}
            >
              <Button type="text" size="small" icon={<MoreOutlined />} />
            </Dropdown>
          </div>
        </div>
        {version.commit_message && (
          <div className="version-card-message">{version.commit_message}</div>
        )}
        {version.model_count > 0 && (
          <div className="version-card-models">
            <Typography.Text type="secondary" style={{ fontSize: 11 }}>
              {version.model_count} model{version.model_count !== 1 ? "s" : ""}
              {version.model_names?.length > 0 &&
                `: ${version.model_names.join(", ")}`}
            </Typography.Text>
          </div>
        )}
        <div className="version-card-meta">
          <span>
            <UserOutlined style={{ marginRight: 2 }} />
            {version.committed_by?.name || "system"}
          </span>
          <span>
            <ClockCircleOutlined style={{ marginRight: 2 }} />
            {formatDate(version.created_at)}
          </span>
        </div>
      </div>
    ),
  }));

  if (loading && versions.length === 0) return <SpinnerLoader />;

  if (!loading && versions.length === 0) {
    return (
      <div className="version-empty">
        <Empty
          description={
            <Typography.Text type="secondary">
              No versions yet. Commit to create the first version.
            </Typography.Text>
          }
        />
      </div>
    );
  }

  return (
    <div className="version-timeline-container">
      <Timeline items={timelineItems} />
      {hasMore && (
        <div className="version-load-more">
          <Button
            type="link"
            onClick={() => loadVersions(page + 1, true)}
            loading={loading}
          >
            Load more
          </Button>
        </div>
      )}
    </div>
  );
}

VersionTimeline.propTypes = {
  onViewVersion: PropTypes.func,
  onCompareVersion: PropTypes.func,
  onRollbackVersion: PropTypes.func,
  onExecuteVersion: PropTypes.func,
  onSyncStatusChange: PropTypes.func,
  onCreatePR: PropTypes.func,
  creatingPR: PropTypes.bool,
};

export { VersionTimeline };
