import { useEffect, useCallback, memo, useState } from "react";
import PropTypes from "prop-types";
import { Alert, message, Segmented, Spin } from "antd";

import { Header } from "./Header";
import { VersionTimeline } from "./VersionTimeline";
import { CommitModal } from "./CommitModal";
import { CompareModal } from "./CompareModal";
import { ViewVersionModal } from "./ViewVersionModal";
import { ExecuteVersionModal } from "./ExecuteVersionModal";
import { RollbackModal } from "./RollbackModal";
import { GitConfigTab } from "./GitConfigTab";
import { DisabledOverlay } from "./DisabledOverlay";
import { useProjectStore } from "../../store/project-store";
import { useVersionHistoryStore } from "../../store/version-history-store";
import { useRefreshModelsStore } from "../../store/refresh-models-store";
import { useAxiosPrivate } from "../../service/axios-service";
import { orgStore } from "../../store/org-store";
import { createVersionPR, fetchGitConfig } from "./services";
import "./version-history.css";

const TAB_OPTIONS = ["History", "Config"];

const VersionHistoryDrawer = memo(function VersionHistoryDrawer({
  isVersionDrawerOpen,
  closeVersionDrawer,
}) {
  const { projectId } = useProjectStore.getState();
  const clearState = useVersionHistoryStore((state) => state.clearState);
  const setGitConfig = useVersionHistoryStore((state) => state.setGitConfig);
  const gitConfig = useVersionHistoryStore((state) => state.gitConfig);
  const isVersioningEnabled = useVersionHistoryStore(
    (state) => state.isVersioningEnabled
  );
  const saveCounter = useVersionHistoryStore((state) => state.saveCounter);

  const axiosRef = useAxiosPrivate();
  const [refreshKey, setRefreshKey] = useState(0);
  const [activeTab, setActiveTab] = useState("History");
  const [configLoading, setConfigLoading] = useState(true);
  const [viewOpen, setViewOpen] = useState(false);
  const [viewTarget, setViewTarget] = useState(null);
  const [compareOpen, setCompareOpen] = useState(false);
  const [compareVersionA, setCompareVersionA] = useState(null);
  const [compareVersionB, setCompareVersionB] = useState(null);
  const [rollbackOpen, setRollbackOpen] = useState(false);
  const [rollbackTarget, setRollbackTarget] = useState(null);
  const [executeOpen, setExecuteOpen] = useState(false);
  const [executeTarget, setExecuteTarget] = useState(null);
  const [syncStatus, setSyncStatus] = useState({ pending: 0, failed: 0 });
  const [creatingPR, setCreatingPR] = useState(false);

  useEffect(() => {
    if (isVersionDrawerOpen && projectId) {
      loadGitConfig();
    }
  }, [isVersionDrawerOpen, projectId]); // eslint-disable-line

  const loadGitConfig = useCallback(async () => {
    setConfigLoading(true);
    try {
      const orgId = orgStore.getState().selectedOrgId;
      const config = await fetchGitConfig(axiosRef, orgId, projectId);
      setGitConfig(config);
      if (!config) setActiveTab("Config");
    } catch {
      setGitConfig(null);
      setActiveTab("Config");
    } finally {
      setConfigLoading(false);
    }
  }, [axiosRef, projectId, setGitConfig]);

  useEffect(() => {
    if (!isVersionDrawerOpen) {
      setViewOpen(false);
      setViewTarget(null);
      setCompareOpen(false);
      setCompareVersionA(null);
      setCompareVersionB(null);
      setRollbackOpen(false);
      setRollbackTarget(null);
      setExecuteOpen(false);
      setExecuteTarget(null);
    }
  }, [isVersionDrawerOpen]);

  useEffect(() => {
    return () => clearState();
  }, []); // eslint-disable-line

  // Refresh once after a transform save (auto-commit needs ~3s to land on GitHub)
  useEffect(() => {
    if (!saveCounter || !isVersionDrawerOpen || !projectId) return;
    const timer = setTimeout(() => {
      setRefreshKey((k) => k + 1);
    }, 3000);
    return () => clearTimeout(timer);
  }, [saveCounter, isVersionDrawerOpen, projectId]);

  const handleCommitSuccess = useCallback(() => {
    // Immediate refresh to show "pending" state, then delayed refresh
    // after the async commit thread lands on GitHub
    setRefreshKey((k) => k + 1);
    setTimeout(() => setRefreshKey((k) => k + 1), 3000);
  }, []);
  const handleViewVersion = useCallback((v) => {
    setViewTarget(v.version_number);
    setViewOpen(true);
  }, []);
  const handleCloseView = useCallback(() => {
    setViewOpen(false);
    setViewTarget(null);
  }, []);
  const handleCompareVersion = useCallback((v) => {
    if (v.version_number > 1) {
      setCompareVersionA(v.version_number - 1);
      setCompareVersionB(v.version_number);
    } else {
      setCompareVersionA(v.version_number);
      setCompareVersionB("current");
    }
    setCompareOpen(true);
  }, []);
  const handleCloseCompare = useCallback(() => {
    setCompareOpen(false);
    setCompareVersionA(null);
    setCompareVersionB(null);
  }, []);
  const handleRollbackVersion = useCallback((v) => {
    setRollbackTarget(v.version_number);
    setRollbackOpen(true);
  }, []);
  const handleCloseRollback = useCallback(() => {
    setRollbackOpen(false);
    setRollbackTarget(null);
  }, []);
  const handleRollbackSuccess = useCallback(() => {
    setRollbackOpen(false);
    setRollbackTarget(null);
    setRefreshKey((k) => k + 1);
  }, []);
  const handleExecuteVersion = useCallback((v) => {
    setExecuteTarget({
      versionNumber: v.version_number,
      commitSha: v.commit_sha || "",
    });
    setExecuteOpen(true);
  }, []);
  const handleCloseExecute = useCallback(() => {
    setExecuteOpen(false);
    setExecuteTarget(null);
  }, []);
  const { setRefreshModels } = useRefreshModelsStore();
  const handleExecuteSuccess = useCallback(() => {
    setRefreshKey((k) => k + 1);
    setRefreshModels(true);
  }, [setRefreshModels]);
  const handleCreatePR = useCallback(
    async (versionNumber) => {
      setCreatingPR(true);
      try {
        const orgId = orgStore.getState().selectedOrgId;
        const csrfToken = (await import("js-cookie")).default.get("csrftoken");
        const res = await createVersionPR(
          axiosRef,
          orgId,
          projectId,
          csrfToken,
          versionNumber
        );
        message.success(
          <span>
            PR #{res.pr_number} created —{" "}
            <a href={res.pr_url} target="_blank" rel="noreferrer">
              View PR
            </a>
          </span>
        );
        setRefreshKey((k) => k + 1);
      } catch (err) {
        if (err?.response?.status === 409) {
          const data = err.response.data?.data || err.response.data;
          message.info(
            <span>
              PR #{data.pr_number} already exists —{" "}
              <a href={data.pr_url} target="_blank" rel="noreferrer">
                View PR
              </a>
            </span>
          );
          setRefreshKey((k) => k + 1);
        } else {
          message.error(
            err?.response?.data?.error_message || "Failed to create PR"
          );
        }
      } finally {
        setCreatingPR(false);
      }
    },
    [axiosRef, projectId]
  );
  const handleExecuteRollback = useCallback((vn) => {
    setExecuteOpen(false);
    setExecuteTarget(null);
    setRollbackTarget(vn);
    setRollbackOpen(true);
  }, []);
  const handleConfigSaved = useCallback(() => {
    setActiveTab("History");
    setRefreshKey((k) => k + 1);
  }, []);
  const handleConfigureClick = useCallback(() => setActiveTab("Config"), []);
  const handleSyncStatusChange = useCallback((s) => setSyncStatus(s), []);

  const renderTabContent = () => {
    if (configLoading)
      return (
        <div className="git-config-loading">
          <Spin size="small" />
        </div>
      );
    if (activeTab === "Config")
      return (
        <GitConfigTab
          projectId={projectId}
          gitConfig={gitConfig}
          onConfigSaved={handleConfigSaved}
        />
      );
    if (!isVersioningEnabled)
      return <DisabledOverlay onConfigure={handleConfigureClick} />;
    if (activeTab === "History") {
      return (
        <>
          {isVersioningEnabled && syncStatus.failed > 0 && (
            <Alert
              type="error"
              showIcon
              message={
                <>
                  Git sync error &mdash;{" "}
                  <a onClick={handleConfigureClick}>check your configuration</a>
                </>
              }
              banner
              style={{ marginBottom: 8 }}
            />
          )}
          {isVersioningEnabled &&
            syncStatus.pending > 0 &&
            syncStatus.failed === 0 && (
              <Alert
                type="warning"
                showIcon
                message={`${syncStatus.pending} version${
                  syncStatus.pending > 1 ? "s" : ""
                } pending git sync`}
                banner
                style={{ marginBottom: 8 }}
              />
            )}
          <VersionTimeline
            key={refreshKey}
            onViewVersion={handleViewVersion}
            onCompareVersion={handleCompareVersion}
            onRollbackVersion={handleRollbackVersion}
            onExecuteVersion={handleExecuteVersion}
            onSyncStatusChange={handleSyncStatusChange}
            onCreatePR={handleCreatePR}
            creatingPR={creatingPR}
          />
        </>
      );
    }
    return null;
  };

  return (
    <div className="chat-ai-container">
      <Header closeDrawer={closeVersionDrawer} onSync={() => setRefreshKey((k) => k + 1)} />
      <div className="version-history-tabs">
        <Segmented
          value={activeTab}
          onChange={setActiveTab}
          options={TAB_OPTIONS}
          block
          size="small"
        />
      </div>
      <div className="version-history-body">{renderTabContent()}</div>
      <CommitModal onCommitSuccess={handleCommitSuccess} />
      <ViewVersionModal
        open={viewOpen}
        onClose={handleCloseView}
        versionNumber={viewTarget}
      />
      <CompareModal
        open={compareOpen}
        onClose={handleCloseCompare}
        initialVersionA={compareVersionA}
        initialVersionB={compareVersionB}
      />
      <RollbackModal
        open={rollbackOpen}
        onClose={handleCloseRollback}
        targetVersion={rollbackTarget}
        onRollbackSuccess={handleRollbackSuccess}
      />
      <ExecuteVersionModal
        open={executeOpen}
        onClose={handleCloseExecute}
        targetVersion={executeTarget}
        onExecuteSuccess={handleExecuteSuccess}
        onRollbackToVersion={handleExecuteRollback}
      />
    </div>
  );
});

VersionHistoryDrawer.propTypes = {
  isVersionDrawerOpen: PropTypes.bool.isRequired,
  closeVersionDrawer: PropTypes.func.isRequired,
};
VersionHistoryDrawer.displayName = "VersionHistoryDrawer";

export { VersionHistoryDrawer };
