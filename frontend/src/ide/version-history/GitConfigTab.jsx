import { useState, useEffect, useCallback, memo } from "react";
import PropTypes from "prop-types";
import {
  Alert,
  Button,
  Card,
  Divider,
  Input,
  Modal,
  Select,
  Space,
  Tag,
  Typography,
} from "antd";
import {
  CheckCircleOutlined,
  CloseCircleOutlined,
  CloudServerOutlined,
  DisconnectOutlined,
  ExclamationCircleOutlined,
  GithubOutlined,
  LinkOutlined,
  PlusOutlined,
  SafetyCertificateOutlined,
} from "@ant-design/icons";
import Cookies from "js-cookie";

import { useAxiosPrivate } from "../../service/axios-service";
import { useNotificationService } from "../../service/notification-service";
import { orgStore } from "../../store/org-store";
import { useProjectStore } from "../../store/project-store";
import { useVersionHistoryStore } from "../../store/version-history-store";
import {
  createBranch,
  deleteGitConfig,
  fetchAvailableRepos,
  fetchBranches,
  importFromBranch,
  listProjectFolders,
  saveGitConfig,
  testGitConnection,
  updatePRMode,
} from "./services";

const { Text, Paragraph } = Typography;
const AUTH_TYPE_OPTIONS = [
  { value: "pat", label: "Personal Access Token" },
  { value: "ssh", label: "SSH Key" },
];

const CREATE_BRANCH_VALUE = "__create_new__";

const GitConfigTab = memo(function GitConfigTab({
  projectId,
  gitConfig,
  onConfigSaved,
}) {
  const axiosRef = useAxiosPrivate();
  const { notify } = useNotificationService();
  const orgId = orgStore.getState().selectedOrgId;
  const projectName = useProjectStore((s) => s.projectName);
  const csrfToken = Cookies.get("csrftoken");
  const setGitConfig = useVersionHistoryStore((s) => s.setGitConfig);

  const [selectedMode, setSelectedMode] = useState(null);
  const [repoUrl, setRepoUrl] = useState("");
  const [authType, setAuthType] = useState("pat");
  const provider =
    repoUrl.includes("gitlab.com") ||
    (repoUrl && !repoUrl.includes("github.com") && repoUrl.includes("gitlab"))
      ? "gitlab"
      : "github";
  const [token, setToken] = useState("");
  const [branchName, setBranchName] = useState("main");
  const [availableRepos, setAvailableRepos] = useState([]);
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState(null);
  const [testError, setTestError] = useState(null);
  const [saving, setSaving] = useState(false);
  const [disconnecting, setDisconnecting] = useState(false);

  // Branch creation state
  const [showCreateBranch, setShowCreateBranch] = useState(false);
  const [newBranchName, setNewBranchName] = useState("");
  const [fromBranch, setFromBranch] = useState("main");
  const [creatingBranch, setCreatingBranch] = useState(false);

  // Import from branch state
  const [importEnabled, setImportEnabled] = useState(false);
  const [sourceBranch, setSourceBranch] = useState("");
  const [projectFolders, setProjectFolders] = useState([]);
  const [foldersLoading, setFoldersLoading] = useState(false);
  const [selectedFolder, setSelectedFolder] = useState("");
  const [importing, setImporting] = useState(false);
  const [importResult, setImportResult] = useState(null);

  // PR base branch state
  const [prBaseBranch, setPrBaseBranch] = useState(
    gitConfig?.pr_base_branch || "main"
  );
  const [branches, setBranches] = useState([]);
  const [branchesLoading, setBranchesLoading] = useState(false);
  const [prSaving, setPrSaving] = useState(false);
  const [prSaveResult, setPrSaveResult] = useState(null);

  const isConfigured = !!gitConfig;
  const isGitLab = (gitConfig?.repo_url || repoUrl || "").includes("gitlab");
  const prLabel = isGitLab ? "MR" : "PR";

  useEffect(() => {
    if (gitConfig?.pr_base_branch) {
      setPrBaseBranch(gitConfig.pr_base_branch);
    }
  }, [gitConfig?.pr_base_branch]);

  // Load branches for the PR base branch dropdown when configured
  useEffect(() => {
    if (isConfigured && branches.length === 0) {
      setBranchesLoading(true);
      fetchBranches(axiosRef, orgId, projectId)
        .then((list) => setBranches(list || []))
        .catch(() => {})
        .finally(() => setBranchesLoading(false));
    }
  }, [isConfigured]); // eslint-disable-line

  useEffect(() => {
    loadAvailableRepos();
  }, []); // eslint-disable-line

  const loadAvailableRepos = useCallback(async () => {
    try {
      const repos = await fetchAvailableRepos(axiosRef, orgId, projectId);
      setAvailableRepos(repos || []);
    } catch {
      /* silent */
    }
  }, [axiosRef, orgId, projectId]);

  const handleSelectAvailableRepo = useCallback(
    (repoUrlValue) => {
      const repo = availableRepos.find((r) => r.repo_url === repoUrlValue);
      if (repo) {
        setSelectedMode("custom");
        setRepoUrl(repo.repo_url);
        setAuthType(repo.auth_type);
        setBranchName(repo.branch_name);
      }
    },
    [availableRepos]
  );

  const buildPayload = useCallback(
    (mode) => {
      if (mode === "default") return { repo_type: "default" };
      return {
        repo_type: "custom",
        repo_url: repoUrl,
        auth_type: authType,
        credentials: { token },
        branch_name: branchName,
      };
    },
    [repoUrl, authType, token, branchName]
  );

  const handleTestConnection = useCallback(async () => {
    setTesting(true);
    setTestResult(null);
    setTestError(null);
    try {
      const result = await testGitConnection(
        axiosRef,
        orgId,
        projectId,
        csrfToken,
        buildPayload(selectedMode)
      );
      setTestResult(result);
      // Set branch dropdown from test result
      if (result.branches?.length > 0) {
        const branchNames = result.branches.map((b) => b.name);
        if (!branchNames.includes(branchName)) {
          setBranchName(
            result.repo_info?.default_branch || branchNames[0] || "main"
          );
        }
      }
    } catch (error) {
      setTestError(
        error?.response?.data?.error_message ||
          error?.message ||
          "Connection test failed"
      );
    } finally {
      setTesting(false);
    }
  }, [
    axiosRef,
    orgId,
    projectId,
    csrfToken,
    selectedMode,
    buildPayload,
    branchName,
  ]);

  const handleCreateBranch = useCallback(async () => {
    if (!newBranchName.trim()) return;
    setCreatingBranch(true);
    try {
      await createBranch(axiosRef, orgId, projectId, csrfToken, {
        repo_url: repoUrl,
        credentials: { token },
        branch_name: newBranchName.trim(),
        from_branch: fromBranch,
      });
      // Add the new branch to the list and select it
      const updatedBranches = [
        ...(testResult?.branches || []),
        { name: newBranchName.trim(), protected: false },
      ];
      setTestResult((prev) => ({ ...prev, branches: updatedBranches }));
      setBranchName(newBranchName.trim());
      setShowCreateBranch(false);
      setNewBranchName("");
      notify({
        type: "success",
        message: `Branch "${newBranchName.trim()}" created`,
      });
    } catch (error) {
      notify({
        error:
          error?.response?.data?.error_message ||
          error?.message ||
          "Failed to create branch",
      });
    } finally {
      setCreatingBranch(false);
    }
  }, [
    axiosRef,
    orgId,
    projectId,
    csrfToken,
    repoUrl,
    token,
    newBranchName,
    fromBranch,
    testResult,
    notify,
  ]);

  const handleSourceBranchChange = useCallback(
    async (branch) => {
      setSourceBranch(branch);
      setSelectedFolder("");
      setProjectFolders([]);
      if (!branch) return;
      setFoldersLoading(true);
      try {
        const folders = await listProjectFolders(
          axiosRef,
          orgId,
          projectId,
          csrfToken,
          {
            repo_url: repoUrl,
            credentials: { token },
            source_branch: branch,
          }
        );
        setProjectFolders(folders || []);
      } catch {
        setProjectFolders([]);
      } finally {
        setFoldersLoading(false);
      }
    },
    [axiosRef, orgId, projectId, csrfToken, repoUrl, token]
  );

  // Compute the migration branch name (used for preview + save)
  const migrationBranchName = (() => {
    const slug = (projectName || "project")
      .toLowerCase()
      .replace(/[^a-z0-9_-]/g, "-")
      .replace(/-+/g, "-")
      .replace(/^-|-$/g, "");
    return `${slug}/migrated`;
  })();

  const handleFolderSelect = useCallback(
    (folder) => {
      setSelectedFolder(folder);
      if (folder) {
        // Preview the auto-generated branch name
        setBranchName(migrationBranchName);
      }
    },
    [migrationBranchName]
  );

  const handleSave = useCallback(async () => {
    setSaving(true);
    setImportResult(null);
    try {
      // Step 1: If importing, create the migration branch first
      if (importEnabled && selectedFolder && sourceBranch) {
        try {
          await createBranch(axiosRef, orgId, projectId, csrfToken, {
            repo_url: repoUrl,
            credentials: { token },
            branch_name: migrationBranchName,
            from_branch: sourceBranch,
          });
        } catch (branchErr) {
          const msg =
            branchErr?.response?.data?.error_message ||
            branchErr?.message ||
            "";
          if (!msg.toLowerCase().includes("already exists")) {
            notify({ error: msg || "Failed to create migration branch" });
            setSaving(false);
            return;
          }
        }
      }

      // Step 2: Save git config (branch_name is already set to migration branch)
      const payload = buildPayload(selectedMode);
      const saved = await saveGitConfig(
        axiosRef,
        orgId,
        projectId,
        csrfToken,
        payload
      );
      setGitConfig(saved);

      // Step 3: Import models if enabled
      if (importEnabled && selectedFolder) {
        setImporting(true);
        try {
          const result = await importFromBranch(
            axiosRef,
            orgId,
            projectId,
            csrfToken,
            { source_folder: selectedFolder, source_branch: sourceBranch }
          );
          setImportResult(result);
          notify({
            type: "success",
            message: `Imported ${result.models_imported} models. Executing latest version...`,
          });
          // Step 4: Auto-execute the imported version
          try {
            const { executeVersion } = await import("./services");
            await executeVersion(
              axiosRef,
              orgId,
              projectId,
              csrfToken,
              result.version_number
            );
            notify({
              type: "success",
              message: "Models imported and executed successfully",
            });
          } catch (execErr) {
            const execMsg =
              execErr?.response?.data?.error_message ||
              execErr?.message ||
              "Execution failed";
            notify({
              type: "warning",
              message: `Models imported but execution failed: ${execMsg}. Check your connection settings.`,
            });
          }
        } catch (err) {
          notify({
            error:
              err?.response?.data?.error_message ||
              err?.message ||
              "Import failed",
          });
        } finally {
          setImporting(false);
        }
      } else {
        notify({ type: "success", message: "Git versioning enabled" });
      }
      onConfigSaved?.(saved);
    } catch (error) {
      notify({ error });
    } finally {
      setSaving(false);
    }
  }, [
    axiosRef,
    orgId,
    projectId,
    csrfToken,
    repoUrl,
    token,
    selectedMode,
    buildPayload,
    importEnabled,
    selectedFolder,
    sourceBranch,
    migrationBranchName,
    setGitConfig,
    notify,
    onConfigSaved,
  ]);

  const handleDisconnect = useCallback(() => {
    Modal.confirm({
      title: "Disconnect Git Repository?",
      icon: <ExclamationCircleOutlined />,
      content:
        "This will disable version control for this project. Existing versions will be preserved.",
      okText: "Disconnect",
      okType: "danger",
      onOk: async () => {
        setDisconnecting(true);
        try {
          await deleteGitConfig(axiosRef, orgId, projectId, csrfToken);
          setGitConfig(null);
          notify({ type: "success", message: "Git configuration removed" });
          setSelectedMode(null);
          setTestResult(null);
          setTestError(null);
          setRepoUrl("");
          setToken("");
          setBranchName("main");
        } catch (error) {
          notify({ error });
        } finally {
          setDisconnecting(false);
        }
      },
    });
  }, [axiosRef, orgId, projectId, csrfToken, setGitConfig, notify]);

  const handleDefaultEnable = useCallback(async () => {
    setSelectedMode("default");
    setTesting(true);
    setTestResult(null);
    setTestError(null);
    try {
      const payload = { repo_type: "default" };
      const result = await testGitConnection(
        axiosRef,
        orgId,
        projectId,
        csrfToken,
        payload
      );
      setTestResult(result);
      setSaving(true);
      const saved = await saveGitConfig(
        axiosRef,
        orgId,
        projectId,
        csrfToken,
        payload
      );
      setGitConfig(saved);
      notify({ type: "success", message: "Git versioning enabled" });
      onConfigSaved?.(saved);
    } catch (error) {
      setTestError(
        error?.response?.data?.error_message ||
          error?.message ||
          "Failed to enable default versioning"
      );
    } finally {
      setTesting(false);
      setSaving(false);
    }
  }, [
    axiosRef,
    orgId,
    projectId,
    csrfToken,
    setGitConfig,
    notify,
    onConfigSaved,
  ]);

  const handlePrBaseBranchSave = useCallback(async () => {
    setPrSaving(true);
    setPrSaveResult(null);
    try {
      const updated = await updatePRMode(
        axiosRef,
        orgId,
        projectId,
        csrfToken,
        "manual",
        prBaseBranch
      );
      setGitConfig(updated);
      setPrSaveResult({
        type: "success",
        message: `${prLabel} target branch saved`,
      });
    } catch (error) {
      setPrSaveResult({
        type: "error",
        message:
          error?.response?.data?.error_message ||
          error?.message ||
          `Failed to save ${prLabel} settings`,
      });
    } finally {
      setPrSaving(false);
    }
  }, [
    axiosRef,
    orgId,
    projectId,
    csrfToken,
    prBaseBranch,
    prLabel,
    setGitConfig,
  ]);

  const canTest =
    selectedMode === "default" ||
    (selectedMode === "custom" && repoUrl && token);
  const canSave = testResult?.success === true;

  // Build branch options for the setup dropdown
  const branchOptions = (testResult?.branches || []).map((b) => ({
    value: b.name,
    label: `${b.name}${b.protected ? " (protected)" : ""}`,
  }));
  branchOptions.push({
    value: CREATE_BRANCH_VALUE,
    label: (
      <span>
        <PlusOutlined style={{ marginRight: 4 }} />
        Create new branch...
      </span>
    ),
  });

  const handleBranchSelect = useCallback(
    (value) => {
      if (value === CREATE_BRANCH_VALUE) {
        setShowCreateBranch(true);
        setFromBranch(testResult?.repo_info?.default_branch || "main");
      } else {
        setBranchName(value);
        setShowCreateBranch(false);
      }
    },
    [testResult]
  );

  if (isConfigured) {
    const workingBranch = gitConfig.branch_name || "main";
    const sameBranch = workingBranch === prBaseBranch;

    return (
      <div className="git-config-tab">
        <Alert
          message="Git versioning is active"
          type="success"
          showIcon
          icon={<CheckCircleOutlined />}
          style={{ marginBottom: 16 }}
        />
        <div className="git-config-info">
          <div className="git-config-info-row">
            <Text type="secondary">Repository</Text>
            <Text>
              <LinkOutlined /> {gitConfig.repo_url}
            </Text>
          </div>
          <div className="git-config-info-row">
            <Text type="secondary">Type</Text>
            <Tag color={gitConfig.repo_type === "default" ? "blue" : "green"}>
              {gitConfig.repo_type}
            </Tag>
          </div>
          <div className="git-config-info-row">
            <Text type="secondary">Working Branch</Text>
            <Tag color="cyan">{workingBranch}</Tag>
          </div>
          <div className="git-config-info-row">
            <Text type="secondary">Status</Text>
            <Tag
              color={
                gitConfig.connection_status === "connected"
                  ? "success"
                  : gitConfig.connection_status === "error"
                  ? "error"
                  : "warning"
              }
            >
              {gitConfig.connection_status}
            </Tag>
          </div>
          {gitConfig.last_synced_at && (
            <div className="git-config-info-row">
              <Text type="secondary">Last Synced</Text>
              <Text>{new Date(gitConfig.last_synced_at).toLocaleString()}</Text>
            </div>
          )}
        </div>
        <Divider plain>{prLabel} Workflow</Divider>
        <Alert
          type="info"
          message={`${prLabel}s will be raised from "${workingBranch}" to the target branch below. Use "Create ${prLabel}" in the version timeline.`}
          showIcon
          style={{ marginBottom: 12 }}
        />
        <div className="git-config-form">
          <div className="git-config-field">
            <Text type="secondary" style={{ fontSize: 12 }}>
              {prLabel} Target Branch
            </Text>
            <Select
              value={prBaseBranch}
              onChange={setPrBaseBranch}
              loading={branchesLoading}
              style={{ width: "100%" }}
              placeholder="Select target branch"
              options={branches.map((b) => ({
                value: b.name,
                label: `${b.name}${b.protected ? " (protected)" : ""}`,
              }))}
            />
          </div>
          {sameBranch && (
            <Alert
              type="warning"
              message={`Working branch and target branch are both "${workingBranch}". Select a different target branch.`}
              showIcon
              style={{ marginBottom: 8 }}
            />
          )}
          <Button
            type="primary"
            onClick={handlePrBaseBranchSave}
            loading={prSaving}
            disabled={sameBranch}
            block
            size="small"
          >
            Save
          </Button>
          {prSaveResult && (
            <Alert
              type={prSaveResult.type}
              message={prSaveResult.message}
              showIcon
              closable
              onClose={() => setPrSaveResult(null)}
              style={{ marginTop: 8 }}
            />
          )}
        </div>
        <Divider />
        <Button
          danger
          icon={<DisconnectOutlined />}
          onClick={handleDisconnect}
          loading={disconnecting}
          block
        >
          Disconnect Repository
        </Button>
      </div>
    );
  }

  return (
    <div className="git-config-tab">
      <Paragraph type="secondary" style={{ marginBottom: 16 }}>
        Enable version control to track changes to your transformation models
        with git-backed versioning.
      </Paragraph>
      {availableRepos.length > 0 && (
        <div style={{ marginBottom: 16 }}>
          <Text
            type="secondary"
            style={{ fontSize: 12, display: "block", marginBottom: 4 }}
          >
            Use an existing repository from your organization
          </Text>
          <Select
            placeholder="Select a previously configured repo..."
            style={{ width: "100%" }}
            onChange={handleSelectAvailableRepo}
            options={availableRepos.map((r) => ({
              value: r.repo_url,
              label: `${r.repo_url} (${r.branch_name})`,
            }))}
            allowClear
          />
          <Divider plain style={{ margin: "12px 0" }}>
            or configure new
          </Divider>
        </div>
      )}
      <div className="git-config-cards">
        <Card
          size="small"
          hoverable
          className={`git-config-card ${
            selectedMode === "default" ? "git-config-card-selected" : ""
          }`}
          onClick={() => !testing && !saving && setSelectedMode("default")}
        >
          <Space>
            <CloudServerOutlined style={{ fontSize: 20 }} />
            <div>
              <Text strong>Default Repo</Text>
              <br />
              <Text type="secondary" style={{ fontSize: 12 }}>
                Managed by Visitran
              </Text>
            </div>
          </Space>
        </Card>
        <Card
          size="small"
          hoverable
          className={`git-config-card ${
            selectedMode === "custom" ? "git-config-card-selected" : ""
          }`}
          onClick={() => !testing && !saving && setSelectedMode("custom")}
        >
          <Space>
            <GithubOutlined style={{ fontSize: 20 }} />
            <div>
              <Text strong>Custom Repo</Text>
              <br />
              <Text type="secondary" style={{ fontSize: 12 }}>
                Your own GitHub repo
              </Text>
            </div>
          </Space>
        </Card>
      </div>
      {selectedMode === "default" && (
        <div style={{ marginTop: 16 }}>
          <Alert
            message="Visitran will manage a git repository for you"
            description="Your YAML versions will be stored in a secure, Visitran-managed repository."
            type="info"
            showIcon
            style={{ marginBottom: 12 }}
          />
          <Button
            type="primary"
            icon={<SafetyCertificateOutlined />}
            onClick={handleDefaultEnable}
            loading={testing || saving}
            block
          >
            Enable Default Versioning
          </Button>
        </div>
      )}
      {selectedMode === "custom" && (
        <div className="git-config-form">
          <div className="git-config-field">
            <Text type="secondary" style={{ fontSize: 12 }}>
              Repository URL *
            </Text>
            <Input
              placeholder={
                provider === "gitlab"
                  ? "https://gitlab.com/org/repo"
                  : "https://github.com/owner/repo"
              }
              value={repoUrl}
              onChange={(e) => setRepoUrl(e.target.value)}
            />
          </div>
          <div className="git-config-field">
            <Text type="secondary" style={{ fontSize: 12 }}>
              Authentication *
            </Text>
            <Select
              value={authType}
              onChange={setAuthType}
              options={AUTH_TYPE_OPTIONS}
              style={{ width: "100%" }}
            />
          </div>
          <div className="git-config-field">
            <Text type="secondary" style={{ fontSize: 12 }}>
              {authType === "pat" ? "Personal Access Token *" : "SSH Key *"}
            </Text>
            {authType === "pat" ? (
              <Input.Password
                placeholder={
                  provider === "gitlab"
                    ? "glpat-xxxxxxxxxxxx"
                    : "ghp_xxxxxxxxxxxx"
                }
                value={token}
                onChange={(e) => setToken(e.target.value)}
              />
            ) : (
              <Input.TextArea
                placeholder="Paste your SSH private key..."
                value={token}
                onChange={(e) => setToken(e.target.value)}
                rows={3}
              />
            )}
          </div>
          <div className="git-config-field">
            <Text type="secondary" style={{ fontSize: 12 }}>
              Working Branch *
            </Text>
            {importEnabled && selectedFolder ? (
              <Input value={branchName} disabled />
            ) : testResult?.branches?.length > 0 ? (
              <Select
                value={branchName}
                onChange={handleBranchSelect}
                style={{ width: "100%" }}
                placeholder="Select a branch"
                options={branchOptions}
              />
            ) : (
              <Input
                placeholder="Run Test Connection to load branches"
                value={branchName}
                onChange={(e) => setBranchName(e.target.value)}
                disabled={!testResult}
              />
            )}
          </div>
          {showCreateBranch && (
            <div
              className="git-config-field"
              style={{
                background: "#fafafa",
                padding: 12,
                borderRadius: 6,
                border: "1px solid #f0f0f0",
              }}
            >
              <Text type="secondary" style={{ fontSize: 12 }}>
                New Branch Name *
              </Text>
              <Input
                placeholder="e.g. feature/my-branch"
                value={newBranchName}
                onChange={(e) => setNewBranchName(e.target.value)}
                style={{ marginBottom: 8 }}
              />
              <Text type="secondary" style={{ fontSize: 12 }}>
                Create from
              </Text>
              <Select
                value={fromBranch}
                onChange={setFromBranch}
                style={{ width: "100%", marginBottom: 8 }}
                options={(testResult?.branches || []).map((b) => ({
                  value: b.name,
                  label: b.name,
                }))}
              />
              <Space>
                <Button
                  type="primary"
                  size="small"
                  onClick={handleCreateBranch}
                  loading={creatingBranch}
                  disabled={!newBranchName.trim()}
                >
                  Create
                </Button>
                <Button size="small" onClick={() => setShowCreateBranch(false)}>
                  Cancel
                </Button>
              </Space>
            </div>
          )}
          {testResult?.branches?.length > 0 && (
            <>
              <Divider plain style={{ margin: "12px 0", fontSize: 12 }}>
                Import from existing project
              </Divider>
              <div className="git-config-field">
                <label
                  style={{
                    display: "flex",
                    alignItems: "center",
                    gap: 6,
                    cursor: "pointer",
                  }}
                >
                  <input
                    type="checkbox"
                    checked={importEnabled}
                    onChange={(e) => {
                      setImportEnabled(e.target.checked);
                      if (!e.target.checked) {
                        setSourceBranch("");
                        setProjectFolders([]);
                        setSelectedFolder("");
                      }
                    }}
                  />
                  <Text style={{ fontSize: 12 }}>
                    Import models from an existing project in this repo
                  </Text>
                </label>
              </div>
              {importEnabled && (
                <>
                  <div className="git-config-field">
                    <Text type="secondary" style={{ fontSize: 12 }}>
                      Source Branch
                    </Text>
                    <Select
                      value={sourceBranch || undefined}
                      onChange={handleSourceBranchChange}
                      style={{ width: "100%" }}
                      placeholder="Select branch to import from"
                      options={(testResult?.branches || []).map((b) => ({
                        value: b.name,
                        label: b.name,
                      }))}
                    />
                  </div>
                  {foldersLoading && (
                    <Text type="secondary" style={{ fontSize: 12 }}>
                      Scanning for projects...
                    </Text>
                  )}
                  {projectFolders.length > 0 && (
                    <div className="git-config-field">
                      <Text type="secondary" style={{ fontSize: 12 }}>
                        Source Project
                      </Text>
                      <Select
                        value={selectedFolder || undefined}
                        onChange={handleFolderSelect}
                        style={{ width: "100%" }}
                        placeholder="Select project to import"
                        options={projectFolders.map((f) => ({
                          value: f.name,
                          label: `${f.name} (${f.model_count} models)`,
                        }))}
                      />
                    </div>
                  )}
                  {selectedFolder && (
                    <Alert
                      type="info"
                      message={`Will import "${selectedFolder}" from "${sourceBranch}" → working branch "${branchName}"`}
                      description={`Models will be migrated to project "${projectName}". Commit history from the source branch will be preserved.`}
                      showIcon
                      style={{ marginBottom: 8 }}
                    />
                  )}
                  {sourceBranch &&
                    !foldersLoading &&
                    projectFolders.length === 0 && (
                      <Alert
                        type="warning"
                        message="No project folders found on this branch."
                        showIcon
                        style={{ marginBottom: 8 }}
                      />
                    )}
                </>
              )}
            </>
          )}
          <Space direction="vertical" style={{ width: "100%", marginTop: 12 }}>
            <Button
              icon={<LinkOutlined />}
              onClick={handleTestConnection}
              loading={testing}
              disabled={!canTest}
              block
            >
              Test Connection
            </Button>
            <Button
              type="primary"
              onClick={handleSave}
              loading={saving || importing}
              disabled={!canSave || (importEnabled && !selectedFolder)}
              block
            >
              {importing
                ? "Importing..."
                : importEnabled && selectedFolder
                ? "Save & Import"
                : "Save Configuration"}
            </Button>
          </Space>
          {importResult && (
            <Alert
              type="success"
              message={`Imported ${importResult.models_imported} models`}
              description={
                importResult.schemas_required?.length > 0
                  ? `Required schemas: ${importResult.schemas_required.join(
                      ", "
                    )}. Ensure your connection has access before executing.`
                  : undefined
              }
              showIcon
              style={{ marginTop: 8 }}
            />
          )}
        </div>
      )}
      {testResult && (
        <Alert
          message="Connection successful"
          description={
            <div>
              <Text>
                {testResult.repo_info?.full_name} (
                {testResult.repo_info?.default_branch})
              </Text>
              <br />
              <Text type="secondary">
                {testResult.repo_info?.private ? "Private" : "Public"}{" "}
                repository
              </Text>
            </div>
          }
          type="success"
          showIcon
          icon={<CheckCircleOutlined />}
          style={{ marginTop: 12 }}
          closable
          onClose={() => setTestResult(null)}
        />
      )}
      {testError && (
        <Alert
          message="Connection failed"
          description={testError}
          type="error"
          showIcon
          icon={<CloseCircleOutlined />}
          style={{ marginTop: 12 }}
          closable
          onClose={() => setTestError(null)}
        />
      )}
    </div>
  );
});

GitConfigTab.propTypes = {
  projectId: PropTypes.string.isRequired,
  gitConfig: PropTypes.object,
  onConfigSaved: PropTypes.func,
};
GitConfigTab.displayName = "GitConfigTab";

export { GitConfigTab };
