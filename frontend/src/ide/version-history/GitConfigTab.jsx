import { useState, useEffect, useCallback, memo } from "react";
import PropTypes from "prop-types";
import {
  Alert,
  Button,
  Card,
  Divider,
  Input,
  Modal,
  Segmented,
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
  SafetyCertificateOutlined,
} from "@ant-design/icons";
import Cookies from "js-cookie";

import { useAxiosPrivate } from "../../service/axios-service";
import { useNotificationService } from "../../service/notification-service";
import { orgStore } from "../../store/org-store";
import { useVersionHistoryStore } from "../../store/version-history-store";
import {
  deleteGitConfig,
  fetchAvailableRepos,
  fetchBranches,
  saveGitConfig,
  testGitConnection,
  updatePRMode,
} from "./services";

const { Text, Paragraph } = Typography;
const AUTH_TYPE_OPTIONS = [
  { value: "pat", label: "Personal Access Token" },
  { value: "ssh", label: "SSH Key" },
];

const GitConfigTab = memo(function GitConfigTab({
  projectId,
  gitConfig,
  onConfigSaved,
}) {
  const axiosRef = useAxiosPrivate();
  const { notify } = useNotificationService();
  const orgId = orgStore.getState().selectedOrgId;
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
  const prLabel = provider === "gitlab" ? "MR" : "PR";
  const [token, setToken] = useState("");
  const [branchName, setBranchName] = useState("main");
  const [basePath, setBasePath] = useState("");
  const [availableRepos, setAvailableRepos] = useState([]);
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState(null);
  const [testError, setTestError] = useState(null);
  const [saving, setSaving] = useState(false);
  const [disconnecting, setDisconnecting] = useState(false);

  // PR workflow state
  const [prMode, setPrMode] = useState(gitConfig?.pr_mode || "disabled");
  const [prBaseBranch, setPrBaseBranch] = useState(
    gitConfig?.pr_base_branch || "main"
  );
  const [prBranchPrefix, setPrBranchPrefix] = useState(
    gitConfig?.pr_branch_prefix || "visitran/"
  );
  const [branches, setBranches] = useState([]);
  const [branchesLoading, setBranchesLoading] = useState(false);
  const [prSaving, setPrSaving] = useState(false);
  const [prSaveResult, setPrSaveResult] = useState(null);

  const isConfigured = !!gitConfig;

  useEffect(() => {
    if (gitConfig?.pr_mode) {
      setPrMode(gitConfig.pr_mode);
    }
    if (gitConfig?.pr_base_branch) {
      setPrBaseBranch(gitConfig.pr_base_branch);
    }
    if (gitConfig?.pr_branch_prefix) {
      setPrBranchPrefix(gitConfig.pr_branch_prefix);
    }
  }, [
    gitConfig?.pr_mode,
    gitConfig?.pr_base_branch,
    gitConfig?.pr_branch_prefix,
  ]);

  useEffect(() => {
    loadAvailableRepos();
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

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
      const payload = {
        repo_type: "custom",
        repo_url: repoUrl,
        auth_type: authType,
        credentials: { token },
        branch_name: branchName,
      };
      if (basePath) payload.base_path = basePath;
      return payload;
    },
    [repoUrl, authType, token, branchName, basePath]
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
    } catch (error) {
      setTestError(
        error?.response?.data?.error_message ||
          error?.message ||
          "Connection test failed"
      );
    } finally {
      setTesting(false);
    }
  }, [axiosRef, orgId, projectId, csrfToken, selectedMode, buildPayload]);

  const handleSave = useCallback(async () => {
    setSaving(true);
    try {
      const saved = await saveGitConfig(
        axiosRef,
        orgId,
        projectId,
        csrfToken,
        buildPayload(selectedMode)
      );
      setGitConfig(saved);
      notify({ type: "success", message: "Git versioning enabled" });
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
    selectedMode,
    buildPayload,
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
          setBasePath("");
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

  const handlePRModeChange = useCallback(
    async (newMode) => {
      const prevMode = prMode;
      setPrMode(newMode);
      setPrSaveResult(null);
      if (newMode !== "disabled" && branches.length === 0) {
        setBranchesLoading(true);
        try {
          const branchList = await fetchBranches(axiosRef, orgId, projectId);
          setBranches(branchList);
        } catch {
          /* silent */
        } finally {
          setBranchesLoading(false);
        }
      }
      setPrSaving(true);
      try {
        const updated = await updatePRMode(
          axiosRef,
          orgId,
          projectId,
          csrfToken,
          newMode,
          prBaseBranch,
          prBranchPrefix
        );
        setGitConfig(updated);
        const modeLabel =
          newMode === "auto" ? "Auto" : newMode === "manual" ? "Manual" : "Off";
        setPrSaveResult({
          type: "success",
          message:
            newMode === "disabled"
              ? "PR workflow disabled"
              : `${modeLabel} PR workflow enabled`,
        });
      } catch (error) {
        setPrMode(prevMode);
        setPrSaveResult({
          type: "error",
          message:
            error?.response?.data?.error_message ||
            error?.message ||
            "Failed to update PR mode",
        });
      } finally {
        setPrSaving(false);
      }
    },
    [
      axiosRef,
      orgId,
      projectId,
      csrfToken,
      prBaseBranch,
      prBranchPrefix,
      prMode,
      branches.length,
      setGitConfig,
    ]
  );

  const handlePrSettingsSave = useCallback(async () => {
    setPrSaving(true);
    setPrSaveResult(null);
    try {
      const updated = await updatePRMode(
        axiosRef,
        orgId,
        projectId,
        csrfToken,
        prMode,
        prBaseBranch,
        prBranchPrefix
      );
      setGitConfig(updated);
      setPrSaveResult({ type: "success", message: "PR settings saved" });
    } catch (error) {
      setPrSaveResult({
        type: "error",
        message:
          error?.response?.data?.error_message ||
          error?.message ||
          "Failed to save PR settings",
      });
    } finally {
      setPrSaving(false);
    }
  }, [
    axiosRef,
    orgId,
    projectId,
    csrfToken,
    prMode,
    prBaseBranch,
    prBranchPrefix,
    setGitConfig,
  ]);

  const canTest =
    selectedMode === "default" ||
    (selectedMode === "custom" && repoUrl && token);
  const canSave = testResult?.success === true;

  if (isConfigured) {
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
            <Text type="secondary">Branch</Text>
            <Text>{gitConfig.branch_name}</Text>
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
          {gitConfig.base_path && (
            <div className="git-config-info-row">
              <Text type="secondary">Base Path</Text>
              <Text>{gitConfig.base_path}</Text>
            </div>
          )}
          {gitConfig.last_synced_at && (
            <div className="git-config-info-row">
              <Text type="secondary">Last Synced</Text>
              <Text>{new Date(gitConfig.last_synced_at).toLocaleString()}</Text>
            </div>
          )}
        </div>
        <Divider plain>
          {(gitConfig.repo_url || "").includes("gitlab") ? "MR" : "PR"} Workflow
        </Divider>
        <div style={{ marginBottom: 12 }}>
          <Segmented
            block
            size="small"
            value={prMode}
            onChange={handlePRModeChange}
            disabled={prSaving}
            options={(() => {
              const isGL = (gitConfig.repo_url || "").includes("gitlab");
              return [
                { label: "Off", value: "disabled" },
                { label: isGL ? "Auto MR" : "Auto PR", value: "auto" },
                { label: isGL ? "Manual MR" : "Manual PR", value: "manual" },
              ];
            })()}
          />
        </div>
        {prMode === "manual" && (
          <Alert
            type="info"
            message="Commits push to a feature branch. Create PRs manually from the version timeline."
            showIcon
            style={{ marginBottom: 8 }}
          />
        )}
        {prMode !== "disabled" && (
          <div className="git-config-form" style={{ marginTop: 8 }}>
            <div className="git-config-field">
              <Text type="secondary" style={{ fontSize: 12 }}>
                Base Branch
              </Text>
              <Select
                value={prBaseBranch}
                onChange={setPrBaseBranch}
                loading={branchesLoading}
                style={{ width: "100%" }}
                placeholder="Select base branch"
                options={branches.map((b) => ({
                  value: b.name,
                  label: `${b.name}${b.protected ? " (protected)" : ""}`,
                }))}
              />
            </div>
            <div className="git-config-field">
              <Text type="secondary" style={{ fontSize: 12 }}>
                Branch Prefix
              </Text>
              <Input
                value={prBranchPrefix}
                onChange={(e) => setPrBranchPrefix(e.target.value)}
                placeholder="visitran/"
              />
            </div>
            <Button
              type="primary"
              onClick={handlePrSettingsSave}
              loading={prSaving}
              block
              size="small"
            >
              Save Settings
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
        )}
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
              Branch Name
            </Text>
            <Input
              placeholder="main"
              value={branchName}
              onChange={(e) => setBranchName(e.target.value)}
            />
          </div>
          <div className="git-config-field">
            <Text type="secondary" style={{ fontSize: 12 }}>
              Base Path (optional)
            </Text>
            <Input
              placeholder="e.g. packages/models"
              value={basePath}
              onChange={(e) => setBasePath(e.target.value)}
            />
          </div>
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
              loading={saving}
              disabled={!canSave}
              block
            >
              Save Configuration
            </Button>
          </Space>
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
