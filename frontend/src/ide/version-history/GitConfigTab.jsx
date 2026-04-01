import { useState, useEffect, useCallback, memo } from "react";
import PropTypes from "prop-types";
import { Alert, Button, Card, Divider, Input, Modal, Select, Space, Tag, Typography } from "antd";
import { CheckCircleOutlined, CloseCircleOutlined, CloudServerOutlined, DisconnectOutlined, ExclamationCircleOutlined, GithubOutlined, LinkOutlined, SafetyCertificateOutlined } from "@ant-design/icons";
import Cookies from "js-cookie";

import { useAxiosPrivate } from "../../service/axios-service";
import { useNotificationService } from "../../service/notification-service";
import { orgStore } from "../../store/org-store";
import { useVersionHistoryStore } from "../../store/version-history-store";
import { deleteGitConfig, fetchAvailableRepos, saveGitConfig, testGitConnection } from "./services";

const { Text, Paragraph } = Typography;
const AUTH_TYPE_OPTIONS = [{ value: "pat", label: "Personal Access Token" }, { value: "ssh", label: "SSH Key" }];

const GitConfigTab = memo(function GitConfigTab({ projectId, gitConfig, onConfigSaved }) {
  const axiosRef = useAxiosPrivate();
  const { notify } = useNotificationService();
  const orgId = orgStore.getState().selectedOrgId;
  const csrfToken = Cookies.get("csrftoken");
  const setGitConfig = useVersionHistoryStore((s) => s.setGitConfig);

  const [selectedMode, setSelectedMode] = useState(null);
  const [repoUrl, setRepoUrl] = useState("");
  const [authType, setAuthType] = useState("pat");
  const [token, setToken] = useState("");
  const [branchName, setBranchName] = useState("main");
  const [basePath, setBasePath] = useState("");
  const [availableRepos, setAvailableRepos] = useState([]);
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState(null);
  const [testError, setTestError] = useState(null);
  const [saving, setSaving] = useState(false);
  const [disconnecting, setDisconnecting] = useState(false);
  const isConfigured = !!gitConfig;

  useEffect(() => { loadAvailableRepos(); }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const loadAvailableRepos = useCallback(async () => {
    try { const repos = await fetchAvailableRepos(axiosRef, orgId, projectId); setAvailableRepos(repos || []); } catch { /* silent */ }
  }, [axiosRef, orgId, projectId]);

  const handleSelectAvailableRepo = useCallback((repoUrlValue) => {
    const repo = availableRepos.find((r) => r.repo_url === repoUrlValue);
    if (repo) { setSelectedMode("custom"); setRepoUrl(repo.repo_url); setAuthType(repo.auth_type); setBranchName(repo.branch_name); }
  }, [availableRepos]);

  const buildPayload = useCallback((mode) => {
    if (mode === "default") return { repo_type: "default" };
    const payload = { repo_type: "custom", repo_url: repoUrl, auth_type: authType, credentials: { token }, branch_name: branchName };
    if (basePath) payload.base_path = basePath;
    return payload;
  }, [repoUrl, authType, token, branchName, basePath]);

  const handleTestConnection = useCallback(async () => {
    setTesting(true); setTestResult(null); setTestError(null);
    try { const result = await testGitConnection(axiosRef, orgId, projectId, csrfToken, buildPayload(selectedMode)); setTestResult(result); }
    catch (error) { setTestError(error?.response?.data?.error_message || error?.message || "Connection test failed"); }
    finally { setTesting(false); }
  }, [axiosRef, orgId, projectId, csrfToken, selectedMode, buildPayload]);

  const handleSave = useCallback(async () => {
    setSaving(true);
    try { const saved = await saveGitConfig(axiosRef, orgId, projectId, csrfToken, buildPayload(selectedMode)); setGitConfig(saved); notify({ type: "success", message: "Git versioning enabled" }); onConfigSaved?.(saved); }
    catch (error) { notify({ error }); }
    finally { setSaving(false); }
  }, [axiosRef, orgId, projectId, csrfToken, selectedMode, buildPayload, setGitConfig, notify, onConfigSaved]);

  const handleDisconnect = useCallback(() => {
    Modal.confirm({
      title: "Disconnect Git Repository?", icon: <ExclamationCircleOutlined />,
      content: "This will disable version control for this project. Existing versions will be preserved.",
      okText: "Disconnect", okType: "danger",
      onOk: async () => {
        setDisconnecting(true);
        try { await deleteGitConfig(axiosRef, orgId, projectId, csrfToken); setGitConfig(null); notify({ type: "success", message: "Git configuration removed" }); setSelectedMode(null); setTestResult(null); setTestError(null); setRepoUrl(""); setToken(""); setBranchName("main"); setBasePath(""); }
        catch (error) { notify({ error }); }
        finally { setDisconnecting(false); }
      },
    });
  }, [axiosRef, orgId, projectId, csrfToken, setGitConfig, notify]);

  const handleDefaultEnable = useCallback(async () => {
    setSelectedMode("default"); setTesting(true); setTestResult(null); setTestError(null);
    try {
      const payload = { repo_type: "default" };
      const result = await testGitConnection(axiosRef, orgId, projectId, csrfToken, payload);
      setTestResult(result);
      setSaving(true);
      const saved = await saveGitConfig(axiosRef, orgId, projectId, csrfToken, payload);
      setGitConfig(saved); notify({ type: "success", message: "Git versioning enabled" }); onConfigSaved?.(saved);
    } catch (error) { setTestError(error?.response?.data?.error_message || error?.message || "Failed to enable default versioning"); }
    finally { setTesting(false); setSaving(false); }
  }, [axiosRef, orgId, projectId, csrfToken, setGitConfig, notify, onConfigSaved]);

  const canTest = selectedMode === "default" || (selectedMode === "custom" && repoUrl && token);
  const canSave = testResult?.success === true;

  if (isConfigured) {
    return (
      <div className="git-config-tab">
        <Alert message="Git versioning is active" type="success" showIcon icon={<CheckCircleOutlined />} style={{ marginBottom: 16 }} />
        <div className="git-config-info">
          <div className="git-config-info-row"><Text type="secondary">Repository</Text><Text><LinkOutlined /> {gitConfig.repo_url}</Text></div>
          <div className="git-config-info-row"><Text type="secondary">Type</Text><Tag color={gitConfig.repo_type === "default" ? "blue" : "green"}>{gitConfig.repo_type}</Tag></div>
          <div className="git-config-info-row"><Text type="secondary">Branch</Text><Text>{gitConfig.branch_name}</Text></div>
          <div className="git-config-info-row"><Text type="secondary">Status</Text><Tag color={gitConfig.connection_status === "connected" ? "success" : gitConfig.connection_status === "error" ? "error" : "warning"}>{gitConfig.connection_status}</Tag></div>
          {gitConfig.base_path && <div className="git-config-info-row"><Text type="secondary">Base Path</Text><Text>{gitConfig.base_path}</Text></div>}
          {gitConfig.last_synced_at && <div className="git-config-info-row"><Text type="secondary">Last Synced</Text><Text>{new Date(gitConfig.last_synced_at).toLocaleString()}</Text></div>}
        </div>
        <Divider />
        <Button danger icon={<DisconnectOutlined />} onClick={handleDisconnect} loading={disconnecting} block>Disconnect Repository</Button>
      </div>
    );
  }

  return (
    <div className="git-config-tab">
      <Paragraph type="secondary" style={{ marginBottom: 16 }}>Enable version control to track changes to your transformation models with git-backed versioning.</Paragraph>
      {availableRepos.length > 0 && (
        <div style={{ marginBottom: 16 }}>
          <Text type="secondary" style={{ fontSize: 12, display: "block", marginBottom: 4 }}>Use an existing repository from your organization</Text>
          <Select placeholder="Select a previously configured repo..." style={{ width: "100%" }} onChange={handleSelectAvailableRepo} options={availableRepos.map((r) => ({ value: r.repo_url, label: `${r.repo_url} (${r.branch_name})` }))} allowClear />
          <Divider plain style={{ margin: "12px 0" }}>or configure new</Divider>
        </div>
      )}
      <div className="git-config-cards">
        <Card size="small" hoverable className={`git-config-card ${selectedMode === "default" ? "git-config-card-selected" : ""}`} onClick={() => !testing && !saving && setSelectedMode("default")}><Space><CloudServerOutlined style={{ fontSize: 20 }} /><div><Text strong>Default Repo</Text><br /><Text type="secondary" style={{ fontSize: 12 }}>Managed by Visitran</Text></div></Space></Card>
        <Card size="small" hoverable className={`git-config-card ${selectedMode === "custom" ? "git-config-card-selected" : ""}`} onClick={() => !testing && !saving && setSelectedMode("custom")}><Space><GithubOutlined style={{ fontSize: 20 }} /><div><Text strong>Custom Repo</Text><br /><Text type="secondary" style={{ fontSize: 12 }}>Your own GitHub repo</Text></div></Space></Card>
      </div>
      {selectedMode === "default" && (
        <div style={{ marginTop: 16 }}>
          <Alert message="Visitran will manage a git repository for you" description="Your YAML versions will be stored in a secure, Visitran-managed repository." type="info" showIcon style={{ marginBottom: 12 }} />
          <Button type="primary" icon={<SafetyCertificateOutlined />} onClick={handleDefaultEnable} loading={testing || saving} block>Enable Default Versioning</Button>
        </div>
      )}
      {selectedMode === "custom" && (
        <div className="git-config-form">
          <div className="git-config-field"><Text type="secondary" style={{ fontSize: 12 }}>Repository URL *</Text><Input placeholder="https://github.com/owner/repo" value={repoUrl} onChange={(e) => setRepoUrl(e.target.value)} /></div>
          <div className="git-config-field"><Text type="secondary" style={{ fontSize: 12 }}>Authentication *</Text><Select value={authType} onChange={setAuthType} options={AUTH_TYPE_OPTIONS} style={{ width: "100%" }} /></div>
          <div className="git-config-field"><Text type="secondary" style={{ fontSize: 12 }}>{authType === "pat" ? "Personal Access Token *" : "SSH Key *"}</Text>{authType === "pat" ? <Input.Password placeholder="ghp_xxxxxxxxxxxx" value={token} onChange={(e) => setToken(e.target.value)} /> : <Input.TextArea placeholder="Paste your SSH private key..." value={token} onChange={(e) => setToken(e.target.value)} rows={3} />}</div>
          <div className="git-config-field"><Text type="secondary" style={{ fontSize: 12 }}>Branch Name</Text><Input placeholder="main" value={branchName} onChange={(e) => setBranchName(e.target.value)} /></div>
          <div className="git-config-field"><Text type="secondary" style={{ fontSize: 12 }}>Base Path (optional)</Text><Input placeholder="e.g. packages/models" value={basePath} onChange={(e) => setBasePath(e.target.value)} /></div>
          <Space direction="vertical" style={{ width: "100%", marginTop: 12 }}>
            <Button icon={<LinkOutlined />} onClick={handleTestConnection} loading={testing} disabled={!canTest} block>Test Connection</Button>
            <Button type="primary" onClick={handleSave} loading={saving} disabled={!canSave} block>Save Configuration</Button>
          </Space>
        </div>
      )}
      {testResult && <Alert message="Connection successful" description={<div><Text>{testResult.repo_info?.full_name} ({testResult.repo_info?.default_branch})</Text><br /><Text type="secondary">{testResult.repo_info?.private ? "Private" : "Public"} repository</Text></div>} type="success" showIcon icon={<CheckCircleOutlined />} style={{ marginTop: 12 }} closable onClose={() => setTestResult(null)} />}
      {testError && <Alert message="Connection failed" description={testError} type="error" showIcon icon={<CloseCircleOutlined />} style={{ marginTop: 12 }} closable onClose={() => setTestError(null)} />}
    </div>
  );
});

GitConfigTab.propTypes = { projectId: PropTypes.string.isRequired, gitConfig: PropTypes.object, onConfigSaved: PropTypes.func };
GitConfigTab.displayName = "GitConfigTab";

export { GitConfigTab };
