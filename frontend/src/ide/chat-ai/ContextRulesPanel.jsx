import { memo, useState, useEffect } from "react";
import PropTypes from "prop-types";
import { Tabs, Typography, Button, Spin } from "antd";
import { CloseOutlined, FileDoneOutlined } from "@ant-design/icons";
import Editor from "@monaco-editor/react";

import { useUserStore } from "../../store/user-store";
import { useProjectStore } from "../../store/project-store";
import { useAIContextRulesService } from "../../services/aiContextRulesService";
import { useNotificationService } from "../../service/notification-service";
import "./ContextRulesPanel.css";

const { Text } = Typography;

const ContextRulesPanel = memo(function ContextRulesPanel({
  visible,
  onClose,
}) {
  const currentTheme = useUserStore(
    (state) => state?.userDetails?.currentTheme
  );
  const isDarkTheme = currentTheme === "dark";
  const projectId = useProjectStore((state) => state.projectId);

  const {
    getUserAIContextRules,
    updateUserAIContextRules,
    getProjectAIContextRules,
    updateProjectAIContextRules,
  } = useAIContextRulesService();

  const { notify } = useNotificationService();

  const [activeTab, setActiveTab] = useState("personal");
  const [personalRules, setPersonalRules] = useState("");
  const [projectRules, setProjectRules] = useState("");
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [projectInfo, setProjectInfo] = useState(null);
  const [personalInfo, setPersonalInfo] = useState(null);

  // Character limit constant
  const MAX_CHARACTERS = 4000;

  // Load data when panel becomes visible
  useEffect(() => {
    if (visible) {
      loadContextRules();
    }
  }, [visible, projectId]);

  const loadContextRules = async () => {
    setLoading(true);
    try {
      // Load personal rules
      const personalResponse = await getUserAIContextRules();
      if (personalResponse.success) {
        setPersonalRules(personalResponse.data.context_rules || "");
        setPersonalInfo(personalResponse.data);
      }

      // Load project rules if projectId exists
      if (projectId) {
        const projectResponse = await getProjectAIContextRules(projectId);
        if (projectResponse.success) {
          setProjectRules(projectResponse.data.context_rules || "");
          setProjectInfo(projectResponse.data);
        }
      }
    } catch (error) {
      console.error("Error loading context rules:", error);
      notify({ error });
    } finally {
      setLoading(false);
    }
  };

  const savePersonalRules = async () => {
    setSaving(true);
    try {
      const response = await updateUserAIContextRules(personalRules);
      if (response.success) {
        // Parse the markdown message to extract title and description
        const message =
          response.message || "Personal context rules saved successfully";
        const [title, ...descriptionParts] = message.split("\n");
        const cleanTitle = title.replace(/\*\*/g, ""); // Remove markdown bold formatting
        const description = descriptionParts.join("\n").trim();

        notify({
          type: "success",
          message: cleanTitle,
          description: description || cleanTitle,
        });
        // Update personal info with new update details
        if (response.data) {
          setPersonalInfo(response.data);
        }
      }
    } catch (error) {
      console.error("Error saving personal rules:", error);
      notify({ error });
    } finally {
      setSaving(false);
    }
  };

  const saveProjectRules = async () => {
    if (!projectId) {
      notify({
        type: "error",
        message: "Error",
        description: "No project selected",
      });
      return;
    }

    setSaving(true);
    try {
      const response = await updateProjectAIContextRules(
        projectId,
        projectRules
      );
      if (response.success) {
        // Parse the markdown message to extract title and description
        const message =
          response.message || "Project context rules saved successfully";
        const [title, ...descriptionParts] = message.split("\n");
        const cleanTitle = title.replace(/\*\*/g, ""); // Remove markdown bold formatting
        const description = descriptionParts.join("\n").trim();

        notify({
          type: "success",
          message: cleanTitle,
          description: description || cleanTitle,
        });
        // Update project info with new update details
        setProjectInfo((prev) => ({
          ...prev,
          updated_by: response.data.updated_by,
          updated_at: response.data.updated_at,
        }));
      }
    } catch (error) {
      console.error("Error saving project rules:", error);
      notify({ error });
    } finally {
      setSaving(false);
    }
  };

  const clearPersonalRules = () => {
    setPersonalRules("");
  };

  const clearProjectRules = () => {
    setProjectRules("");
  };

  const projectSpecificContent = (
    <div
      style={{
        padding: "16px 20px",
        height: "100%",
        display: "flex",
        flexDirection: "column",
      }}
    >
      <div style={{ marginBottom: "16px" }}>
        <div
          style={{
            display: "flex",
            alignItems: "center",
            marginBottom: "16px",
          }}
        >
          <span style={{ fontSize: "16px", marginRight: "8px" }}>🎯</span>
          <Text
            strong
            style={{
              color: isDarkTheme ? "#1890ff" : "#1890ff",
              fontSize: "16px",
            }}
          >
            Project Context Rules
          </Text>
        </div>

        <div style={{ marginLeft: "0px" }}>
          <Text
            style={{
              color: isDarkTheme ? "#9ca3af" : "#6b7280",
              fontSize: "14px",
              lineHeight: "1.5",
            }}
          >
            Set shared preferences that guide how Visitran AI responds within
            this project. These rules are visible to all project members and
            help ensure consistent outputs across the team. They provide
            guidance but cannot override system or safety policies.
          </Text>
        </div>
      </div>

      <div style={{ flex: 1, display: "flex", flexDirection: "column" }}>
        <div
          style={{
            border: `1px solid ${isDarkTheme ? "#374151" : "#d1d5db"}`,
            borderRadius: "4px",
            overflow: "hidden",
            flex: 1,
            position: "relative",
          }}
        >
          {!projectRules && (
            <div
              style={{
                position: "absolute",
                top: 12,
                left: 12,
                color: isDarkTheme ? "#6b7280" : "#9ca3af",
                fontSize: 14,
                fontFamily: "'Fira Code', 'Monaco', 'Menlo', monospace",
                pointerEvents: "none",
                zIndex: 1,
              }}
            >
              {projectId
                ? "Enter project-specific context rules shared with team members..."
                : "No project selected"}
            </div>
          )}
          <Editor
            height="400px"
            defaultLanguage="markdown"
            value={projectRules}
            onChange={(value) => {
              if ((value || "").length <= MAX_CHARACTERS) {
                setProjectRules(value || "");
              }
            }}
            theme={isDarkTheme ? "vs-dark" : "vs-light"}
            options={{
              readOnly: loading || !projectId,
              minimap: { enabled: false },
              scrollBeyondLastLine: false,
              automaticLayout: true,
              wordWrap: "on",
              wrappingIndent: "same",
              lineNumbers: "off",
              renderLineHighlight: "none",
              fontSize: 14,
              fontFamily: "'Fira Code', 'Monaco', 'Menlo', monospace",
              padding: { top: 12, bottom: 12 },
              scrollbar: {
                horizontal: "hidden",
                vertical: "auto",
              },
              overviewRulerLanes: 0,
            }}
          />
        </div>

        {/* Character counter for project rules */}
        <div className="character-counter">
          <Text
            style={{
              fontSize: "12px",
              color:
                projectRules.length >= MAX_CHARACTERS
                  ? isDarkTheme
                    ? "#ef4444"
                    : "#dc2626"
                  : isDarkTheme
                  ? "#9ca3af"
                  : "#6b7280",
            }}
          >
            {projectRules.length}/{MAX_CHARACTERS} characters
          </Text>
        </div>

        {/* Project info */}
        {projectInfo && projectInfo.updated_by && (
          <div
            style={{
              marginTop: "12px",
              fontSize: "12px",
              color: isDarkTheme ? "#9ca3af" : "#6b7280",
            }}
          >
            Last updated by{" "}
            {projectInfo.updated_by.full_name ||
              projectInfo.updated_by.username}{" "}
            on {new Date(projectInfo.updated_at).toLocaleString()}
          </div>
        )}
      </div>
    </div>
  );

  const personalRulesContent = (
    <div
      style={{
        padding: "16px 20px",
        height: "100%",
        display: "flex",
        flexDirection: "column",
      }}
    >
      <div style={{ marginBottom: "16px" }}>
        <div
          style={{
            display: "flex",
            alignItems: "center",
            marginBottom: "16px",
          }}
        >
          <span style={{ fontSize: "16px", marginRight: "8px" }}>👤</span>
          <Text
            strong
            style={{
              color: isDarkTheme ? "#1890ff" : "#1890ff",
              fontSize: "16px",
            }}
          >
            Personal Context Rules
          </Text>
        </div>

        <div style={{ marginLeft: "0px" }}>
          <Text
            style={{
              color: isDarkTheme ? "#9ca3af" : "#6b7280",
              fontSize: "14px",
              lineHeight: "1.5",
            }}
          >
            Define your own preferences for how Visitran AI should respond.
            These rules are visible only to you and apply across all projects.
            They can shape tone, style, or formatting, but will never override
            core system or safety policies.
          </Text>
        </div>
      </div>

      <div style={{ flex: 1, display: "flex", flexDirection: "column" }}>
        <div
          style={{
            border: `1px solid ${isDarkTheme ? "#374151" : "#d1d5db"}`,
            borderRadius: "4px",
            overflow: "hidden",
            flex: 1,
            position: "relative",
          }}
        >
          {!personalRules && (
            <div
              style={{
                position: "absolute",
                top: 12,
                left: 12,
                color: isDarkTheme ? "#6b7280" : "#9ca3af",
                fontSize: 14,
                fontFamily: "'Fira Code', 'Monaco', 'Menlo', monospace",
                pointerEvents: "none",
                zIndex: 1,
              }}
            >
              Enter your personal context rules that will follow you across all
              projects...
            </div>
          )}
          <Editor
            height="400px"
            defaultLanguage="markdown"
            value={personalRules}
            onChange={(value) => {
              if ((value || "").length <= MAX_CHARACTERS) {
                setPersonalRules(value || "");
              }
            }}
            theme={isDarkTheme ? "vs-dark" : "vs-light"}
            options={{
              readOnly: loading,
              minimap: { enabled: false },
              scrollBeyondLastLine: false,
              automaticLayout: true,
              wordWrap: "on",
              wrappingIndent: "same",
              lineNumbers: "off",
              renderLineHighlight: "none",
              fontSize: 14,
              fontFamily: "'Fira Code', 'Monaco', 'Menlo', monospace",
              padding: { top: 12, bottom: 12 },
              scrollbar: {
                horizontal: "hidden",
                vertical: "auto",
              },
              overviewRulerLanes: 0,
            }}
          />
        </div>

        {/* Character counter for personal rules */}
        <div className="character-counter">
          <Text
            style={{
              fontSize: "12px",
              color:
                personalRules.length >= MAX_CHARACTERS
                  ? isDarkTheme
                    ? "#ef4444"
                    : "#dc2626"
                  : isDarkTheme
                  ? "#9ca3af"
                  : "#6b7280",
            }}
          >
            {personalRules.length}/{MAX_CHARACTERS} characters
          </Text>
        </div>

        {/* Personal info */}
        {personalInfo && personalInfo.updated_at && (
          <div
            style={{
              marginTop: "12px",
              fontSize: "12px",
              color: isDarkTheme ? "#9ca3af" : "#6b7280",
            }}
          >
            Last updated on {new Date(personalInfo.updated_at).toLocaleString()}
          </div>
        )}
      </div>
    </div>
  );

  const tabItems = [
    {
      key: "personal",
      label: "Personal",
      children: personalRulesContent,
    },
    {
      key: "project",
      label: "Project",
      children: projectSpecificContent,
    },
    // Commented out Global tab for now
    // {
    //   key: "global",
    //   label: "Global",
    //   children: globalRulesContent,
    // },
  ];

  return (
    <>
      {/* Backdrop overlay */}
      <div
        className={`context-rules-backdrop ${visible ? "visible" : ""}`}
        onClick={onClose}
      />

      {/* Sliding panel */}
      <div
        className={`context-rules-panel ${visible ? "visible" : ""} ${
          isDarkTheme ? "dark" : "light"
        }`}
      >
        {/* Panel header */}
        <div className="context-rules-header">
          <div style={{ display: "flex", alignItems: "center" }}>
            <FileDoneOutlined
              style={{
                fontSize: "18px",
                marginRight: "12px",
                color: isDarkTheme ? "#1890ff" : "#1890ff",
              }}
            />
            <Typography.Text
              strong
              style={{ color: isDarkTheme ? "#ffffff" : "#000000" }}
            >
              Context & Rules Manager
            </Typography.Text>
          </div>
          <Button
            type="text"
            size="small"
            icon={<CloseOutlined />}
            onClick={onClose}
            className="close-button"
            style={{
              color: isDarkTheme ? "#ffffff" : "#000000",
            }}
          />
        </div>

        {/* Panel content */}
        <div
          className="context-rules-content"
          style={{ flex: 1, overflow: "auto" }}
        >
          {loading ? (
            <div style={{ textAlign: "center", padding: "40px" }}>
              <Spin size="large" />
              <div
                style={{
                  marginTop: "16px",
                  color: isDarkTheme ? "#9ca3af" : "#6b7280",
                }}
              >
                Loading context rules...
              </div>
            </div>
          ) : (
            <Tabs
              activeKey={activeTab}
              onChange={setActiveTab}
              items={tabItems}
              centered
              style={{
                color: isDarkTheme ? "#ffffff" : "#000000",
                height: "100%",
              }}
            />
          )}
        </div>

        {/* Panel footer */}
        <div
          className="context-rules-footer"
          style={{
            padding: "16px 20px",
            borderTop: `1px solid ${isDarkTheme ? "#374151" : "#e5e7eb"}`,
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
          }}
        >
          <Text
            style={{
              color: isDarkTheme ? "#9ca3af" : "#6b7280",
              fontSize: "14px",
            }}
          >
            Rules will apply to all future conversations
          </Text>

          <div style={{ display: "flex", gap: "12px" }}>
            <Button
              onClick={
                activeTab === "personal"
                  ? clearPersonalRules
                  : clearProjectRules
              }
              disabled={
                loading || saving || (activeTab === "project" && !projectId)
              }
            >
              Clear
            </Button>
            <Button
              type="primary"
              onClick={
                activeTab === "personal" ? savePersonalRules : saveProjectRules
              }
              loading={saving}
              disabled={loading || (activeTab === "project" && !projectId)}
              style={{
                backgroundColor: isDarkTheme ? "#1890ff" : "#1890ff",
                borderColor: isDarkTheme ? "#1890ff" : "#1890ff",
              }}
            >
              Save
            </Button>
          </div>
        </div>
      </div>
    </>
  );
});

ContextRulesPanel.propTypes = {
  visible: PropTypes.bool.isRequired,
  onClose: PropTypes.func.isRequired,
};

ContextRulesPanel.displayName = "ContextRulesPanel";

export { ContextRulesPanel };
