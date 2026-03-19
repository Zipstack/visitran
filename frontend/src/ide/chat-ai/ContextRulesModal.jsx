import { memo, useState } from "react";
import PropTypes from "prop-types";
import { Modal, Tabs, Typography, Button, Input, Space } from "antd";
import { CloseOutlined } from "@ant-design/icons";

import { useUserStore } from "../../store/user-store";

const { TextArea } = Input;
const { Text } = Typography;

const ContextRulesModal = memo(function ContextRulesModal({
  visible,
  onClose,
}) {
  const currentTheme = useUserStore(
    (state) => state?.userDetails?.currentTheme
  );
  const isDarkTheme = currentTheme === "dark";

  const [activeTab, setActiveTab] = useState("global");
  const [globalRules, setGlobalRules] =
    useState(`Enter your persistent context rules here...

Example rules:
- Database: Always use PostgreSQL syntax
- Style: Prefer readable code with comments
- Performance: Consider query optimization
- Format: Include proper indentation and structure
- Business context: We work with e-commerce data

These rules will be automatically applied to every AI conversation, so you don't need to repeat them.`);

  const [projectRules, setProjectRules] =
    useState(`Enter project-specific context rules...

Example for current project:
- Database: Jaffle Shop PostgreSQL database
- Tables: customers, orders, payments
- Focus: E-commerce analytics and reporting
- Style: Business-friendly SQL with clear naming`);

  const globalRulesContent = (
    <div style={{ padding: "16px 0" }}>
      <div style={{ marginBottom: "24px" }}>
        <div
          style={{
            display: "flex",
            alignItems: "center",
            marginBottom: "16px",
          }}
        >
          <span style={{ fontSize: "16px", marginRight: "8px" }}>💡</span>
          <Text strong style={{ color: "#7c3aed", fontSize: "16px" }}>
            Example Context Rules
          </Text>
        </div>

        <div style={{ marginLeft: "0px" }}>
          <div style={{ marginBottom: "8px" }}>
            <Text style={{ color: isDarkTheme ? "#9ca3af" : "#6b7280" }}>
              • Always use PostgreSQL syntax for SQL generation
            </Text>
          </div>
          <div style={{ marginBottom: "8px" }}>
            <Text style={{ color: isDarkTheme ? "#9ca3af" : "#6b7280" }}>
              • Prefer CTEs over subqueries for readability
            </Text>
          </div>
          <div style={{ marginBottom: "8px" }}>
            <Text style={{ color: isDarkTheme ? "#9ca3af" : "#6b7280" }}>
              • Include detailed comments for complex transformations
            </Text>
          </div>
          <div style={{ marginBottom: "8px" }}>
            <Text style={{ color: isDarkTheme ? "#9ca3af" : "#6b7280" }}>
              • Consider performance implications in recommendations
            </Text>
          </div>
          <div style={{ marginBottom: "8px" }}>
            <Text style={{ color: isDarkTheme ? "#9ca3af" : "#6b7280" }}>
              • Format output as clean, production-ready code
            </Text>
          </div>
        </div>
      </div>

      <TextArea
        value={globalRules}
        onChange={(e) => setGlobalRules(e.target.value)}
        placeholder="Enter your persistent context rules here..."
        rows={12}
        style={{
          backgroundColor: isDarkTheme ? "#1f2937" : "#f9fafb",
          border: `1px solid ${isDarkTheme ? "#374151" : "#d1d5db"}`,
          borderRadius: "8px",
          color: isDarkTheme ? "#9ca3af" : "#374151",
          fontSize: "14px",
          fontFamily:
            "Monaco, 'Cascadia Code', 'Roboto Mono', Consolas, 'Courier New', monospace",
          resize: "none",
        }}
      />
    </div>
  );

  const projectSpecificContent = (
    <div style={{ padding: "16px 0" }}>
      <div style={{ marginBottom: "24px" }}>
        <div
          style={{
            display: "flex",
            alignItems: "center",
            marginBottom: "16px",
          }}
        >
          <span style={{ fontSize: "16px", marginRight: "8px" }}>💡</span>
          <Text strong style={{ color: "#7c3aed", fontSize: "16px" }}>
            Example Context Rules
          </Text>
        </div>

        <div style={{ marginLeft: "0px" }}>
          <div style={{ marginBottom: "8px" }}>
            <Text style={{ color: isDarkTheme ? "#9ca3af" : "#6b7280" }}>
              • Always use PostgreSQL syntax for SQL generation
            </Text>
          </div>
          <div style={{ marginBottom: "8px" }}>
            <Text style={{ color: isDarkTheme ? "#9ca3af" : "#6b7280" }}>
              • Prefer CTEs over subqueries for readability
            </Text>
          </div>
          <div style={{ marginBottom: "8px" }}>
            <Text style={{ color: isDarkTheme ? "#9ca3af" : "#6b7280" }}>
              • Include detailed comments for complex transformations
            </Text>
          </div>
          <div style={{ marginBottom: "8px" }}>
            <Text style={{ color: isDarkTheme ? "#9ca3af" : "#6b7280" }}>
              • Consider performance implications in recommendations
            </Text>
          </div>
          <div style={{ marginBottom: "8px" }}>
            <Text style={{ color: isDarkTheme ? "#9ca3af" : "#6b7280" }}>
              • Format output as clean, production-ready code
            </Text>
          </div>
        </div>
      </div>

      <TextArea
        value={projectRules}
        onChange={(e) => setProjectRules(e.target.value)}
        placeholder="Enter project-specific context rules..."
        rows={12}
        style={{
          backgroundColor: isDarkTheme ? "#1f2937" : "#f9fafb",
          border: `1px solid ${isDarkTheme ? "#374151" : "#d1d5db"}`,
          borderRadius: "8px",
          color: isDarkTheme ? "#9ca3af" : "#374151",
          fontSize: "14px",
          fontFamily:
            "Monaco, 'Cascadia Code', 'Roboto Mono', Consolas, 'Courier New', monospace",
          resize: "none",
        }}
      />
    </div>
  );

  const tabItems = [
    {
      key: "global",
      label: "Global Rules",
      children: globalRulesContent,
    },
    {
      key: "project",
      label: "Project Specific",
      children: projectSpecificContent,
    },
  ];

  const handleClearAll = () => {
    if (activeTab === "global") {
      setGlobalRules("");
    } else {
      setProjectRules("");
    }
  };

  const handleSaveRules = () => {
    // TODO: Implement save functionality
    onClose();
  };

  return (
    <Modal
      title={
        <div style={{ display: "flex", alignItems: "center" }}>
          <span style={{ fontSize: "18px", marginRight: "8px" }}>📋</span>
          <Text strong style={{ fontSize: "18px" }}>
            Context & Rules Manager
          </Text>
        </div>
      }
      open={visible}
      onCancel={onClose}
      width={800}
      centered
      footer={null}
      closeIcon={<CloseOutlined />}
      styles={{
        body: {
          padding: "0",
          backgroundColor: isDarkTheme ? "#111827" : "#ffffff",
        },
        header: {
          borderBottom: `1px solid ${isDarkTheme ? "#374151" : "#e5e7eb"}`,
          padding: "16px 24px",
          backgroundColor: isDarkTheme ? "#111827" : "#ffffff",
        },
        content: {
          backgroundColor: isDarkTheme ? "#111827" : "#ffffff",
        },
      }}
    >
      <div
        style={{
          minHeight: "500px",
          backgroundColor: isDarkTheme ? "#111827" : "#ffffff",
        }}
      >
        <Tabs
          activeKey={activeTab}
          onChange={setActiveTab}
          items={tabItems}
          centered
          size="large"
          style={{
            padding: "0 24px",
            backgroundColor: isDarkTheme ? "#111827" : "#ffffff",
          }}
          tabBarStyle={{
            marginBottom: "0",
            borderBottom: `1px solid ${isDarkTheme ? "#374151" : "#e5e7eb"}`,
            backgroundColor: isDarkTheme ? "#111827" : "#ffffff",
          }}
        />

        <div
          style={{
            padding: "24px",
            backgroundColor: isDarkTheme ? "#111827" : "#ffffff",
          }}
        >
          <div
            style={{
              borderTop: `1px solid ${isDarkTheme ? "#374151" : "#e5e7eb"}`,
              paddingTop: "16px",
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

            <Space>
              <Button
                onClick={handleClearAll}
                style={{
                  backgroundColor: "#6b7280",
                  borderColor: "#6b7280",
                  color: "white",
                }}
              >
                Clear All
              </Button>
              <Button
                type="primary"
                onClick={handleSaveRules}
                style={{
                  backgroundColor: "#7c3aed",
                  borderColor: "#7c3aed",
                }}
              >
                Save Context Rules
              </Button>
            </Space>
          </div>
        </div>
      </div>
    </Modal>
  );
});

ContextRulesModal.propTypes = {
  visible: PropTypes.bool.isRequired,
  onClose: PropTypes.func.isRequired,
};

ContextRulesModal.displayName = "ContextRulesModal";

export { ContextRulesModal };
