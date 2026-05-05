import { memo, useCallback, useEffect, useMemo } from "react";
import { Space, Typography, Select, Switch } from "antd";
import { DatabaseOutlined, WalletOutlined } from "@ant-design/icons";
import PropTypes from "prop-types";

import CircularTokenDisplay from "./CircularTokenDisplay";
import InfoChip from "./InfoChip";
import { useTokenStore } from "../../store/token-store";
import { useSessionStore } from "../../store/session-store";
import { useProjectStore } from "../../store/project-store";
import { explorerService } from "../explorer/explorer-service";
import { useNotificationService } from "../../service/notification-service";

const HIDE_EDITOR_SELECTOR = true;

const PromptActions = memo(function PromptActions({
  useMonaco,
  onUseMonacoSwitch,
  llmModels = [],
  selectedLlmModel,
  setSelectedLlmModel,
  isOnboardingMode = false,
  isTypingPrompt = false,
  onBuyTokens,
}) {
  // Get token balance from store
  const { tokenBalance, isLoading: isTokenLoading } = useTokenStore();
  const isCloud = useSessionStore((state) => state.sessionDetails?.is_cloud);
  const currentSchema = useProjectStore((state) => state.currentSchema);
  const setCurrentSchema = useProjectStore((state) => state.setCurrentSchema);
  const schemaList = useProjectStore((state) => state.schemaList);
  const projectId = useProjectStore((state) => state.projectId);
  const expService = explorerService();
  const { notify } = useNotificationService();

  const schemaOptions = useMemo(
    () => schemaList.map((s) => ({ label: s, value: s })),
    [schemaList]
  );

  const handleSchemaChange = useCallback(
    (value) => {
      expService
        .setProjectSchema(projectId, value)
        .then(() => {
          setCurrentSchema(value);
          notify({ type: "success", message: "Schema updated successfully" });
        })
        .catch((error) => {
          console.error(error);
          notify({ error });
        });
    },
    [expService, projectId, setCurrentSchema, notify]
  );

  const llmOptions = useMemo(
    () =>
      llmModels.map((m) => ({
        label: m.display_name,
        value: m.model,
      })),
    [llmModels]
  );

  // If there's no selected LLM, pick the default
  useEffect(() => {
    if (selectedLlmModel || !llmModels.length) return;
    const defaultModel = llmModels.find((m) => m.default);
    if (defaultModel?.model) {
      setSelectedLlmModel(defaultModel.model);
    }
  }, [llmModels, selectedLlmModel]);

  return (
    <div className="chat-ai-prompt-actions-container">
      <Space>
        <Space size={0}>
          <Typography.Text type="secondary" className="font-size-12">
            Model:
          </Typography.Text>
          <Select
            showSearch
            size="small"
            placeholder="LLM model"
            optionFilterProp="label"
            options={llmOptions}
            value={selectedLlmModel}
            onChange={setSelectedLlmModel}
            variant="borderless"
            dropdownClassName="small-font-dropdown"
            className="chat-ai-prompt-actions-model-select"
          />
        </Space>
      </Space>

      <Space>
        {/* Cloud: full credit display | OSS: link to billing page */}
        {isCloud ? (
          <CircularTokenDisplay
            tokenData={tokenBalance}
            onBuyTokens={onBuyTokens}
            isLoading={isTokenLoading}
          />
        ) : (
          <a
            href="https://us.app.visitran.com/project/setting/subscriptions"
            target="_blank"
            rel="noopener noreferrer"
            className="chat-ai-manage-credits-link"
          >
            <WalletOutlined />
            <span>Manage Credits</span>
          </a>
        )}

        {/* Schema selector */}
        {schemaList.length > 0 ? (
          <div className="chat-ai-info-chip chat-ai-info-chip-clickable">
            <DatabaseOutlined className="chat-ai-info-chip-icon" />
            <Select
              size="small"
              variant="borderless"
              showSearch
              placeholder="Schema"
              value={currentSchema || undefined}
              onChange={handleSchemaChange}
              options={schemaOptions}
              popupMatchSelectWidth={false}
              className="chat-ai-schema-select"
            />
          </div>
        ) : (
          <InfoChip
            icon={<DatabaseOutlined className="chat-ai-info-chip-icon" />}
            text="No schema"
            tooltipTitle="No schemas available. Please configure a database connection and select a schema from the explorer."
            className="chat-ai-info-chip-error"
          />
        )}
      </Space>

      {!HIDE_EDITOR_SELECTOR && (
        <div>
          <Space size={5}>
            <Switch
              size="small"
              checked={useMonaco}
              onChange={onUseMonacoSwitch}
              disabled={isOnboardingMode && isTypingPrompt}
            />
            <Typography.Text
              className="chat-ai-prompt-actions-monaco-font-size-10"
              type="secondary"
              style={{
                opacity: isOnboardingMode && isTypingPrompt ? 0.5 : 1,
              }}
            >
              Use Editor
            </Typography.Text>
          </Space>
        </div>
      )}
    </div>
  );
});

PromptActions.propTypes = {
  useMonaco: PropTypes.bool.isRequired,
  onUseMonacoSwitch: PropTypes.func.isRequired,
  llmModels: PropTypes.array,
  selectedLlmModel: PropTypes.string,
  setSelectedLlmModel: PropTypes.func.isRequired,
  isOnboardingMode: PropTypes.bool,
  isTypingPrompt: PropTypes.bool,
  onBuyTokens: PropTypes.func,
};

PromptActions.displayName = "PromptActions";

export { PromptActions };
