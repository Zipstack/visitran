import { memo, useEffect, useMemo } from "react";
import { Space, Typography, Select, Switch, Segmented, Tooltip } from "antd";
import {
  ConsoleSqlOutlined,
  MessageOutlined,
  RetweetOutlined,
  WalletOutlined,
} from "@ant-design/icons";
import PropTypes from "prop-types";

import { CHAT_INTENTS } from "./helper";
import CircularTokenDisplay from "./CircularTokenDisplay";
import { useTokenStore } from "../../store/token-store";
import { useSessionStore } from "../../store/session-store";

// Define hidden intents and a fixed order array
const HIDDEN_CHAT_INTENTS = ["AUTO", "NOTA", "INFO"];
const CHAT_INTENTS_ORDER = ["TRANSFORM", "SQL"];

const CHAT_INTENTS_ICONS = {
  TRANSFORM: <RetweetOutlined rotate={90} />,
  SQL: <ConsoleSqlOutlined />,
  INFO: <MessageOutlined />,
};
const DEFAULT_CHAT_INTENT = "TRANSFORM";

const HIDE_EDITOR_SELECTOR = true;

const IS_MODELS_UNIFIED = true; // Use the same model for both Architect and Coder

const PromptActions = memo(function PromptActions({
  useMonaco,
  onUseMonacoSwitch,
  chatIntents,
  selectedChatIntent,
  setSelectedChatIntent,
  llmModels = [],
  selectedLlmModel,
  setSelectedLlmModel,
  selectedCoderLlmModel,
  setSelectedCoderLlmModel,
  selectedChatId,
  selectedChatIntentName,
  isNewChat = false,
  isOnboardingMode = false,
  isTypingPrompt = false,
  onBuyTokens,
}) {
  // Get token balance from store
  const { tokenBalance, isLoading: isTokenLoading } = useTokenStore();
  const isCloud = useSessionStore((state) => state.sessionDetails?.is_cloud);

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

  // If there's no selected LLM, pick the default
  useEffect(() => {
    if (selectedCoderLlmModel || !llmModels.length) return;
    const defaultModel = llmModels.find((m) => m.default);
    if (defaultModel?.model) {
      setSelectedCoderLlmModel(defaultModel.model);
    }
  }, [llmModels, selectedCoderLlmModel]);

  // If there's no selected Chat Intent, pick the default
  useEffect(() => {
    if (selectedChatIntent || !chatIntents.length) return;
    const defaultChatIntent = chatIntents.find(
      (intent) => intent.name === DEFAULT_CHAT_INTENT
    );
    if (defaultChatIntent?.chat_intent_id) {
      setSelectedChatIntent(defaultChatIntent.chat_intent_id);
    }
  }, [chatIntents, selectedChatIntent]);

  // Filter out hidden intents, then sort by CHAT_INTENTS_ORDER
  const filteredAndSortedIntents = useMemo(() => {
    return chatIntents
      .filter((intent) => !HIDDEN_CHAT_INTENTS.includes(intent.name))
      .sort(
        (a, b) =>
          CHAT_INTENTS_ORDER.indexOf(a.name) -
          CHAT_INTENTS_ORDER.indexOf(b.name)
      );
  }, [chatIntents]);

  return (
    <div className="chat-ai-prompt-actions-container">
      <Space>
        <Space size={0}>
          <Typography.Text type="secondary" className="font-size-12">
            {selectedChatIntentName === CHAT_INTENTS.TRANSFORM &&
            !IS_MODELS_UNIFIED
              ? "Architect:"
              : "Model:"}
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

        {selectedChatIntentName === CHAT_INTENTS.TRANSFORM &&
          !IS_MODELS_UNIFIED && (
            <Space size={0}>
              <Typography.Text type="secondary" className="font-size-12">
                Coder:
              </Typography.Text>
              <Select
                showSearch
                size="small"
                placeholder="LLM model"
                optionFilterProp="label"
                options={llmOptions}
                value={selectedCoderLlmModel}
                onChange={setSelectedCoderLlmModel}
                variant="borderless"
                dropdownClassName="small-font-dropdown"
                className="chat-ai-prompt-actions-model-select"
              />
            </Space>
          )}
      </Space>

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

      <div>
        <Tooltip
          title={
            selectedChatId &&
            "Chat intent is locked for this conversation. Please start a new chat to use a different intent."
          }
        >
          <Segmented
            className="chat-ai-custom-segmented"
            size="small"
            shape="round"
            value={selectedChatIntent}
            onChange={setSelectedChatIntent}
            disabled={selectedChatId}
            options={filteredAndSortedIntents.map((intent) => ({
              label: (
                <Typography.Text className="chat-ai-prompt-actions-monaco-font-size-10">
                  {intent.display_name}
                </Typography.Text>
              ),
              value: intent.chat_intent_id,
              icon: CHAT_INTENTS_ICONS[intent.name],
            }))}
          />
        </Tooltip>

        {!HIDE_EDITOR_SELECTOR && (
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
        )}
      </div>
    </div>
  );
});

PromptActions.propTypes = {
  useMonaco: PropTypes.bool.isRequired,
  onUseMonacoSwitch: PropTypes.func.isRequired,
  chatIntents: PropTypes.array.isRequired,
  selectedChatIntent: PropTypes.string,
  setSelectedChatIntent: PropTypes.func.isRequired,
  llmModels: PropTypes.array,
  selectedLlmModel: PropTypes.string,
  setSelectedLlmModel: PropTypes.func.isRequired,
  selectedCoderLlmModel: PropTypes.string,
  setSelectedCoderLlmModel: PropTypes.func.isRequired,
  selectedChatId: PropTypes.string,
  selectedChatIntentName: PropTypes.string,
  isNewChat: PropTypes.bool,
  isOnboardingMode: PropTypes.bool,
  isTypingPrompt: PropTypes.bool,
  onBuyTokens: PropTypes.func,
};

PromptActions.displayName = "PromptActions";

export { PromptActions };
