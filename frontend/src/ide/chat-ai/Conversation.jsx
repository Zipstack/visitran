import { memo, useMemo, useState, useCallback } from "react";
import PropTypes from "prop-types";
import { Alert, Divider, Space } from "antd";

import { PromptInfo } from "./PromptInfo";
import { EnhancedPromptInfo } from "./EnhancedPromptInfo";
import { MarkdownView } from "./MarkdownView";
import { UserPrompt } from "./UserPrompt";
import { useUserStore } from "../../store/user-store";
import { ResponseFooter } from "./ResponseFooter";
import { DisplayErrorMessages } from "./DisplayErrorMessages";
import RetryIndicator from "./RetryIndicator";
import ModelGenerationProgress from "./ModelGenerationProgress";

const Conversation = memo(function Conversation({
  savePrompt,
  message,
  chatIntents,
  llmModels,
  isPromptRunning,
  isLastConversation,
  selectedChatId,
  handleTransformApply,
  triggerRetryTransform,
  handleSqlRun,
  isLatestTransform,
  selectedChatIntent,
}) {
  const userDetails = useUserStore((state) => state.userDetails);
  const [detectedAction, setDetectedAction] = useState(null);

  // Handle action detection from MarkdownView
  const handleActionDetected = useCallback((actionType) => {
    setDetectedAction(actionType);
  }, []);

  // Create UI action object based on detected action
  const uiAction = useMemo(() => {
    if (!detectedAction) return null;

    if (detectedAction === "proceed") {
      return {
        show_button: true,
        button_type: "proceed",
        button_text: "Proceed with this approach",
      };
    } else if (detectedAction === "build_models") {
      return {
        show_button: true,
        button_type: "build_models",
        button_text: "Build Models",
      };
    } else if (detectedAction === "run_sql") {
      return {
        show_button: true,
        button_type: "run_sql",
        button_text: "Run",
      };
    } else if (detectedAction === "apply") {
      return {
        show_button: true,
        button_type: "apply",
        button_text: "Apply",
      };
    }

    return null;
  }, [detectedAction]);

  // Derive display names from IDs
  const llmModelDisplayName = useMemo(() => {
    return llmModels.find((m) => m?.model === message?.llm_model_architect)
      ?.display_name;
  }, [llmModels, message?.llm_model_architect]);

  const coderLlmModelDisplayName = useMemo(() => {
    return llmModels.find((m) => m?.model === message?.llm_model_developer)
      ?.display_name;
  }, [llmModels, message?.llm_model_developer]);

  const chatIntentName = useMemo(() => {
    return chatIntents.find(
      (intent) => intent?.chat_intent_id === message?.chat_intent
    )?.name;
  }, [chatIntents, message?.chat_intent]);

  // Check if we have any "thought chain" data
  const isThoughtChainReceived = useMemo(() => {
    return !!(message?.response?.length && message.response[0]?.length);
  }, [message?.response]);

  // Check if transformation is in progress
  const isTransformRunning = useMemo(() => {
    return message?.transformation_status === "RUNNING";
  }, [message?.transformation_status]);

  // Feature flag: Use enhanced UI components (can be toggled)
  const useEnhancedUI = true; // Set to false to use legacy components

  /** --------------------------------------------------------------------
   *  Derive the intent once; re-computes only when its deps change.
   * ------------------------------------------------------------------- */
  const intent = useMemo(
    () =>
      chatIntents.find(
        ({ chat_intent_id }) => chat_intent_id === message?.chat_intent
      ),
    [chatIntents, message?.chat_intent]
  );

  return (
    <div>
      {/* PROMPT */}
      <div className="border-radius-5-top">
        <UserPrompt prompt={message?.prompt} user={message?.user} />
      </div>

      {/* RESPONSE */}
      <Space direction="vertical" className="chat-ai-existing-chat-response">
        {/* Retry Indicator - NEW */}
        {message?.is_retry_transform && isTransformRunning && (
          <div className="chat-ai-existing-chat-response-pad">
            <RetryIndicator
              isRetrying={true}
              errorMessage={
                message?.transformation_error_message?.error_message
              }
            />
          </div>
        )}

        {/* Model Generation Progress - NEW */}
        {message?.generate_model_list &&
          message.generate_model_list.length > 0 && (
            <div className="chat-ai-existing-chat-response-pad">
              <ModelGenerationProgress
                modelList={message.generate_model_list}
                modelStatus={message.generate_model_status}
                isProcessing={isTransformRunning}
              />
            </div>
          )}

        {/* Thought Chain - Enhanced or Legacy */}
        <div className="chat-ai-conversation chat-ai-existing-chat-response-pad">
          {useEnhancedUI ? (
            <EnhancedPromptInfo
              isThoughtChainReceived={isThoughtChainReceived}
              shouldStream={isPromptRunning && isLastConversation}
              thoughtChain={message?.thought_chain || []}
              llmModel={llmModelDisplayName}
              coderLlmModel={coderLlmModelDisplayName}
              chatIntent={chatIntentName}
              errorState={message?.error_state}
              errorDetails={{
                transformation_error_message:
                  message?.transformation_error_message,
                prompt_error_message: message?.prompt_error_message,
              }}
            />
          ) : (
            <PromptInfo
              isThoughtChainReceived={isThoughtChainReceived}
              shouldStream={isPromptRunning && isLastConversation}
              thoughtChain={message?.thought_chain || []}
              llmModel={llmModelDisplayName}
              coderLlmModel={coderLlmModelDisplayName}
              chatIntent={chatIntentName}
            />
          )}
        </div>

        <div className="chat-ai-existing-chat-response-pad">
          <MarkdownView
            markdownChunks={message?.response || []}
            shouldStream={isPromptRunning && isLastConversation}
            currentTheme={userDetails?.currentTheme}
            onActionDetected={handleActionDetected}
          />
        </div>

        {message?.prompt_status === "SUCCESS" && (
          <ResponseFooter
            intent={intent}
            message={message}
            selectedChatId={selectedChatId}
            handleTransformApply={handleTransformApply}
            triggerRetryTransform={triggerRetryTransform}
            handleSqlRun={handleSqlRun}
            isLatestTransform={isLatestTransform}
            savePrompt={savePrompt}
            selectedChatIntent={selectedChatIntent}
            uiAction={uiAction}
          />
        )}
        {(message?.prompt_status === "FAILED" ||
          message?.transformation_status === "FAILED") &&
          intent?.name === "TRANSFORM" && (
            <Alert
              message="Note: No credits were charged for this request."
              type="info"
            />
          )}
        <DisplayErrorMessages
          socketError={message?.error_msg}
          promptError={message?.prompt_error_message}
          transformError={message?.transformation_error_message}
          errorState={message?.error_state}
        />
      </Space>
      {isLastConversation ? <div className="pad-12" /> : <Divider />}
    </div>
  );
});

Conversation.propTypes = {
  savePrompt: PropTypes.func.isRequired,
  message: PropTypes.object.isRequired,
  chatIntents: PropTypes.array.isRequired,
  llmModels: PropTypes.array.isRequired,
  isPromptRunning: PropTypes.bool.isRequired,
  isLastConversation: PropTypes.bool.isRequired,
  selectedChatId: PropTypes.string.isRequired,
  handleTransformApply: PropTypes.func.isRequired,
  triggerRetryTransform: PropTypes.bool.isRequired,
  handleSqlRun: PropTypes.func.isRequired,
  isLatestTransform: PropTypes.bool.isRequired,
  selectedChatIntent: PropTypes.string,
};

Conversation.displayName = "Conversation";

export { Conversation };
