import { memo, useMemo, useState, useCallback } from "react";
import PropTypes from "prop-types";
import { Alert, Divider, Space } from "antd";

import { PromptInfo } from "./PromptInfo";
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

  // Handle troubleshoot button click from error messages
  const handleTroubleshoot = useCallback(
    (errorMessage) => {
      const prompt = `There was an error encountered. We have the detailed error message below. Please see how we can fix this:\n\n${errorMessage}`;
      savePrompt(prompt, selectedChatIntent);
    },
    [savePrompt, selectedChatIntent]
  );

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

  // Check if transformation is in progress
  const isTransformRunning = useMemo(() => {
    return message?.transformation_status === "RUNNING";
  }, [message?.transformation_status]);

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

  // Memoize errorDetails to prevent unnecessary re-renders of PromptInfo
  const errorDetailsMemo = useMemo(
    () => ({
      transformation_error_message: message?.transformation_error_message,
      prompt_error_message: message?.prompt_error_message,
    }),
    [message?.transformation_error_message, message?.prompt_error_message]
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

        {/* Thought Chain */}
        <div className="chat-ai-conversation chat-ai-existing-chat-response-pad">
          <PromptInfo
            shouldStream={isPromptRunning && isLastConversation}
            thoughtChain={message?.thought_chain || []}
            errorState={message?.error_state}
            errorDetails={errorDetailsMemo}
          />
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
          onTroubleshoot={handleTroubleshoot}
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
