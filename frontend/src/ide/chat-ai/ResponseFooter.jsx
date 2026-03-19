import { memo } from "react";
import PropTypes from "prop-types";
import { Space } from "antd";

import { SqlRun } from "./SqlRun";
import { ActionButtons } from "./ActionButtons";
import FeedbackButtons from "./FeedbackButtons";
import TokenUsageDisplay from "./TokenUsageDisplay";

const ResponseFooter = memo(
  ({
    intent,
    message,
    selectedChatId,
    handleTransformApply,
    triggerRetryTransform,
    handleSqlRun,
    isLatestTransform,
    savePrompt,
    selectedChatIntent,
    uiAction,
  }) => {
    if (!intent) return null;

    /** ------------------------------- SQL ------------------------------ */
    if (intent?.name === "SQL") {
      return (
        <div className="chat-ai-response-container" style={{ width: "100%" }}>
          {/* Message content area */}
          <Space
            direction="vertical"
            className="chat-ai-response-footer"
            style={{ width: "100%" }}
          >
            <SqlRun
              message={message}
              selectedChatId={selectedChatId}
              handleSqlRun={handleSqlRun}
              uiAction={uiAction}
            />
          </Space>

          {/* Token usage and feedback buttons - always shown */}
          <div
            className="chat-feedback-container"
            style={{
              display: "flex",
              justifyContent: "space-between",
              marginTop: "10px",
            }}
          >
            <div>{/* This is where the Proceed button will be */}</div>
            <div style={{ display: "flex", gap: "4px", alignItems: "center" }}>
              <TokenUsageDisplay tokenUsageData={message?.token_usage_data} />
              <FeedbackButtons chatMessageId={message?.chat_message_id} />
            </div>
          </div>
        </div>
      );
    }

    /** --------------------------- TRANSFORM --------------------------- */
    if (intent?.name === "TRANSFORM") {
      return (
        <div className="chat-ai-response-container" style={{ width: "100%" }}>
          {/* Message content area */}
          <Space
            direction="vertical"
            className="chat-ai-response-footer width-100"
          >
            <ActionButtons
              chatMessageId={message?.chat_message_id}
              uiAction={uiAction}
              savePrompt={savePrompt}
              selectedChatIntent={selectedChatIntent}
              isLatestTransform={isLatestTransform}
              message={message}
              selectedChatId={selectedChatId}
              handleTransformApply={handleTransformApply}
              triggerRetryTransform={triggerRetryTransform}
            />
          </Space>
          {/* Token usage and feedback buttons - always shown, separate from action buttons */}
          <div
            className="chat-feedback-container"
            style={{
              display: "flex",
              justifyContent: "space-between",
              marginTop: "10px",
            }}
          >
            <div>{/* This is where action buttons will appear */}</div>
            <div style={{ display: "flex", gap: "4px", alignItems: "center" }}>
              <TokenUsageDisplay tokenUsageData={message?.token_usage_data} />
              <FeedbackButtons chatMessageId={message?.chat_message_id} />
            </div>
          </div>
        </div>
      );
    }

    /** ------------------------------- INFO (CHAT) ------------------------------ */
    if (intent?.name === "INFO") {
      return (
        <div className="chat-ai-response-container" style={{ width: "100%" }}>
          {/* Token usage display only - no feedback buttons for chat messages */}
          <div
            className="chat-feedback-container"
            style={{
              display: "flex",
              justifyContent: "flex-end",
              marginTop: "10px",
            }}
          >
            <TokenUsageDisplay tokenUsageData={message?.token_usage_data} />
          </div>
        </div>
      );
    }

    return null;
  }
);

ResponseFooter.displayName = "ResponseFooter";

ResponseFooter.propTypes = {
  intent: PropTypes.shape({
    chat_intent_id: PropTypes.oneOfType([PropTypes.string, PropTypes.number])
      .isRequired,
    name: PropTypes.string.isRequired,
  }),
  message: PropTypes.object.isRequired,
  selectedChatId: PropTypes.string.isRequired,
  handleTransformApply: PropTypes.func.isRequired,
  triggerRetryTransform: PropTypes.bool.isRequired,
  handleSqlRun: PropTypes.func.isRequired,
  isLatestTransform: PropTypes.bool.isRequired,
  savePrompt: PropTypes.func.isRequired,
  selectedChatIntent: PropTypes.string,
  uiAction: PropTypes.object,
};

export { ResponseFooter };
