import PropTypes from "prop-types";
import { Tooltip } from "antd";
import { SendOutlined } from "@ant-design/icons";
import { Sender } from "@ant-design/x";

function DefaultPromptInput({
  value,
  isPromptRunning,
  isResponseStreaming = false,
  onSenderChange,
  onSubmit,
  onCancel,
  isChatConversationDisabled,
  shouldHighlightSend,
  onSendButtonClick,
}) {
  return (
    <Sender
      value={value}
      onChange={onSenderChange}
      loading={isPromptRunning}
      onSubmit={onSubmit}
      onCancel={onCancel}
      placeholder="How can I help you?"
      disabled={isChatConversationDisabled}
      actions={(_, info) => {
        const { SendButton, LoadingButton } = info.components;

        if (isPromptRunning && !isResponseStreaming) {
          return (
            <Tooltip title="Stop">
              <LoadingButton variant="text" color="secondary" shape="default" />
            </Tooltip>
          );
        }

        return (
          <Tooltip title={value ? "Send ↵" : "Please type something"}>
            <SendButton
              variant="text"
              color="secondary"
              icon={
                <SendOutlined
                  style={
                    shouldHighlightSend
                      ? {
                          color: "#1890ff",
                          animation: "heartbeat-icon 1.2s ease-in-out infinite",
                        }
                      : {}
                  }
                />
              }
              shape="default"
              disabled={!value || isPromptRunning}
              className={shouldHighlightSend ? "onboarding-send-highlight" : ""}
            />
          </Tooltip>
        );
      }}
    />
  );
}

DefaultPromptInput.propTypes = {
  value: PropTypes.string.isRequired,
  isPromptRunning: PropTypes.bool.isRequired,
  isResponseStreaming: PropTypes.bool,
  onSenderChange: PropTypes.func.isRequired,
  onSubmit: PropTypes.func.isRequired,
  onCancel: PropTypes.func.isRequired,
  isChatConversationDisabled: PropTypes.bool,
  shouldHighlightSend: PropTypes.bool,
  onSendButtonClick: PropTypes.func,
};

export { DefaultPromptInput };
