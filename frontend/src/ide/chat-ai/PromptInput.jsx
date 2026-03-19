import { memo } from "react";
import PropTypes from "prop-types";

import { useUserStore } from "../../store/user-store";
import { DefaultPromptInput } from "./DefaultPromptInput";
import { MonacoPromptInput } from "./MonacoPromptInput";

const PromptInput = memo(function PromptInput({
  useMonaco,
  value,
  editorHeight,
  isPromptRunning,
  onSenderChange,
  onSubmit,
  onEditorMount,
  onMonacoChange,
  onCancel,
  isChatConversationDisabled,
  shouldHighlightSend,
  onSendButtonClick,
  onMentionTrigger,
  onMentionSearchChange,
  onMentionDismiss,
  mentionTriggerRef,
}) {
  // Retrieve user details once, then pass to the Monaco component (if used)
  const userDetails = useUserStore((state) => state.userDetails);

  // Determine if input should be disabled (during onboarding after typing)
  const isDisabled = isChatConversationDisabled || shouldHighlightSend;

  if (!useMonaco) {
    return (
      <DefaultPromptInput
        value={value}
        isPromptRunning={isPromptRunning}
        onSenderChange={onSenderChange}
        onSubmit={onSubmit}
        onCancel={onCancel}
        isChatConversationDisabled={isDisabled}
        shouldHighlightSend={shouldHighlightSend}
        onSendButtonClick={onSendButtonClick}
      />
    );
  }

  return (
    <MonacoPromptInput
      value={value}
      editorHeight={editorHeight}
      isPromptRunning={isPromptRunning}
      onEditorMount={onEditorMount}
      onMonacoChange={onMonacoChange}
      onSubmit={onSubmit}
      onCancel={onCancel}
      userDetails={userDetails}
      isChatConversationDisabled={isDisabled}
      shouldHighlightSend={shouldHighlightSend}
      onSendButtonClick={onSendButtonClick}
      onMentionTrigger={onMentionTrigger}
      onMentionSearchChange={onMentionSearchChange}
      onMentionDismiss={onMentionDismiss}
      mentionTriggerRef={mentionTriggerRef}
    />
  );
});

PromptInput.propTypes = {
  useMonaco: PropTypes.bool.isRequired,
  value: PropTypes.string.isRequired,
  editorHeight: PropTypes.number.isRequired,
  isPromptRunning: PropTypes.bool.isRequired,
  onSenderChange: PropTypes.func.isRequired,
  onSubmit: PropTypes.func.isRequired,
  onEditorMount: PropTypes.func.isRequired,
  onMonacoChange: PropTypes.func.isRequired,
  onCancel: PropTypes.func.isRequired,
  isChatConversationDisabled: PropTypes.bool,
  shouldHighlightSend: PropTypes.bool,
  onSendButtonClick: PropTypes.func,
  onMentionTrigger: PropTypes.func,
  onMentionSearchChange: PropTypes.func,
  onMentionDismiss: PropTypes.func,
  mentionTriggerRef: PropTypes.object,
};

PromptInput.displayName = "PromptInput";
export { PromptInput };
