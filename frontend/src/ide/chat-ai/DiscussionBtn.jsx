import { memo, useCallback, useMemo } from "react";
import PropTypes from "prop-types";
import { Button, Space, Tooltip } from "antd";

const BTN_TEXT_MAP = {
  PROCEED: { label: "Proceed", value: "APPROVED" },
  BUILD_MODEL: { label: "Build Model", value: "GENERATE" },
};

const INFO_PROCEED_REJECT =
  "If you want to interact with Visitran AI, please use the chat area below. If you want Visitran AI to proceed without any more input from you, press the 'Proceed' button, or reply with 'Proceed'";

const INFO_APPROVED =
  "If you want to interact with Visitran AI, please use the chat area below. If you want Visitran AI to proceed without any more input from you, press the 'Build Model' button, or reply with 'Build Model'";

const DiscussionBtn = memo(function DiscussionBtn({
  chatMessageId,
  savePrompt,
  selectedChatIntent,
  isLatestTransform,
  uiAction,
}) {
  const buttonData = useMemo(() => {
    if (!uiAction?.show_button) return null;

    // Map button type to BTN_TEXT_MAP or use custom text
    if (uiAction.button_type === "proceed") {
      return {
        label: BTN_TEXT_MAP.PROCEED.label,
        value: BTN_TEXT_MAP.PROCEED.value,
        tooltip: INFO_PROCEED_REJECT,
      };
    } else if (uiAction.button_type === "build_models") {
      return {
        label: BTN_TEXT_MAP.BUILD_MODEL.label,
        value: BTN_TEXT_MAP.BUILD_MODEL.value,
        tooltip: INFO_APPROVED,
      };
    }

    return null;
  }, [uiAction]);

  const handleSubmit = useCallback(
    ({ label, value }) => {
      if (value === "GENERATE") {
        savePrompt?.(label, selectedChatIntent, false, value, chatMessageId);
        return;
      }
      savePrompt?.(label, selectedChatIntent, false, value);
    },
    [savePrompt, selectedChatIntent, chatMessageId]
  );

  if (!buttonData) return null;

  return (
    <Space direction="vertical" className="width-100">
      <Tooltip title={buttonData?.tooltip}>
        <Button
          key={buttonData?.value}
          size="small"
          onClick={() =>
            handleSubmit({ label: buttonData?.label, value: buttonData?.value })
          }
          disabled={!isLatestTransform}
        >
          {buttonData?.label}
        </Button>
      </Tooltip>
    </Space>
  );
});

DiscussionBtn.displayName = "DiscussionBtn";

DiscussionBtn.propTypes = {
  chatMessageId: PropTypes.string.isRequired,
  savePrompt: PropTypes.func.isRequired,
  selectedChatIntent: PropTypes.string.isRequired,
  isLatestTransform: PropTypes.bool.isRequired,
  uiAction: PropTypes.object,
};

export { DiscussionBtn };
