import { memo, useCallback, useMemo, useEffect, useState, useRef } from "react";
import PropTypes from "prop-types";
import {
  DownOutlined,
  UpOutlined,
  LoadingOutlined,
  CheckCircleFilled,
} from "@ant-design/icons";
import { Alert, Button, Space, Tooltip, Typography } from "antd";

import { useSocketMessagesStore } from "../../store/socket-messages-store";
import { useProjectStore } from "../../store/project-store";

const BTN_TEXT_MAP = {
  PROCEED: { label: "Proceed", value: "APPROVED" },
  BUILD_MODEL: { label: "Build Model", value: "GENERATE" },
};

const INFO_PROCEED_REJECT =
  "If you want to interact with Visitran AI, please use the chat area below. If you want Visitran AI to proceed without any more input from you, press the 'Proceed' button, or reply with 'Proceed'";

const INFO_APPROVED =
  "If you want to interact with Visitran AI, please use the chat area below. If you want Visitran AI to proceed without any more input from you, press the 'Build Model' button, or reply with 'Build Model'";

const ActionButtons = memo(function ActionButtons({
  chatMessageId,
  savePrompt,
  selectedChatIntent,
  isLatestTransform,
  uiAction,
  message,
  selectedChatId,
  handleTransformApply,
  triggerRetryTransform,
}) {
  const generateModelMap = useSocketMessagesStore((s) => s.generateModelMap);
  const [expanded, setExpanded] = useState(false);
  const [isOperationInProgress, setIsOperationInProgress] = useState(false);
  const applyInProgressRef = useRef(false);
  const hasAutoOpened = useRef(false);
  const applyTriggered = useRef(false);

  const MAX_VISIBLE_MODELS = 3;

  const { projectName } = useProjectStore();

  // Handle Discussion buttons (proceed, build_models)
  const discussionButtonData = useMemo(() => {
    if (!uiAction?.show_button) return null;

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

  // Handle Apply button logic
  const shouldShowApplyButton = useMemo(() => {
    const res =
      uiAction?.button_type === "apply" &&
      isLatestTransform &&
      message?.prompt_status === "SUCCESS" &&
      message?.transformation_status;
    return res;
  }, [uiAction, isLatestTransform, message]);

  const modelData = generateModelMap?.[chatMessageId];
  const modelList = useMemo(() => {
    const fromSocket = modelData?.models?.length > 0 ? modelData.models : null;
    const fromMessage = Array.isArray(message?.generated_models)
      ? message.generated_models
      : null;

    if (fromSocket) return fromSocket;
    if (fromMessage) return fromMessage;
    return [];
  }, [modelData, message]);

  const transformStatus = message?.transformation_status;
  const isLoading = transformStatus === "RUNNING";
  const isDisabled =
    transformStatus === "SUCCESS" || transformStatus === "FAILED";
  const shouldShowDropdown =
    transformStatus === "SUCCESS" && modelList.length > 0;

  const handleDiscussionSubmit = useCallback(
    ({ label, value }) => {
      if (isOperationInProgress) return;

      setIsOperationInProgress(true);
      setTimeout(() => setIsOperationInProgress(false), 3000);

      if (value === "GENERATE") {
        savePrompt?.(label, selectedChatIntent, false, value, chatMessageId);
        return;
      }
      savePrompt?.(label, selectedChatIntent, false, value);
    },
    [savePrompt, selectedChatIntent, chatMessageId, isOperationInProgress]
  );

  const onApplyClick = useCallback(() => {
    if (applyInProgressRef.current) {
      return;
    }

    applyInProgressRef.current = true;

    applyTriggered.current = true;
    hasAutoOpened.current = false;

    handleTransformApply?.({
      chatId: selectedChatId,
      chatMessageId,
      response: message?.response,
    });
  }, [handleTransformApply, selectedChatId, chatMessageId, message?.response]);

  useEffect(() => {
    if (triggerRetryTransform && isLatestTransform && isLoading) {
      onApplyClick();
    }
  }, [triggerRetryTransform, isLatestTransform, isLoading, onApplyClick]);

  const handleExportModel = useCallback(
    (modelName) => {
      const projectId = useProjectStore.getState().projectId;
      const { projectDetails } = useProjectStore.getState();
      const openedTabs = projectDetails[projectId]?.openedTabs || [];

      const newTab = {
        label: modelName,
        key: `${projectName}/models/no_code/${modelName}`,
        type: "NO_CODE_MODEL",
        extension: modelName,
      };

      const alreadyExists = openedTabs.some((tab) => tab.key === newTab.key);

      if (!alreadyExists) {
        useProjectStore
          .getState()
          .setOpenedTabs([...openedTabs, newTab], projectId);
      }

      useProjectStore.getState().makeActiveTab(newTab, projectId);
    },
    [projectName]
  );

  // Auto-open last generated model only once after Apply success
  useEffect(() => {
    // Skip if nothing triggered or no models
    if (!applyTriggered.current || modelList.length === 0) return;

    // We only want to act when status *just turned* SUCCESS
    if (transformStatus === "SUCCESS" && !hasAutoOpened.current) {
      const lastModel = modelList[modelList.length - 1];
      handleExportModel(lastModel);
      hasAutoOpened.current = true;
      applyTriggered.current = false;
    }

    // If status goes back to something else (e.g. RUNNING/FAILED), reset open flag
    if (transformStatus !== "SUCCESS") {
      hasAutoOpened.current = false;
    }
  }, [transformStatus]);

  // Render Discussion buttons (proceed, build_models)
  if (discussionButtonData) {
    if (!isLatestTransform) {
      return (
        <Tooltip
          title={`This button is no longer applicable as we've moved forward from this point.`}
        >
          <Button
            key={discussionButtonData.value}
            size="small"
            onClick={() =>
              handleDiscussionSubmit({
                label: discussionButtonData.label,
                value: discussionButtonData.value,
              })
            }
            disabled={!isLatestTransform}
          >
            {discussionButtonData.label}
          </Button>
        </Tooltip>
      );
    }

    return (
      <div className="action-btn-wrapper">
        <Space
          direction="horizontal"
          className="width-100 action-btn-container"
          align="start"
        >
          <Button
            key={discussionButtonData.value}
            size="small"
            onClick={() =>
              handleDiscussionSubmit({
                label: discussionButtonData.label,
                value: discussionButtonData.value,
              })
            }
          >
            {discussionButtonData.label}
          </Button>

          <Typography.Text type="secondary" className="action-btn-info">
            {discussionButtonData.tooltip}
          </Typography.Text>
        </Space>
      </div>
    );
  }

  // Helper function to get button content based on status
  const getButtonContent = () => {
    if (isLoading) {
      return (
        <>
          <LoadingOutlined style={{ marginRight: 6 }} />
          Generating Models...
        </>
      );
    }
    if (transformStatus === "SUCCESS") {
      return (
        <>
          <CheckCircleFilled style={{ marginRight: 6 }} />
          Models Materialized
        </>
      );
    }
    return "Apply";
  };

  // Render Apply or Dropdown
  if (shouldShowApplyButton || shouldShowDropdown) {
    return (
      <Space direction="vertical" className="width-100">
        {shouldShowApplyButton && (
          <Space>
            <Button
              type="primary"
              size="small"
              onClick={onApplyClick}
              disabled={isDisabled}
            >
              {getButtonContent()}
            </Button>
            {transformStatus === "FAILED" && (
              <Typography.Text type="danger">Failed</Typography.Text>
            )}
          </Space>
        )}

        {shouldShowDropdown && (
          <Alert
            message={
              <span>
                Visitran AI successfully generated the model
                {modelList.length > 1 ? "(s)" : ""}:{" "}
                {modelList
                  .slice(0, expanded ? modelList.length : MAX_VISIBLE_MODELS)
                  .map((modelName, idx, arr) => (
                    <span key={modelName}>
                      <a
                        onClick={() => handleExportModel(modelName)}
                        style={{ marginRight: 4 }}
                      >
                        {modelName}
                      </a>
                      {idx < arr.length - 1 && ", "}
                    </span>
                  ))}
                {modelList.length > MAX_VISIBLE_MODELS && !expanded && (
                  <a onClick={() => setExpanded(true)}>
                    &nbsp;and {modelList.length - MAX_VISIBLE_MODELS} more{" "}
                    <DownOutlined />
                  </a>
                )}
                {expanded && modelList.length > MAX_VISIBLE_MODELS && (
                  <a onClick={() => setExpanded(false)}>
                    &nbsp;
                    <UpOutlined /> Show less
                  </a>
                )}
              </span>
            }
            type="success"
            showIcon
            icon={<CheckCircleFilled />}
            style={{ marginTop: 8 }}
          />
        )}
      </Space>
    );
  }

  return null;
});

ActionButtons.displayName = "ActionButtons";

ActionButtons.propTypes = {
  chatMessageId: PropTypes.string.isRequired,
  savePrompt: PropTypes.func,
  selectedChatIntent: PropTypes.string,
  isLatestTransform: PropTypes.bool.isRequired,
  uiAction: PropTypes.object,
  message: PropTypes.object,
  selectedChatId: PropTypes.string,
  handleTransformApply: PropTypes.func,
  triggerRetryTransform: PropTypes.bool,
};

export { ActionButtons };
