import { Button, Space, Typography, Menu, Dropdown } from "antd";
import { memo, useEffect } from "react";
import PropTypes from "prop-types";
import {
  DownOutlined,
  ExportOutlined,
  LoadingOutlined,
  CheckCircleFilled,
} from "@ant-design/icons";

import { useSocketMessagesStore } from "../../store/socket-messages-store";

const TransformApply = memo(function TransformApply({
  message,
  selectedChatId,
  handleTransformApply,
  triggerRetryTransform,
  isLatestTransform,
}) {
  // Render‑guard: only latest + successful prompt
  if (
    !isLatestTransform ||
    message.prompt_status !== "SUCCESS" ||
    !message.transformation_status
  ) {
    return null;
  }
  const generateModelMap = useSocketMessagesStore((s) => s.generateModelMap);
  const modelData = generateModelMap?.[message.chat_message_id];

  const modelList =
    modelData?.models?.length > 0
      ? modelData.models
      : Array.isArray(message.generated_models)
      ? message.generated_models
      : [];

  const status = message.transformation_status;
  const isLoading = status === "RUNNING";
  const isDisabled = status === "SUCCESS" || status === "FAILED";
  const shouldShowDropdown = status === "SUCCESS" && modelList.length > 0;

  useEffect(() => {
    if (triggerRetryTransform && isLatestTransform && isLoading) {
      onApplyClick();
    }
  }, [triggerRetryTransform]);

  const onApplyClick = () => {
    handleTransformApply({
      chatId: selectedChatId,
      chatMessageId: message.chat_message_id,
      response: message.response,
    });
  };

  const handleExportClick = (modelName) => {
    // TODO: Hook this to actual export logic
  };

  const generateModelMenu = (
    <Menu>
      {modelList.map((modelName) => (
        <Menu.Item
          key={modelName}
          onClick={() => handleExportClick(modelName)}
          title={modelName}
        >
          <Space style={{ justifyContent: "space-between", width: "100%" }}>
            <span>{modelName}</span>
            <ExportOutlined />
          </Space>
        </Menu.Item>
      ))}
    </Menu>
  );

  // Button text and icon based on status
  const getButtonContent = () => {
    if (isLoading) {
      return (
        <>
          <LoadingOutlined style={{ marginRight: 6 }} />
          Generating Models...
        </>
      );
    }
    if (status === "SUCCESS") {
      return (
        <>
          <CheckCircleFilled style={{ marginRight: 6 }} />
          Models Materialized
        </>
      );
    }
    return "Apply";
  };

  return (
    <Space direction="vertical" className="width-100">
      <Space>
        <Button
          type="primary"
          size="small"
          onClick={onApplyClick}
          disabled={isDisabled}
        >
          {getButtonContent()}
        </Button>
        {status === "FAILED" && (
          <Typography.Text type="danger">Failed</Typography.Text>
        )}
        {shouldShowDropdown && (
          <Dropdown overlay={generateModelMenu} trigger={["click"]}>
            <Button size="small">
              Generated Models <DownOutlined />
            </Button>
          </Dropdown>
        )}
      </Space>
    </Space>
  );
});

TransformApply.propTypes = {
  message: PropTypes.object.isRequired,
  selectedChatId: PropTypes.string.isRequired,
  handleTransformApply: PropTypes.func.isRequired,
  triggerRetryTransform: PropTypes.bool.isRequired,
  isLatestTransform: PropTypes.bool.isRequired,
};

TransformApply.displayName = "TransformApply";

export { TransformApply };
