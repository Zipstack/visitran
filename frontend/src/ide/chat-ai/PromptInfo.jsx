import { memo } from "react";
import PropTypes from "prop-types";
import { Bubble } from "@ant-design/x";
import { Space, Typography } from "antd";

const PromptInfo = memo(function PromptInfo({
  isThoughtChainReceived,
  shouldStream,
  thoughtChain = [],
  llmModel,
  coderLlmModel,
  chatIntent,
}) {
  const renderThoughtChain = () => (
    <div className="width-100">
      <Space direction="vertical">
        {thoughtChain?.map((msg) => {
          return (
            <Typography.Text
              key={msg}
              type="secondary"
              className="font-size-12"
            >
              {msg}
            </Typography.Text>
          );
        })}
      </Space>
    </div>
  );

  return (
    <Bubble
      typing={shouldStream ? { step: 100, interval: 50 } : false}
      messageRender={renderThoughtChain}
      variant="borderless"
      className="width-100"
    />
  );
});

PromptInfo.propTypes = {
  isThoughtChainReceived: PropTypes.bool.isRequired,
  shouldStream: PropTypes.bool,
  thoughtChain: PropTypes.array,
  llmModel: PropTypes.string,
  coderLlmModel: PropTypes.string,
  chatIntent: PropTypes.string,
};

PromptInfo.displayName = "PromptInfo";

export { PromptInfo };
