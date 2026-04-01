import { memo } from "react";
import PropTypes from "prop-types";
import { Button, Result } from "antd";
import { SettingOutlined } from "@ant-design/icons";

const DisabledOverlay = memo(function DisabledOverlay({ onConfigure }) {
  return (
    <div className="git-disabled-overlay">
      <Result
        status="info"
        title="Version control is not enabled"
        subTitle="Configure a git repository to start tracking changes to your transformation models."
        extra={
          <Button
            type="primary"
            icon={<SettingOutlined />}
            onClick={onConfigure}
          >
            Configure Versioning
          </Button>
        }
      />
    </div>
  );
});

DisabledOverlay.propTypes = {
  onConfigure: PropTypes.func.isRequired,
};

DisabledOverlay.displayName = "DisabledOverlay";

export { DisabledOverlay };
