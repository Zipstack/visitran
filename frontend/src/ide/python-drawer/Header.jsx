import { memo } from "react";
import PropTypes from "prop-types";
import { Button, Typography } from "antd";
import { CloseOutlined } from "@ant-design/icons";

// Create a CSS file for the Python drawer components
const Header = memo(function Header({ closePythonDrawer, modelName }) {
  return (
    <div className="flex-space-between chat-ai-header">
      <div>
        <Typography.Text strong style={{ fontWeight: "bold" }}>
          [PYTHON]
        </Typography.Text>
        <Typography.Text strong> Generated Code : </Typography.Text>
        <Typography.Text italic>{modelName}</Typography.Text>
      </div>
      <div>
        <Button
          type="text"
          size="small"
          icon={<CloseOutlined />}
          onClick={closePythonDrawer}
        />
      </div>
    </div>
  );
});

Header.propTypes = {
  closePythonDrawer: PropTypes.func.isRequired,
  modelName: PropTypes.string.isRequired,
};

Header.displayName = "Header";

export { Header };
