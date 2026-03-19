import { memo } from "react";
import PropTypes from "prop-types";
import { Button, Typography } from "antd";
import { CloseOutlined } from "@ant-design/icons";

import { useProjectStore } from "../../store/project-store";
import { getActiveModelName } from "../../common/helpers";

const Header = memo(function Header({ closeSQLDrawer }) {
  const { projectId, projectDetails } = useProjectStore.getState();
  const modelName = getActiveModelName(projectId, projectDetails);
  return (
    <div className="flex-space-between chat-ai-header">
      <div>
        <>
          <Typography.Text strong style={{ fontWeight: "bold" }}>
            [SQL]
          </Typography.Text>
          <Typography.Text strong> Generated Query : </Typography.Text>
          <Typography.Text italic>{modelName}</Typography.Text>
        </>
      </div>
      <div>
        <Button
          type="text"
          size="small"
          icon={<CloseOutlined />}
          onClick={closeSQLDrawer}
        />
      </div>
    </div>
  );
});

Header.propTypes = {
  closeSQLDrawer: PropTypes.func.isRequired,
};

Header.displayName = "Header";

export { Header };
