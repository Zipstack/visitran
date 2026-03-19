import { memo } from "react";
import PropTypes from "prop-types";
import { Typography } from "antd";
import { FullscreenOutlined } from "@ant-design/icons";

import { useProjectStore } from "../../store/project-store";

import "../ide-layout.css";

const ICON_STYLE = { color: "#fff" };
const TITLE_STYLE = { margin: "unset", color: "var(--font-color-1)" };

const IdeTopMenu = memo(function IdeTopMenu({ onFullscreenToggle = () => {} }) {
  const { Title } = Typography;
  const projectName = useProjectStore((state) => state.projectName);

  return (
    <div className="ideHeader">
      <Title level={5} style={TITLE_STYLE} ellipsis>
        {projectName}
      </Title>
      <FullscreenOutlined style={ICON_STYLE} onClick={onFullscreenToggle} />
    </div>
  );
});

IdeTopMenu.propTypes = {
  onFullscreenToggle: PropTypes.func,
};

export { IdeTopMenu };
