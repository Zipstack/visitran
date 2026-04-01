import { Tooltip, Typography, theme } from "antd";
import PropTypes from "prop-types";

const { useToken } = theme;

const InfoChip = ({
  icon,
  text,
  tooltipTitle,
  tooltipPlacement = "top",
  className = "",
}) => {
  const { token } = useToken();

  const chip = (
    <div className={`chat-ai-info-chip ${className}`}>
      {icon}
      <Typography.Text className="chat-ai-info-chip-text">
        {text}
      </Typography.Text>
    </div>
  );

  if (!tooltipTitle) return chip;

  return (
    <Tooltip
      title={tooltipTitle}
      placement={tooltipPlacement}
      color={token.colorBgElevated}
      overlayInnerStyle={{
        border: `1px solid ${token.colorBorder}`,
        borderRadius: "8px",
      }}
    >
      {chip}
    </Tooltip>
  );
};

InfoChip.propTypes = {
  icon: PropTypes.node.isRequired,
  text: PropTypes.string.isRequired,
  tooltipTitle: PropTypes.node,
  tooltipPlacement: PropTypes.string,
  className: PropTypes.string,
};

export default InfoChip;
