import { useEffect, useState } from "react";
import { Tooltip, Typography, Space, Spin, theme } from "antd";
import { LoadingOutlined, WalletOutlined } from "@ant-design/icons";
import PropTypes from "prop-types";

const { useToken } = theme;

/**
 * Circular progress indicator for organization credit usage
 * Shows percentage used with yellow (used) and green (balance) colors
 * Displays in drawer header with animated updates and hover tooltip
 *
 * @param {Object} tokenData - credit balance data from store
 * @param {Function} onBuyTokens - Callback when "Buy Credits" button clicked
 * @return {JSX.Element} Circular progress display component
 */
const CircularTokenDisplay = ({ tokenData, onBuyTokens, isLoading }) => {
  const [percentage, setPercentage] = useState(0);
  const [usedTokens, setUsedTokens] = useState(0);
  const [balanceTokens, setBalanceTokens] = useState(0);
  const [totalTokens, setTotalTokens] = useState(0);

  // Get Ant Design theme token for dynamic theming
  const { token } = useToken();

  useEffect(() => {
    if (tokenData) {
      const { total_consumed, total_purchased, current_balance } = tokenData;
      const calculatedPercentage =
        total_purchased > 0
          ? Math.round((total_consumed / total_purchased) * 100)
          : 0;

      // Animate percentage change
      setPercentage(calculatedPercentage);
      setUsedTokens(total_consumed);
      setBalanceTokens(current_balance);
      setTotalTokens(total_purchased);
    }
  }, [tokenData]);

  // Pie chart configuration for tooltip
  const size = 80;
  const radius = size / 2;
  const centerX = radius;
  const centerY = radius;
  const angle = (percentage / 100) * 360;
  const radians = ((angle - 90) * Math.PI) / 180;
  // eslint-disable-next-line no-mixed-operators
  const endX = centerX + radius * Math.cos(radians);
  // eslint-disable-next-line no-mixed-operators
  const endY = centerY + radius * Math.sin(radians);
  const largeArcFlag = percentage > 50 ? 1 : 0;
  const piePath =
    percentage === 0
      ? ""
      : percentage === 100
      ? `M ${centerX},${centerY} m -${radius},0 a ${radius},${radius} 0 1,1 ${
          radius * 2
        },0 a ${radius},${radius} 0 1,1 -${radius * 2},0`
      : `M ${centerX},${centerY} L ${centerX},${
          centerY - radius
        } A ${radius},${radius} 0 ${largeArcFlag},1 ${endX},${endY} Z`;

  // Tooltip content with pie chart and credits breakdown
  // Only create tooltip if we have valid data
  const tooltipContent = tokenData ? (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        gap: "12px",
        padding: "4px",
      }}
    >
      {/* Pie Chart Circle */}
      <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`}>
        <defs>
          <clipPath id="tooltip-circle-clip">
            <circle cx={centerX} cy={centerY} r={radius} />
          </clipPath>
        </defs>
        {/* Background circle (green - remaining balance) */}
        <circle cx={centerX} cy={centerY} r={radius} fill="#52c41a" />
        {/* Used portion (yellow) - pie slice */}
        {percentage > 0 && (
          <path
            d={piePath}
            fill="#faad14"
            clipPath="url(#tooltip-circle-clip)"
          />
        )}
        {/* Percentage text in center */}
        <text
          x={centerX}
          y={centerY}
          textAnchor="middle"
          dominantBaseline="central"
          fill="#fff"
          fontSize="16"
          fontWeight="600"
        >
          {percentage}%
        </text>
      </svg>

      {/* Credits breakdown */}
      <Space direction="vertical" size={4} style={{ width: "100%" }}>
        <div style={{ fontSize: "12px", color: token.colorText }}>
          <span style={{ color: "#faad14", fontWeight: 600 }}>Used:</span>{" "}
          {(usedTokens || 0).toLocaleString()} credits ({percentage}%)
        </div>
        <div style={{ fontSize: "12px", color: token.colorText }}>
          <span style={{ color: "#52c41a", fontWeight: 600 }}>Balance:</span>{" "}
          {(balanceTokens || 0).toLocaleString()} credits ({100 - percentage}%)
        </div>
        <div style={{ fontSize: "12px", color: token.colorText }}>
          <span style={{ fontWeight: 600 }}>Total:</span>{" "}
          {(totalTokens || 0).toLocaleString()} credits
        </div>
      </Space>
    </div>
  ) : (
    "Loading credit balance..."
  );

  // Show loading spinner during initial fetch
  if (isLoading) {
    return (
      <Tooltip title="Loading credit balance..." placement="bottomRight">
        <div
          style={{
            display: "inline-flex",
            alignItems: "center",
            justifyContent: "center",
            width: "24px",
            height: "24px",
          }}
        >
          <Spin
            indicator={<LoadingOutlined style={{ fontSize: 16 }} spin />}
            size="small"
          />
        </div>
      </Tooltip>
    );
  }

  // Show "Out of Credits" message if balance is 0 or below
  if (balanceTokens <= 0) {
    const noCreditsTooltip = (
      <div style={{ color: token.colorText }}>
        Your credit balance has been depleted. Click to top-up and continue
        using Visitran AI.
      </div>
    );

    return (
      <Tooltip
        title={noCreditsTooltip}
        color={token.colorBgElevated}
        overlayInnerStyle={{
          border: `1px solid ${token.colorBorder}`,
          borderRadius: "8px",
        }}
      >
        <div
          style={{
            display: "inline-flex",
            alignItems: "center",
            gap: "6px",
            padding: "4px 12px",
            borderRadius: "100px",
            backgroundColor: token.colorErrorBg,
            border: `1px solid ${token.colorErrorBorder}`,
            cursor: "default",
            transition: "all 0.3s ease",
          }}
        >
          <WalletOutlined
            style={{ fontSize: "12px", color: token.colorError }}
          />
          <Typography.Text
            className="chat-ai-prompt-actions-monaco-font-size-10"
            style={{
              whiteSpace: "nowrap",
              color: token.colorError,
            }}
          >
            No Credits
          </Typography.Text>
        </div>
      </Tooltip>
    );
  }

  // Format credits remaining (use current_balance from tokenData)
  const creditsLeft = tokenData?.current_balance || 0;
  const formattedCredits = creditsLeft.toLocaleString();

  return (
    <Tooltip
      title={tooltipContent}
      placement="top"
      color={token.colorBgElevated}
      overlayInnerStyle={{
        border: `1px solid ${token.colorBorder}`,
        borderRadius: "8px",
      }}
    >
      <div
        style={{
          display: "inline-flex",
          alignItems: "center",
          gap: "6px",
          padding: "4px 12px",
          borderRadius: "100px", // Pill shape
          backgroundColor: token.colorBgContainer,
          border: `1px solid ${token.colorBorder}`,
          cursor: "pointer",
          transition: "all 0.3s ease",
        }}
      >
        <WalletOutlined style={{ fontSize: "12px", color: token.colorText }} />
        <Typography.Text
          className="chat-ai-prompt-actions-monaco-font-size-10"
          style={{
            whiteSpace: "nowrap",
          }}
        >
          {formattedCredits} credits left
        </Typography.Text>
      </div>
    </Tooltip>
  );
};

CircularTokenDisplay.propTypes = {
  tokenData: PropTypes.shape({
    total_consumed: PropTypes.number,
    total_purchased: PropTypes.number,
    current_balance: PropTypes.number,
    utilization_percentage: PropTypes.number,
  }),
  onBuyTokens: PropTypes.func,
  isLoading: PropTypes.bool,
};

CircularTokenDisplay.defaultProps = {
  tokenData: null,
  onBuyTokens: () => {},
  isLoading: false,
};

export default CircularTokenDisplay;
