import { useState, useEffect } from "react";
import PropTypes from "prop-types";
import { Button, Tooltip } from "antd";
import { BarChartOutlined } from "@ant-design/icons";

/**
 * TokenUsageDisplay component that only shows token usage without feedback
 * @param {Object} props - Component props
 * @param {Object} props.tokenUsageData - Token usage data from socket
 * @param {Object} props.style - Additional styles
 * @return {JSX.Element} The TokenUsageDisplay component
 */
const TokenUsageDisplay = ({ tokenUsageData, style = {} }) => {
  // Real token stats state
  const [tokenStats, setTokenStats] = useState({
    tokensUsed: 0,
    tokensRemaining: 0,
    messageTokensConsumed: 0,
  });

  // Token display state
  const [showStats, setShowStats] = useState(false);
  const [animatedTokens, setAnimatedTokens] = useState(0);
  const [animatedBalance, setAnimatedBalance] = useState(0);

  // Listen for token usage data updates from props
  useEffect(() => {
    if (tokenUsageData) {
      const { remaining_balance, message_tokens_consumed } = tokenUsageData;
      const newTokenStats = {
        tokensUsed: message_tokens_consumed || 0,
        tokensRemaining: remaining_balance || 0,
        messageTokensConsumed: message_tokens_consumed || 0,
      };

      setTokenStats(newTokenStats);
    }
  }, [tokenUsageData]);

  // Toggle stats visibility with animation
  const toggleStats = () => {
    setShowStats(!showStats);

    if (!showStats) {
      // Reset counters
      setAnimatedTokens(0);
      setAnimatedBalance(0);

      // Start animations
      let tokenCounter = 0;
      let balanceCounter = 0;
      const tokenIncrement = Math.ceil(tokenStats.tokensUsed / 20);
      const balanceIncrement = Math.ceil(tokenStats.tokensRemaining / 20);

      const intervalId = setInterval(() => {
        tokenCounter = Math.min(
          tokenCounter + tokenIncrement,
          tokenStats.tokensUsed
        );
        balanceCounter = Math.min(
          balanceCounter + balanceIncrement,
          tokenStats.tokensRemaining
        );

        setAnimatedTokens(tokenCounter);
        setAnimatedBalance(balanceCounter);

        if (
          tokenCounter >= tokenStats.tokensUsed &&
          balanceCounter >= tokenStats.tokensRemaining
        ) {
          clearInterval(intervalId);
        }
      }, 40);
    }
  };

  return (
    <div
      style={{
        display: "inline-flex",
        alignItems: "center",
        height: "32px",
        ...style,
      }}
    >
      <div
        style={{
          display: "flex",
          gap: "4px",
          alignItems: "center",
          position: "relative",
        }}
      >
        {showStats && (
          <span
            className="token-display"
            style={{
              fontSize: "13px",
              whiteSpace: "nowrap",
              opacity: 1,
              transition: "opacity 0.3s ease-in",
              color: "var(--ant-primary-color, #1890ff)",
              position: "absolute",
              right: "100%",
              marginRight: "8px",
            }}
          >
            Tokens used: {animatedTokens} | Balance:{" "}
            {animatedBalance.toLocaleString()}
          </span>
        )}
        <Tooltip title="Credit Usage">
          <Button
            size="small"
            type="text"
            icon={<BarChartOutlined />}
            onClick={toggleStats}
          />
        </Tooltip>
      </div>
    </div>
  );
};

TokenUsageDisplay.propTypes = {
  tokenUsageData: PropTypes.shape({
    remaining_balance: PropTypes.number.isRequired,
    message_tokens_consumed: PropTypes.number.isRequired,
    total_consumed: PropTypes.number.isRequired,
  }),
  style: PropTypes.object,
};

export default TokenUsageDisplay;
