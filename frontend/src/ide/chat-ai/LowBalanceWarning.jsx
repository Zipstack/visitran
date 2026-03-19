import { memo, useState, useEffect } from "react";
import PropTypes from "prop-types";
import { Alert, Button } from "antd";
import { ExclamationCircleOutlined, CloseOutlined } from "@ant-design/icons";
import "./LowBalanceWarning.css";

const LowBalanceWarning = memo(function LowBalanceWarning({
  tokenUsageData,
  style = {},
}) {
  const [isVisible, setIsVisible] = useState(false);
  const [balancePercentage, setBalancePercentage] = useState(0);

  // Auto-reset localStorage flag on component mount (page load/hard refresh)
  useEffect(() => {
    localStorage.removeItem("lowBalanceWarningDismissed");
  }, []);

  useEffect(() => {
    // Check localStorage flag on mount
    const warningDismissed = localStorage.getItem("lowBalanceWarningDismissed");

    if (!tokenUsageData) {
      return;
    }

    const { remaining_balance, total_consumed } = tokenUsageData;

    // Ensure remaining_balance is a valid number
    const balance =
      typeof remaining_balance === "number" ? remaining_balance : 0;
    const consumed = typeof total_consumed === "number" ? total_consumed : 0;

    // If balance is zero or negative, always show critical warning (can't be dismissed)
    if (balance <= 0 && consumed > 0) {
      // Only show zero balance warning if there was previous consumption (not a data loading issue)
      setBalancePercentage(0);
      setIsVisible(true);
      return;
    }

    // Calculate total balance (remaining + consumed) using validated values
    const total = balance + consumed;

    if (total > 0) {
      const percentage = (balance / total) * 100;
      const roundedPercentage = Math.round(percentage);
      setBalancePercentage(roundedPercentage);

      // Check if balance is 25% or less and remaining balance is greater than 0
      if (roundedPercentage <= 25 && balance > 0) {
        if (!warningDismissed) {
          setIsVisible(true);
        } else {
          setIsVisible(false);
        }
      } else {
        setIsVisible(false);
      }
    } else {
      // Don't show warning if we have no data (total is 0)
      setIsVisible(false);
    }
  }, [tokenUsageData]);

  const handleDismiss = () => {
    setIsVisible(false);
    localStorage.setItem("lowBalanceWarningDismissed", "true");
  };

  const handleReset = () => {
    localStorage.removeItem("lowBalanceWarningDismissed");
    window.location.reload();
  };

  if (!isVisible) return null;

  // Determine message and alert type based on balance
  const isZeroBalance = balancePercentage === 0;
  const alertType = isZeroBalance ? "error" : "warning";
  const message = isZeroBalance ? (
    <div className="warning-content">
      <div className="warning-text">
        <ExclamationCircleOutlined className="warning-icon" />
        <div>
          <strong>Out of Tokens</strong>
          <div style={{ marginTop: "4px" }}>
            AI features are temporarily unavailable because your token balance
            has been used up.
          </div>
          <div style={{ marginTop: "4px" }}>
            Please top up your tokens to continue using Visitran AI.
          </div>
        </div>
      </div>
    </div>
  ) : (
    <div className="warning-content">
      <div className="warning-text">
        <ExclamationCircleOutlined className="warning-icon" />
        <span>
          <strong>💡 Low Balance:</strong> {balancePercentage}% tokens
          remaining. Top up soon to avoid interruption.
        </span>
      </div>
      <div className="warning-actions">
        <Button
          size="small"
          type="link"
          onClick={handleReset}
          className="reset-button"
        >
          Show Warning Again
        </Button>
        <Button
          size="small"
          type="text"
          icon={<CloseOutlined />}
          onClick={handleDismiss}
          className="close-button"
        />
      </div>
    </div>
  );

  return (
    <div className="low-balance-warning" style={style}>
      <Alert message={message} type={alertType} showIcon={false} />
    </div>
  );
});

LowBalanceWarning.propTypes = {
  tokenUsageData: PropTypes.shape({
    remaining_balance: PropTypes.number,
    total_consumed: PropTypes.number,
  }),
  style: PropTypes.object,
};

LowBalanceWarning.displayName = "LowBalanceWarning";

export { LowBalanceWarning };
