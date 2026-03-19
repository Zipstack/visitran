import { SyncOutlined } from "@ant-design/icons";
import PropTypes from "prop-types";
import "./ThoughtChainEnhancements.css";

/**
 * RetryIndicator - Modern component to show when transformation is retrying
 * Displays when is_retry_transform flag is true from WebSocket
 *
 * @return {JSX.Element|null} Retry indicator component or null
 */
const RetryIndicator = ({ isRetrying, errorMessage }) => {
  if (!isRetrying) return null;

  return (
    <div className="retry-indicator fade-in">
      <div className="retry-indicator-icon">
        <SyncOutlined />
      </div>
      <div className="retry-indicator-content">
        <div className="retry-indicator-title">
          <span className="shimmer-text">
            🔄 Automatic Recovery in Progress
          </span>
        </div>
        <div className="retry-indicator-message">
          {errorMessage ? (
            <>
              Previous attempt encountered an error. Analyzing and retrying with
              corrections...
            </>
          ) : (
            <>Retrying transformation with corrections from AI...</>
          )}
        </div>
      </div>
    </div>
  );
};

RetryIndicator.propTypes = {
  isRetrying: PropTypes.bool,
  errorMessage: PropTypes.string,
};

export default RetryIndicator;
