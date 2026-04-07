import { memo, useState, useMemo } from "react";
import PropTypes from "prop-types";
import { Typography, Popover, Button, notification, Collapse } from "antd";
import {
  DatabaseOutlined,
  BulbOutlined,
  CodeOutlined,
  SafetyCertificateOutlined,
  PlayCircleOutlined,
  InfoCircleOutlined,
  CheckCircleFilled,
  LoadingOutlined,
  SyncOutlined,
  CopyOutlined,
  RightOutlined,
} from "@ant-design/icons";

import "./ThoughtChainEnhancements.css";

const { Text } = Typography;

/**
 * PromptInfo - Modern thought chain with shimmer effects and info icons
 * Detects message patterns and applies appropriate styling
 */
const PromptInfo = memo(function PromptInfo({
  shouldStream,
  thoughtChain = [],
  errorState,
  errorDetails = {},
}) {
  const [openPopoverId, setOpenPopoverId] = useState(null);
  const [isExpanded, setIsExpanded] = useState(false);

  // Pre-process thought chain - memoized to prevent re-computation on every render
  const processedChain = useMemo(() => {
    if (!thoughtChain || thoughtChain.length === 0) return [];

    const result = [];
    for (let i = 0; i < thoughtChain.length; i++) {
      const currentMsg = thoughtChain[i];
      const nextMsg = thoughtChain[i + 1];

      // Check if current message is an "Attempt" message
      const currentStr =
        typeof currentMsg === "string"
          ? currentMsg
          : currentMsg?.display || String(currentMsg);
      const isAttemptMsg = currentStr.match(
        /attempt\s+\d+.*disapproved.*retrying/i
      );

      if (isAttemptMsg && nextMsg) {
        const nextStr =
          typeof nextMsg === "string"
            ? nextMsg
            : nextMsg?.display || String(nextMsg);
        const isDisapprovalReason =
          nextStr.match(/\[DISAPPROVE\s+REASON\]/i) ||
          nextStr.match(/^[^:]+:\s*.+/);

        if (isDisapprovalReason) {
          // Extract the disapproval reason (remove the prefix)
          const reasonText = nextStr
            .replace(/^\[DISAPPROVE\s+REASON\]:\s*/i, "")
            .trim();

          // Create enhanced attempt message with embedded error details
          const enhancedMsg = {
            display: currentStr,
            error_details: reasonText,
            error_summary:
              reasonText.length > 150
                ? reasonText.substring(0, 150) + "..."
                : reasonText,
            retry_message: "Fixing the issues and regenerating...",
            attempt: currentStr.match(/attempt\s+(\d+)/i)?.[1] || 1,
            source: "Agent Visitran Critic",
          };

          result.push(enhancedMsg);
          i++; // Skip the next message since we've incorporated it
          continue;
        }
      }

      result.push(currentMsg);
    }
    return result;
  }, [thoughtChain]);

  // Copy error details to clipboard
  const copyErrorToClipboard = (
    message,
    attemptNumber,
    errorStage,
    parsedErrorDetails = null
  ) => {
    const timestamp = new Date().toLocaleString();
    const { transformation_error_message, prompt_error_message } = errorDetails;

    // Get the most relevant error message from multiple sources
    let detailedError = null;
    if (parsedErrorDetails) {
      // Use error from structured message (visitran-ai agent errors)
      detailedError =
        typeof parsedErrorDetails === "string"
          ? parsedErrorDetails
          : JSON.stringify(parsedErrorDetails, null, 2);
    } else if (transformation_error_message) {
      // Use transformation error from backend
      detailedError =
        typeof transformation_error_message === "string"
          ? transformation_error_message
          : JSON.stringify(transformation_error_message, null, 2);
    } else if (prompt_error_message) {
      // Use prompt error from backend
      detailedError =
        typeof prompt_error_message === "string"
          ? prompt_error_message
          : JSON.stringify(prompt_error_message, null, 2);
    }

    const errorText = `
Attempt ${attemptNumber || 1} - Error Details
========================================
Timestamp: ${timestamp}
Error Stage: ${errorStage || "Unknown"}
Message: ${message}
${detailedError ? `\nDetailed Error:\n${detailedError}` : ""}
    `.trim();

    navigator.clipboard
      .writeText(errorText)
      .then(() => {
        notification.success({
          message: "Copied to clipboard",
          description: "Error details have been copied successfully",
          placement: "topRight",
          duration: 2,
        });
      })
      .catch((err) => {
        notification.error({
          message: "Copy failed",
          description: "Could not copy to clipboard",
          placement: "topRight",
          duration: 2,
        });
      });
  };

  // Detect message type and extract metadata
  const parseMessage = (msg) => {
    // Handle structured messages from visitran-ai (agent disapprovals)
    let message;
    let errorDetails = null;
    let attemptNumber = null;
    let source = null;

    let errorSummary = null;
    let retryMessage = null;

    if (typeof msg === "object" && msg !== null && msg.display) {
      // Structured message with error details
      message = msg.display;
      errorDetails = msg.error_details;
      errorSummary = msg.error_summary; // Concise summary
      retryMessage = msg.retry_message; // Dynamic retry message
      attemptNumber = msg.attempt;
      source = msg.source;
    } else {
      // Plain string message
      message = String(msg);
    }

    // Detect stage based on keywords
    let stage = "general";
    let icon = null;

    if (
      message.toLowerCase().includes("preparing") ||
      message.toLowerCase().includes("database")
    ) {
      stage = "preparation";
      icon = <DatabaseOutlined />;
    } else if (message.toLowerCase().includes("planning")) {
      stage = "planning";
      icon = <BulbOutlined />;
    } else if (
      message.toLowerCase().includes("creating") ||
      message.toLowerCase().includes("generating")
    ) {
      stage = "generation";
      icon = <CodeOutlined />;
    } else if (message.toLowerCase().includes("validat")) {
      stage = "validation";
      icon = <SafetyCertificateOutlined />;
    } else if (
      message.toLowerCase().includes("execut") ||
      message.toLowerCase().includes("running")
    ) {
      stage = "execution";
      icon = <PlayCircleOutlined />;
    }

    // Detect message type
    const isError =
      message.includes("failed") ||
      message.includes("error") ||
      message.match(/❌|⚠️/) ||
      message.toLowerCase().includes("disapproved");
    const isSuccess =
      message.includes("✓") ||
      message.includes("successfully") ||
      message.includes("created");
    const isRetry =
      message.includes("retry") ||
      message.includes("retrying") ||
      message.match(/🔄|attempt/i);
    const isProgress = message.match(/\(\d+\s+of\s+\d+\)/);

    // Extract attempt number if not already from structured message
    if (!attemptNumber) {
      const attemptMatch = message.match(/attempt\s+(\d+)/i);
      attemptNumber = attemptMatch ? parseInt(attemptMatch[1]) : null;
    }

    return {
      original: message,
      stage,
      icon,
      isError,
      isSuccess,
      isRetry,
      isProgress,
      attemptNumber,
      errorDetails, // Full error details from structured messages
      errorSummary, // Concise summary for quick view
      retryMessage, // Dynamic retry message
      source, // Source from structured messages
    };
  };

  // Create error popover content
  const createErrorPopover = (
    message,
    attemptNumber,
    parsedErrorDetails = null,
    errorSummary = null,
    retryMsg = null,
    source = null
  ) => {
    const { transformation_error_message, prompt_error_message } = errorDetails;

    // Get detailed error from multiple sources:
    // 1. From structured message (agent disapprovals from visitran-ai)
    // 2. From chat message error fields (transformation errors from backend)
    let detailedError = null;

    if (parsedErrorDetails) {
      // Use error details from structured message (visitran-ai agent errors)
      detailedError =
        typeof parsedErrorDetails === "string"
          ? parsedErrorDetails
          : JSON.stringify(parsedErrorDetails, null, 2);
    } else if (transformation_error_message) {
      // Use transformation error from backend
      detailedError =
        typeof transformation_error_message === "string"
          ? transformation_error_message
          : JSON.stringify(transformation_error_message, null, 2);
    } else if (prompt_error_message) {
      // Use prompt error from backend
      detailedError =
        typeof prompt_error_message === "string"
          ? prompt_error_message
          : JSON.stringify(prompt_error_message, null, 2);
    }

    // Use concise summary if available, otherwise use full error
    const displayError = errorSummary || detailedError;

    return (
      <div className="error-popover-content">
        {/* Removed error-popover-header with red X icon */}
        <div style={{ marginBottom: "16px" }}>
          <div style={{ fontWeight: 600, fontSize: "14px", color: "#262626" }}>
            Attempt {attemptNumber || 1} Failed
          </div>
        </div>

        {displayError && (
          <div className="error-popover-section">
            <div
              style={{
                display: "flex",
                justifyContent: "space-between",
                alignItems: "center",
                marginBottom: "8px",
              }}
            >
              <div className="error-popover-label">
                {errorSummary ? "Issues Identified" : "Detailed Error"}
              </div>
              {/* Copy button moved here */}
              <Button
                type="text"
                size="small"
                icon={<CopyOutlined />}
                onClick={() =>
                  copyErrorToClipboard(
                    message,
                    attemptNumber,
                    source || errorState,
                    parsedErrorDetails
                  )
                }
                style={{ padding: "4px 8px" }}
                title="Copy Error"
              />
            </div>
            <div
              className="error-popover-value error-popover-code"
              style={{ whiteSpace: "pre-wrap" }}
            >
              {displayError}
            </div>
            {/* Commented out Show Full Details button
            {hasFullDetails && (
              <Button
                type="link"
                size="small"
                onClick={() => setShowFullError(prev => ({ ...prev, [popoverId]: !prev[popoverId] }))}
                style={{ padding: '4px 0', marginTop: '8px' }}
              >
                {showFullError[popoverId] ? 'Show Less' : 'Show Full Details'}
              </Button>
            )}
            */}
          </div>
        )}

        {/* Dynamic retry message without title */}
        {retryMsg && (
          <div
            className="error-popover-section"
            style={{ borderTop: "1px solid #f0f0f0", paddingTop: "12px" }}
          >
            <div
              className="error-popover-value"
              style={{ fontStyle: "italic", color: "#595959" }}
            >
              {retryMsg}
            </div>
          </div>
        )}
      </div>
    );
  };

  // Render a single thought chain message
  const renderMessage = (msg, index, totalCount, isLatestMessage = false) => {
    const parsed = parseMessage(msg);
    const isInProgress = shouldStream && isLatestMessage;

    // Determine item class
    let itemClass = "thought-chain-item";
    if (isInProgress || parsed.isProgress) {
      itemClass += " in-progress";
    } else if (parsed.isSuccess) {
      itemClass += " success";
    } else if (parsed.isError) {
      itemClass += " error";
    } else if (parsed.isRetry) {
      itemClass += " warning";
    }

    return (
      <div key={`${msg}-${index}`} className={itemClass}>
        {parsed.icon && (
          <div className={`stage-icon ${parsed.stage}`}>
            {isInProgress ? <LoadingOutlined /> : parsed.icon}
          </div>
        )}

        <div className="thought-chain-message">
          {isInProgress || (parsed.isProgress && shouldStream) ? (
            <Text className="shimmer-text">{parsed.original}</Text>
          ) : parsed.isError ? (
            <Text type="warning">
              {parsed.original}
              {parsed.attemptNumber && (
                <Popover
                  content={createErrorPopover(
                    parsed.original,
                    parsed.attemptNumber,
                    parsed.errorDetails,
                    parsed.errorSummary,
                    parsed.retryMessage,
                    parsed.source
                  )}
                  title={null}
                  trigger="click"
                  open={openPopoverId === `error-${index}`}
                  onOpenChange={(visible) =>
                    setOpenPopoverId(visible ? `error-${index}` : null)
                  }
                  overlayStyle={{ maxWidth: 500 }}
                >
                  <span
                    className="error-info-icon"
                    onClick={(e) => {
                      e.stopPropagation();
                      setOpenPopoverId(`error-${index}`);
                    }}
                  >
                    <InfoCircleOutlined />
                  </span>
                </Popover>
              )}
            </Text>
          ) : parsed.isSuccess ? (
            <Text type="success">
              <CheckCircleFilled
                style={{ marginRight: 6 }}
                className="success-checkmark"
              />
              {parsed.original}
            </Text>
          ) : parsed.isRetry ? (
            <Text type="warning">
              <SyncOutlined style={{ marginRight: 6 }} />
              {parsed.original}
            </Text>
          ) : (
            <Text type="secondary">{parsed.original}</Text>
          )}
        </div>
      </div>
    );
  };

  // Render thought chain
  const renderThoughtChain = () => {
    if (processedChain.length === 0) return null;
    const latestMessage = processedChain[processedChain.length - 1];
    const previousMessages = processedChain.slice(0, -1);
    const previousCount = previousMessages.length;

    // Render the latest message for the collapse header
    const latestParsed = parseMessage(latestMessage);
    const isInProgress = shouldStream;

    const collapseLabel = (
      <div className="thought-chain-collapse-header">
        <div className="thought-chain-collapse-content">
          {latestParsed.icon && (
            <span className={`stage-icon ${latestParsed.stage}`}>
              {isInProgress && !isExpanded ? (
                <LoadingOutlined />
              ) : (
                latestParsed.icon
              )}
            </span>
          )}
          <span className="thought-chain-collapse-message">
            {isInProgress && !isExpanded ? (
              <Text className="shimmer-text">{latestParsed.original}</Text>
            ) : latestParsed.isError ? (
              <Text type="warning">
                {latestParsed.original}
                {latestParsed.attemptNumber && (
                  <Popover
                    content={createErrorPopover(
                      latestParsed.original,
                      latestParsed.attemptNumber,
                      latestParsed.errorDetails,
                      latestParsed.errorSummary,
                      latestParsed.retryMessage,
                      latestParsed.source
                    )}
                    title={null}
                    trigger="click"
                    open={openPopoverId === "error-latest"}
                    onOpenChange={(visible) =>
                      setOpenPopoverId(visible ? "error-latest" : null)
                    }
                    overlayStyle={{ maxWidth: 500 }}
                  >
                    <span
                      className="error-info-icon"
                      onClick={(e) => {
                        e.stopPropagation();
                        setOpenPopoverId("error-latest");
                      }}
                    >
                      <InfoCircleOutlined />
                    </span>
                  </Popover>
                )}
              </Text>
            ) : latestParsed.isSuccess ? (
              <Text type="success">
                <CheckCircleFilled
                  style={{ marginRight: 6 }}
                  className="success-checkmark"
                />
                {latestParsed.original}
              </Text>
            ) : latestParsed.isRetry ? (
              <Text type="warning">
                <SyncOutlined style={{ marginRight: 6 }} />
                {latestParsed.original}
              </Text>
            ) : (
              <Text type="secondary">{latestParsed.original}</Text>
            )}
          </span>
        </div>
        {!isExpanded && previousCount > 0 && (
          <Text type="secondary" className="thought-chain-collapse-count">
            ({previousCount} previous {previousCount === 1 ? "step" : "steps"})
          </Text>
        )}
      </div>
    );

    return (
      <div className="modern-thought-chain collapsible">
        <Collapse
          activeKey={isExpanded ? ["1"] : []}
          onChange={() => setIsExpanded(!isExpanded)}
          expandIcon={({ isActive }) => (
            <RightOutlined rotate={isActive ? 90 : 0} />
          )}
          className="thought-chain-collapse"
          items={[
            {
              key: "1",
              label: collapseLabel,
              children: (
                <div className="thought-chain-timeline">
                  {processedChain.map((msg, index) =>
                    renderMessage(
                      msg,
                      index,
                      processedChain.length,
                      index === processedChain.length - 1
                    )
                  )}
                </div>
              ),
            },
          ]}
        />
      </div>
    );
  };

  // Show thought chain if we have messages
  if (thoughtChain && thoughtChain.length > 0) {
    return renderThoughtChain();
  }

  // Only show loading state when actively streaming
  if (!shouldStream) return null;

  // Fallback to bubble with shimmer
  return (
    <div style={{ padding: "8px 0" }}>
      <Text className="shimmer-text">Processing...</Text>
    </div>
  );
});

PromptInfo.propTypes = {
  shouldStream: PropTypes.bool,
  thoughtChain: PropTypes.array,
  errorState: PropTypes.string,
  errorDetails: PropTypes.shape({
    transformation_error_message: PropTypes.oneOfType([
      PropTypes.string,
      PropTypes.object,
    ]),
    prompt_error_message: PropTypes.oneOfType([
      PropTypes.string,
      PropTypes.object,
    ]),
  }),
};

PromptInfo.displayName = "PromptInfo";

export { PromptInfo };
