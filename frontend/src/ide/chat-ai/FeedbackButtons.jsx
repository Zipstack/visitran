import { useState } from "react";
import PropTypes from "prop-types";
import { Button, Tooltip, message } from "antd";
import {
  LikeOutlined,
  DislikeOutlined,
  LikeFilled,
  DislikeFilled,
} from "@ant-design/icons";
import axios from "axios";

/**
 * FeedbackButtons component that handles feedback functionality
 * @param {Object} props - Component props
 * @param {string} props.chatMessageId - The chat message ID
 * @param {Object} props.style - Additional styles
 * @return {JSX.Element} The FeedbackButtons component
 */
const FeedbackButtons = ({ chatMessageId, style = {} }) => {
  const [feedback, setFeedback] = useState(null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [hasFeedback, setHasFeedback] = useState(false);
  const [saveAnimationThumbUp, setSaveAnimationThumbUp] = useState(false);
  const [saveAnimationThumbDown, setSaveAnimationThumbDown] = useState(false);

  // Get organization ID, project ID and chat ID directly from the URL or storage
  const getIdsForFeedback = () => {
    // Get organization ID with org_ prefix for header and URL
    let orgId;

    // First check the newer format
    try {
      const orgidJson = localStorage.getItem("orgid");
      if (orgidJson) {
        const parsedOrgId = JSON.parse(orgidJson);
        if (parsedOrgId?.state?.selectedOrgId) {
          orgId = parsedOrgId.state.selectedOrgId;
        }
      }
    } catch (e) {
      console.error("Error parsing orgid from localStorage:", e);
    }

    // If not found, try other storage locations
    if (!orgId) {
      orgId =
        localStorage.getItem("currentOrganizationId") ||
        localStorage.getItem("organizationId") ||
        "default_org";
    }

    // Get project ID from URL or localStorage
    const pathParts = window.location.pathname.split("/");
    const projectIdIndex = pathParts.findIndex((part) => part === "project");
    const projectId =
      projectIdIndex !== -1 && pathParts[projectIdIndex + 1]
        ? pathParts[projectIdIndex + 1]
        : localStorage.getItem("currentProjectId") ||
          localStorage.getItem("projectId") ||
          "default_project";

    // Get chat ID from URL or localStorage
    const chatIdIndex = pathParts.findIndex((part) => part === "chat");
    const chatId =
      chatIdIndex !== -1 && pathParts[chatIdIndex + 1]
        ? pathParts[chatIdIndex + 1]
        : localStorage.getItem("currentChatId") ||
          localStorage.getItem("chatId") ||
          "default_chat";

    return { orgId, projectId, chatId };
  };

  const handleFeedbackSubmit = async (isPositive) => {
    if (isSubmitting) return;

    // Check for valid chatMessageId
    if (!chatMessageId) {
      setIsSubmitting(false);
      message.error("Unable to submit feedback: Message ID is missing");
      return;
    }

    const handleFeedback = async (isPositive) => {
      // Prevent duplicate submissions
      if (isSubmitting) return;

      // Determine feedback value: 'P' for positive, 'N' for negative, '0' for neutral
      const feedbackValue =
        isPositive === true ? "P" : isPositive === false ? "N" : "0";

      // If same feedback clicked again and no prior submission, treat as toggle
      if (
        (feedback === true && isPositive === true) ||
        (feedback === false && isPositive === false)
      ) {
        if (!hasFeedback) {
          setFeedback(null);
          return;
        }
      }

      // Get current feedback value for comparison
      const currentFeedback =
        feedback === true ? "P" : feedback === false ? "N" : "0";
      if (currentFeedback === feedbackValue && hasFeedback) return;

      setIsSubmitting(true);

      try {
        // Get IDs needed for API call
        const { orgId, projectId, chatId } = getIdsForFeedback();

        // Construct the direct API URL - organization ID WITH org_ prefix in URL path
        const apiUrl = `/api/v1/visitran/${orgId}/project/${projectId}/chat/${chatId}/chat-message/${chatMessageId}/feedback/`;

        // Get CSRF token from cookie if available
        const csrftoken = document.cookie
          .split(";")
          .find((c) => c.trim().startsWith("csrftoken="))
          ?.split("=")?.[1];

        // Get auth token if available
        const authToken =
          localStorage.getItem("token") || sessionStorage.getItem("token");

        await axios.post(
          apiUrl,
          {
            feedback: feedbackValue, // Using format: '0', 'P', or 'N'
          },
          {
            headers: {
              "X-Organization": orgId, // Organization ID with prefix in header
              "X-CSRFToken": csrftoken, // Django CSRF token
              ...(authToken && { Authorization: `Bearer ${authToken}` }), // Auth token if available
              "Content-Type": "application/json",
            },
            withCredentials: true, // Include cookies in the request
          }
        );

        // Update local state - convert to boolean for UI rendering
        const newFeedback =
          feedbackValue === "P" ? true : feedbackValue === "N" ? false : null;
        setFeedback(newFeedback);
        setHasFeedback(true);

        // Trigger save animation
        if (isPositive) {
          setSaveAnimationThumbUp(true);
          setTimeout(() => setSaveAnimationThumbUp(false), 1000);
        } else {
          setSaveAnimationThumbDown(true);
          setTimeout(() => setSaveAnimationThumbDown(false), 1000);
        }
      } catch (error) {
        console.error("Error submitting feedback:", error);
        message.error(error.message || "Failed to submit feedback");
      } finally {
        setIsSubmitting(false);
      }
    };

    handleFeedback(isPositive);
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
        }}
      >
        <Tooltip title="This response was helpful">
          <Button
            size="small"
            type={feedback === true ? "primary" : "text"}
            icon={
              feedback === true ? (
                <LikeFilled
                  className={saveAnimationThumbUp ? "save-animation" : ""}
                />
              ) : (
                <LikeOutlined />
              )
            }
            onClick={() => handleFeedbackSubmit(true)}
            loading={isSubmitting}
            disabled={isSubmitting || feedback === true} // Disable when selected
            className={saveAnimationThumbUp ? "button-pulse" : ""}
          />
        </Tooltip>

        <Tooltip title="This response could be improved">
          <Button
            size="small"
            type={feedback === false ? "primary" : "text"}
            icon={
              feedback === false ? (
                <DislikeFilled
                  className={saveAnimationThumbDown ? "save-animation" : ""}
                />
              ) : (
                <DislikeOutlined />
              )
            }
            onClick={() => handleFeedbackSubmit(false)}
            loading={isSubmitting}
            disabled={isSubmitting || feedback === false} // Disable when selected
            className={saveAnimationThumbDown ? "button-pulse" : ""}
          />
        </Tooltip>
      </div>
    </div>
  );
};

FeedbackButtons.propTypes = {
  chatMessageId: PropTypes.string.isRequired,
  style: PropTypes.object,
};

export default FeedbackButtons;
