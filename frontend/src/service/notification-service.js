import {
  createContext,
  useContext,
  useEffect,
  useRef,
  useCallback,
} from "react";
import PropTypes from "prop-types";
import { notification, Button } from "antd";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import "./notification-border.css";
import { useSessionStore } from "../store/session-store";

const NotificationContext = createContext(null);

function NotificationProvider({ children }) {
  const [api, contextHolder] = notification.useNotification();
  const apiRef = useRef(api);

  // Keep the latest api instance
  useEffect(() => {
    apiRef.current = api;
  }, [api]);

  // Close a specific notification by key, or all if no key passed
  const closeAll = useCallback((key) => {
    if (key) {
      apiRef.current.destroy(key);
    } else {
      apiRef.current.destroy();
    }
  }, []);

  // Notify helper
  const notify = useCallback(
    ({
      type: explicitType,
      message = "",
      description = "",
      duration: customDuration,
      error = null,
      renderMarkdown = true,
    }) => {
      // Determine notification type
      const isErrorContext = !explicitType && error;
      let type = isErrorContext ? "error" : explicitType ?? "info";

      // Prepare final message & description
      let finalMessage = message;
      let finalDescription = description;

      if (type === "error") {
        // Skip all notifications when session expired modal is showing
        if (
          error?.response?.status === 401 ||
          useSessionStore.getState().showSessionExpiredModal
        ) {
          return;
        }

        const errorData = error?.response?.data;
        if (errorData && typeof errorData.error_message === "string") {
          const text = errorData.error_message;
          if (errorData.is_markdown) {
            finalMessage = "";
            const formatted = text.replace(/\n/g, "  \n");
            finalDescription = formatted;
            type = errorData?.severity?.toLowerCase() || "error";
          } else {
            finalMessage = "Failed";
            finalDescription = text;
          }
        } else {
          finalMessage = message || "Failed";
          finalDescription = description || "Something went wrong";
        }
        // Ensure description is never empty
        if (!finalDescription) {
          finalDescription = "Something went wrong";
        }
      }

      // Build description content
      const contentDescription = renderMarkdown ? (
        <div className="notification-markdown-content">
          <ReactMarkdown remarkPlugins={[remarkGfm]}>
            {typeof finalDescription === "object"
              ? JSON.stringify(finalDescription)
              : finalDescription}
          </ReactMarkdown>
        </div>
      ) : (
        finalDescription
      );

      // Common settings
      const key = `notif_${Date.now()}`;
      const duration = customDuration ?? (type === "success" ? 6 : 0);
      const btn = (
        <Button type="link" size="small" onClick={() => closeAll()}>
          Close All
        </Button>
      );

      // Show notification
      apiRef.current.open({
        key,
        type,
        message: finalMessage,
        description: contentDescription,
        btn,
        duration,
      });
    },
    [closeAll]
  );

  return (
    <NotificationContext.Provider value={{ notify, closeAll }}>
      {contextHolder}
      {children}
    </NotificationContext.Provider>
  );
}

NotificationProvider.propTypes = {
  children: PropTypes.node.isRequired,
};

function useNotificationService() {
  const context = useContext(NotificationContext);
  if (!context) {
    throw new Error(
      "useNotificationService must be used inside a NotificationProvider"
    );
  }
  return context;
}

export { NotificationProvider, useNotificationService };
