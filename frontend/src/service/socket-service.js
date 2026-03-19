import { useCallback, useEffect, useRef, useState } from "react";
import { io } from "socket.io-client";

import { useSocketMessagesStore } from "../store/socket-messages-store";
import { useSessionStore } from "../store/session-store";
import { useProjectStore } from "../store/project-store";
import { orgStore } from "../store/org-store";
import { getBaseUrl } from "../common/helpers";
import { useSocketChannelStore } from "../store/socket-channel-store";
import { useTokenStore } from "../store/token-store";

// Socket server URL — use the env var if provided, otherwise derive from window.location
const SOCKET_SERVER_URL =
  process.env.REACT_APP_SOCKET_SERVICE_BASE_URL || getBaseUrl();

// When connecting to a dedicated socket server (env var set), use default /socket.io/ path.
// When going through nginx (production), use the custom proxy path.
const SOCKET_PATH = process.env.REACT_APP_SOCKET_SERVICE_BASE_URL
  ? "/socket.io/"
  : "/api/v1/socket";

// Custom hook for managing socket connections with a 500ms throttle on incoming messages.
export function useSocketService() {
  const [isConnected, setIsConnected] = useState(false);
  const socketRef = useRef(null);

  const { projectId } = useProjectStore();
  const { selectedOrgId } = orgStore();
  const sessionDetails = useSessionStore.getState().sessionDetails;
  const orgId = selectedOrgId || "default_org";

  // Store incoming messages and flush them in batches
  const messageQueueRef = useRef([]);
  const timerRef = useRef(null);

  const { pushMessagesBatch } = useSocketMessagesStore();

  // channel persistence
  const { activeChannels, activeChannelMeta, addChannel, clear } =
    useSocketChannelStore();

  const flushQueue = useCallback(() => {
    if (messageQueueRef.current.length > 0) {
      pushMessagesBatch(messageQueueRef.current);
      messageQueueRef.current = [];
    }
    timerRef.current = null;
  }, [pushMessagesBatch]);

  // Queues a message to be sent to the store, starts a timer if not already running
  const addThrottledMessage = useCallback(
    (msg) => {
      messageQueueRef.current.push(msg);
      if (!timerRef.current) {
        timerRef.current = setTimeout(flushQueue, 500);
      }
    },
    [flushQueue]
  );

  // Called when a new message arrives; adds it to the queue for batched processing
  const handleSocketData = useCallback(
    (data) => {
      // Update token balance in store if token_usage_data is present
      if (data.token_usage_data) {
        useTokenStore.getState().updateFromWebSocket(data);
      }

      addThrottledMessage(data);
    },
    [addThrottledMessage]
  );

  // Connects to the socket server if not already connected
  const connectSocket = useCallback(
    (chatId) => {
      if (!chatId) {
        console.error("Chat ID is required to establish a connection.");
        return;
      }
      if (socketRef.current && isConnected) return;

      // if (!sessionDetails?.user_id || !sessionDetails?.organization_id) return;
      const socketOptions = {
        transports: ["websocket", "polling"], // Fallback to polling if WebSocket fails
        query: {
          chatId,
          projectId,
          ...(sessionDetails?.user_id && { user_id: sessionDetails.user_id }),
          ...(sessionDetails?.organization_id && {
            organization_id: sessionDetails.organization_id,
          }),
          ...(sessionDetails?.email && { email: sessionDetails.email }),
          ...(sessionDetails?.user_role && {
            user_role: sessionDetails.user_role,
          }),
          env: process.env.REACT_APP_ENV,
        },
        // Reconnection configuration - critical for auto-recovery
        reconnection: true, // Enable auto-reconnect
        reconnectionAttempts: 10, // Try 10 times before giving up
        reconnectionDelay: 1000, // Start with 1 second delay
        reconnectionDelayMax: 5000, // Max 5 seconds between attempts
        randomizationFactor: 0.5, // Add jitter to prevent thundering herd
        // Connection timeouts
        timeout: 20000, // Connection timeout 20 seconds
        // Auto-connect behavior
        autoConnect: true, // Connect immediately when created
      };
      socketOptions.path = SOCKET_PATH;

      socketRef.current = io(SOCKET_SERVER_URL, socketOptions);

      // Connection successful event
      socketRef.current?.on("connect", () => {
        setIsConnected(true);
        // resume existing channels
        activeChannelMeta.forEach((meta, channelId) => {
          socketRef.current?.emit(
            "subscribe_channel",
            {
              channelId,
              chatId: meta.chatId,
              chatMessageId: meta.chatMessageId,
              projectId,
              orgId,
              sessionDetails,
            },
            () => {}
          );
          socketRef.current?.off(channelId);
          socketRef.current?.on(channelId, handleSocketData);
        });
      });

      // Disconnection event - critical for detecting connection loss
      socketRef.current?.on("disconnect", (reason) => {
        console.warn("[Socket] Disconnected. Reason:", reason);
        setIsConnected(false);

        // Reasons:
        // - "io server disconnect": server kicked us out (manual disconnect)
        // - "io client disconnect": we called disconnect()
        // - "ping timeout": no pong received (dead connection)
        // - "transport close": connection lost
        // - "transport error": network error

        if (reason === "io server disconnect") {
          // Server kicked us out, must reconnect manually
          socketRef.current?.connect();
        }
        // For other reasons, Socket.IO will auto-reconnect based on reconnection config
      });

      // Reconnection attempt event - shows progress
      socketRef.current?.on("reconnect_attempt", (_attemptNumber) => {});

      // Reconnection successful event
      socketRef.current?.on("reconnect", (_attemptNumber) => {
        setIsConnected(true);
      });

      // Reconnection failed event - all attempts exhausted
      socketRef.current?.on("reconnect_failed", () => {
        console.error("[Socket] Reconnection failed after all attempts");
        setIsConnected(false);
        // Could add user notification here
        // alert("Connection to server lost. Please refresh the page.");
      });

      // Connection error event - happens during connection attempts
      socketRef.current?.on("connect_error", (error) => {
        console.error("[Socket] Connection error:", error.message);
        // Socket.IO will automatically retry with exponential backoff
      });

      socketRef.current?.on("subscribe_ack", (data) => {});

      socketRef.current?.on("error", (err) => {
        console.error("[Socket] Error:", err);
      });
    },
    [isConnected, projectId, activeChannelMeta]
  );

  const createChannel = useCallback(
    (chatId, chatMessageId) => {
      if (!socketRef.current) {
        console.error("Cannot create channel; socket is not connected.");
        return;
      }
      if (!chatId || !chatMessageId) {
        console.error(
          "chatId and chatMessageId are required to create a channel."
        );
        return;
      }

      const channelId = `${orgId}_${projectId}_${chatId}_${chatMessageId}`;
      socketRef.current?.emit(
        "get_prompt_response",
        { channelId, chatId, chatMessageId, projectId, orgId, sessionDetails },
        () => {}
      );

      socketRef.current?.off(channelId);
      socketRef.current?.on(channelId, (data) => {
        handleSocketData(data);
      });

      // persist channel
      addChannel(channelId, { chatId, chatMessageId });
    },
    [projectId, orgId, handleSocketData]
  );

  const disconnectSocket = useCallback(() => {
    socketRef.current?.disconnect();
    socketRef.current = null;
    setIsConnected(false);
  }, [activeChannels, activeChannelMeta]);

  const handleTransformApply = useCallback(
    ({ chatId, chatMessageId }) => {
      if (!chatId || !chatMessageId || !projectId) {
        console.error("Missing identifiers for transformation.");
        return;
      }
      const channelId = `${orgId}_${projectId}_${chatId}_${chatMessageId}`;
      socketRef.current?.emit("transformation_applied", {
        chatId,
        chatMessageId,
        projectId,
        channelId,
        orgId,
      });
      socketRef.current?.off(channelId);
      socketRef.current?.on(channelId, handleSocketData);
      addChannel(channelId, { chatId, chatMessageId });
    },
    [projectId, orgId, handleSocketData]
  );

  const stopPromptRun = useCallback(
    ({ chatId, chatMessageId }) => {
      if (!chatId || !chatMessageId || !projectId) {
        console.error("Missing identifiers for termination of prompt run.");
        return;
      }
      const channelId = `${orgId}_${projectId}_${chatId}_${chatMessageId}`;
      socketRef.current?.emit("stop_chat_ai", {
        chatId,
        chatMessageId,
        projectId,
        channelId,
        orgId,
      });
      socketRef.current?.off(channelId);
      socketRef.current?.on(channelId, handleSocketData);
      addChannel(channelId, { chatId, chatMessageId });
    },
    [projectId, orgId, handleSocketData]
  );

  const handleSqlRun = useCallback(
    ({ chatId, chatMessageId }) => {
      if (!chatId || !chatMessageId || !projectId) {
        return;
      }
      const channelId = `${orgId}_${projectId}_${chatId}_${chatMessageId}`;
      socketRef.current?.emit("run_sql_query", {
        chatId,
        chatMessageId,
        projectId,
        channelId,
        orgId,
      });
      socketRef.current?.off(channelId);
      socketRef.current?.on(channelId, handleSocketData);
      addChannel(channelId, { chatId, chatMessageId });
    },
    [projectId, orgId, handleSocketData]
  );

  useEffect(() => {
    return () => {
      socketRef.current?.disconnect();
      if (timerRef.current) clearTimeout(timerRef.current);
      clear();
    };
  }, [clear]);

  return {
    connectSocket,
    createChannel,
    disconnectSocket,
    handleTransformApply,
    stopPromptRun,
    handleSqlRun,
    isConnected,
  };
}
