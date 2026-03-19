import {
  useRef,
  useState,
  useCallback,
  useEffect,
  useLayoutEffect,
} from "react";
import PropTypes from "prop-types";
import { Layout, Tooltip } from "antd";
import {
  RightCircleFilled,
  PlusOutlined,
  LeftCircleFilled,
} from "@ant-design/icons";

import { useUserStore } from "../../store/user-store";
import { useProjectStore } from "../../store/project-store";
import { useTokenStore } from "../../store/token-store";
import { VisitranAIDarkIcon, VisitranAILightIcon } from "../../base/icons";

// Cloud-only: fetch token balance (unavailable in OSS — import fails gracefully)
let fetchOrganizationTokenBalance = null;
try {
  ({
    fetchOrganizationTokenBalance,
  } = require("../../plugins/token-management/token-balance"));
} catch {
  // OSS: token balance not available
}

const DEFAULT_WIDTH = 600;
const MIN_WIDTH = 600;
const MAX_WIDTH = 1200;
// Minimum width when tips panel is visible (fullscreen mode)
const MIN_WIDTH_WITH_TIPS = 900;

import { ChatAI } from "./ChatAI";
import { SQLDrawer } from "../sql-drawer/SQLDrawer";
import { PythonDrawer } from "../python-drawer/PythonDrawer";
import { SequenceDrawer } from "../sequence-drawer/SequenceDrawer";
import { DRAWER_TYPES } from "../../common/constants";
import { useNoCodeModelDrawerStore } from "../../store/no-code-model-drawer-store";
import "./ChatAI.css";

const { Sider } = Layout;

// Tab types that determine if AI drawer should auto-expand/collapse
const MODEL_TAB_TYPES = [
  "NO_CODE_MODEL",
  "FULL_CODE_MODEL",
  "SQL_FLOW",
  "ROOT_DB",
  "SEED_CSV_FILE",
];

// Helper to calculate full width (used for initial state and resizing)
const calculateFullWidth = () => {
  const leftExplorer = document.querySelector(".ideResizeExplorer");
  const leftWidth = leftExplorer
    ? leftExplorer.getBoundingClientRect().width
    : 0;
  return window.innerWidth - leftWidth;
};

// Helper to check if there are model tabs open
const hasModelTabsOpen = () => {
  const state = useProjectStore.getState();
  const openedTabs = state.projectDetails?.[state.projectId]?.openedTabs || [];
  return openedTabs.some((tab) => MODEL_TAB_TYPES.includes(tab.type));
};

function ChatLayout({ children }) {
  const lastDrawerTypeRef = useRef(DRAWER_TYPES.CHAT_AI);
  const chatAIRef = useRef(null);
  const userClosedDrawerRef = useRef(false);

  // Calculate initial width - full width if no model tabs, otherwise default
  const [drawerWidth, setDrawerWidth] = useState(() => {
    if (!hasModelTabsOpen()) {
      return calculateFullWidth() || window.innerWidth;
    }
    return DEFAULT_WIDTH;
  });

  const [isResizing, setIsResizing] = useState(false);
  const initialMouseXRef = useRef(0);
  const initialWidthRef = useRef(DEFAULT_WIDTH);

  // Initialize isFullWidth based on whether model tabs are open
  const [isFullWidth, setIsFullWidth] = useState(() => !hasModelTabsOpen());
  const prevWidthRef = useRef(DEFAULT_WIDTH);

  const currentTheme = useUserStore(
    (state) => state?.userDetails?.currentTheme
  );
  const { tokenBalance, setTokenBalance } = useTokenStore();

  // Fetch token balance on mount if not already loaded (Cloud only)
  useEffect(() => {
    if (!fetchOrganizationTokenBalance || tokenBalance !== null) return;
    fetchOrganizationTokenBalance()
      .then((data) => setTokenBalance(data))
      .catch((err) => console.error("Failed to fetch token balance:", err));
  }, [tokenBalance, setTokenBalance]);

  const {
    rightDrawerStatus: {
      isRightDrawerOpen = false,
      rightDrawerType = null,
    } = {},
    handleRightDrawer,
  } = useNoCodeModelDrawerStore();

  // Resize handlers
  const handleMouseMove = useCallback(
    (e) => {
      if (!isResizing) return;
      // For right-side drawer: dragging left (decreasing clientX) increases width
      const delta = initialMouseXRef.current - e.clientX;
      let newWidth = initialWidthRef.current + delta;

      // When in fullscreen mode with tips panel, enforce higher minimum
      // If user drags below tips threshold, exit fullscreen mode
      if (isFullWidth && newWidth < MIN_WIDTH_WITH_TIPS) {
        setIsFullWidth(false);
        prevWidthRef.current = DEFAULT_WIDTH;
      }

      // Apply min/max constraints
      newWidth = Math.min(MAX_WIDTH, Math.max(MIN_WIDTH, newWidth));
      setDrawerWidth(newWidth);
    },
    [isResizing, isFullWidth]
  );

  const handleMouseUp = useCallback(() => {
    setIsResizing(false);
    document.body.style.cursor = "";
    document.body.style.userSelect = "";
  }, []);

  const handleResizeStart = useCallback(
    (e) => {
      e.preventDefault();
      initialMouseXRef.current = e.clientX;
      initialWidthRef.current = drawerWidth;
      setIsResizing(true);
      document.body.style.cursor = "ew-resize";
      document.body.style.userSelect = "none";
    },
    [drawerWidth]
  );

  useEffect(() => {
    if (isResizing) {
      window.addEventListener("mousemove", handleMouseMove);
      window.addEventListener("mouseup", handleMouseUp);
    }
    return () => {
      window.removeEventListener("mousemove", handleMouseMove);
      window.removeEventListener("mouseup", handleMouseUp);
    };
  }, [isResizing, handleMouseMove, handleMouseUp]);

  // Remember the last drawer type when it changes
  if (rightDrawerType) {
    lastDrawerTypeRef.current = rightDrawerType;
  }

  // Get opened tabs from project store
  const openedTabs = useProjectStore(
    (state) => state.projectDetails?.[state.projectId]?.openedTabs || []
  );

  // Track previous model tab count to detect new tabs being opened
  const prevModelTabCountRef = useRef(
    openedTabs.filter((tab) => MODEL_TAB_TYPES.includes(tab.type)).length
  );

  // Auto-resize to default width when a NEW model tab is opened while in full width mode
  useEffect(() => {
    const currentModelTabCount = openedTabs.filter((tab) =>
      MODEL_TAB_TYPES.includes(tab.type)
    ).length;

    // Only resize if a new model tab was added (count increased) while in full width
    if (isFullWidth && currentModelTabCount > prevModelTabCountRef.current) {
      setDrawerWidth(prevWidthRef.current || DEFAULT_WIDTH);
      setIsFullWidth(false);
    }

    // Update the ref for next comparison
    prevModelTabCountRef.current = currentModelTabCount;
  }, [openedTabs, isFullWidth]);

  // Auto-open and expand AI drawer to full width when no model tabs are open
  // Using useLayoutEffect to avoid visible resize animation on initial load
  useLayoutEffect(() => {
    const hasModelTabs = openedTabs.some((tab) =>
      MODEL_TAB_TYPES.includes(tab.type)
    );

    // If no model tabs, auto-open AI drawer in full width
    // But skip if the user explicitly closed the drawer
    if (!hasModelTabs) {
      if (userClosedDrawerRef.current) {
        return;
      }
      // Open the AI drawer if not already open
      if (!isRightDrawerOpen || rightDrawerType !== DRAWER_TYPES.CHAT_AI) {
        handleRightDrawer(DRAWER_TYPES.CHAT_AI);
      }
      // Expand to full width if not already
      if (!isFullWidth) {
        prevWidthRef.current = drawerWidth;
        const leftExplorer = document.querySelector(".ideResizeExplorer");
        const leftWidth = leftExplorer
          ? leftExplorer.getBoundingClientRect().width
          : 0;
        const availableWidth = window.innerWidth - leftWidth;
        setDrawerWidth(availableWidth);
        setIsFullWidth(true);
      }
    } else {
      // Reset the flag when model tabs are opened, so auto-open works again
      userClosedDrawerRef.current = false;
    }
  }, [
    openedTabs,
    isRightDrawerOpen,
    rightDrawerType,
    isFullWidth,
    drawerWidth,
    handleRightDrawer,
  ]);

  // Auto-adjust fullscreen width when left explorer is resized
  useEffect(() => {
    if (!isFullWidth) return;

    const handleExplorerResize = () => {
      const leftExplorer = document.querySelector(".ideResizeExplorer");
      const leftWidth = leftExplorer
        ? leftExplorer.getBoundingClientRect().width
        : 0;
      const availableWidth = window.innerWidth - leftWidth;
      setDrawerWidth(availableWidth);
    };

    // Listen for resize events (dispatched by ResizerComponent)
    window.addEventListener("resize", handleExplorerResize);
    return () => window.removeEventListener("resize", handleExplorerResize);
  }, [isFullWidth]);

  const closeDrawer = () => {
    userClosedDrawerRef.current = true;
    handleRightDrawer(null);
  };

  // Collapse: if fullscreen, go to default width; otherwise close
  const collapseDrawer = () => {
    if (isFullWidth) {
      setDrawerWidth(prevWidthRef.current || DEFAULT_WIDTH);
      setIsFullWidth(false);
    } else {
      userClosedDrawerRef.current = true;
      handleRightDrawer(null);
    }
  };

  const openDrawer = () => {
    userClosedDrawerRef.current = false;
    handleRightDrawer(lastDrawerTypeRef.current);
  };
  const toggleDrawer = () =>
    isRightDrawerOpen ? collapseDrawer() : openDrawer();

  // Start a new chat and open the drawer
  const handleNewChat = () => {
    chatAIRef.current?.startNewChat();
    handleRightDrawer(DRAWER_TYPES.CHAT_AI);
  };

  const toggleFullWidth = useCallback(() => {
    if (isFullWidth) {
      // Restore to previous width
      setDrawerWidth(prevWidthRef.current);
      setIsFullWidth(false);
    } else {
      // Save current width and expand to fill available space
      prevWidthRef.current = drawerWidth;
      const leftExplorer = document.querySelector(".ideResizeExplorer");
      const leftWidth = leftExplorer
        ? leftExplorer.getBoundingClientRect().width
        : 0;
      const availableWidth = window.innerWidth - leftWidth;
      setDrawerWidth(availableWidth);
      setIsFullWidth(true);
    }
  }, [isFullWidth, drawerWidth]);

  const renderDrawerContent = () => {
    // Always render based on lastDrawerTypeRef to keep content mounted
    const drawerType = rightDrawerType || lastDrawerTypeRef.current;

    if (drawerType === DRAWER_TYPES.CHAT_AI) {
      return (
        <ChatAI
          ref={chatAIRef}
          isChatDrawerOpen={isRightDrawerOpen}
          closeChatDrawer={closeDrawer}
          collapseDrawer={collapseDrawer}
          toggleFullWidth={toggleFullWidth}
          isFullWidth={isFullWidth}
        />
      );
    }

    if (drawerType === DRAWER_TYPES.SQL) {
      return (
        <SQLDrawer
          isSQLDrawerOpen={isRightDrawerOpen}
          closeSQLDrawer={closeDrawer}
        />
      );
    }

    if (drawerType === DRAWER_TYPES.PYTHON) {
      return (
        <PythonDrawer
          isPythonDrawerOpen={isRightDrawerOpen}
          closePythonDrawer={closeDrawer}
        />
      );
    }

    if (drawerType === DRAWER_TYPES.SEQUENCE) {
      return (
        <SequenceDrawer
          isSequenceDrawerOpen={isRightDrawerOpen}
          closeSequenceDrawer={closeDrawer}
        />
      );
    }

    return null;
  };

  return (
    <Layout className="height-100">
      <Layout>{children}</Layout>
      <div className="chat-ai-sider-wrapper">
        {/* Collapsed sidebar bar - shown when drawer is closed */}
        {!isRightDrawerOpen && (
          <div className="chat-ai-collapsed-bar">
            {/* AI Icon - click to expand */}
            <Tooltip title="Open Visitran AI" placement="left">
              <div
                className="collapsed-bar-item collapsed-bar-logo"
                onClick={toggleDrawer}
                role="button"
                tabIndex={0}
                onKeyDown={(e) => e.key === "Enter" && toggleDrawer()}
              >
                {currentTheme === "dark" ? (
                  <VisitranAIDarkIcon className="chat-ai-collapsed-icon" />
                ) : (
                  <VisitranAILightIcon className="chat-ai-collapsed-icon" />
                )}
              </div>
            </Tooltip>

            {/* New Chat button */}
            <Tooltip title="New Chat" placement="left">
              <div
                className="collapsed-bar-item collapsed-bar-action"
                onClick={handleNewChat}
                role="button"
                tabIndex={0}
                onKeyDown={(e) => e.key === "Enter" && handleNewChat()}
              >
                <PlusOutlined />
              </div>
            </Tooltip>

            {/* Spacer */}
            <div className="collapsed-bar-spacer" />

            {/* Credits display */}
            <Tooltip title="Credits remaining" placement="left">
              <div className="collapsed-bar-item collapsed-bar-credits">
                {tokenBalance && tokenBalance.current_balance !== null
                  ? tokenBalance.current_balance >= 1000
                    ? `${(tokenBalance.current_balance / 1000).toFixed(1)}K`
                    : tokenBalance.current_balance
                  : "—"}
              </div>
            </Tooltip>

            {/* Expand arrow */}
            <div
              className="collapsed-bar-item collapsed-bar-expand"
              onClick={toggleDrawer}
              role="button"
              tabIndex={0}
              onKeyDown={(e) => e.key === "Enter" && toggleDrawer()}
            >
              <LeftCircleFilled />
            </div>
          </div>
        )}

        {/* Collapse button - shown when drawer is open but not in full width mode */}
        {isRightDrawerOpen && !isFullWidth && (
          <div
            className="chat-ai-collapse-btn"
            onClick={toggleDrawer}
            role="button"
            tabIndex={0}
            onKeyDown={(e) => e.key === "Enter" && toggleDrawer()}
          >
            <RightCircleFilled />
          </div>
        )}
        <Sider
          width={drawerWidth}
          collapsedWidth={0}
          collapsible
          collapsed={!isRightDrawerOpen}
          trigger={null}
          className={`chat-ai-sider ${isResizing ? "resizing" : ""}`}
        >
          {/* Resize handle on the left edge */}
          {isRightDrawerOpen && (
            <div
              className="chat-ai-resize-handle"
              onMouseDown={handleResizeStart}
            />
          )}
          <div
            className={`chat-ai-content ${!isRightDrawerOpen ? "hidden" : ""}`}
          >
            {renderDrawerContent()}
          </div>
        </Sider>
      </div>
    </Layout>
  );
}

ChatLayout.propTypes = {
  children: PropTypes.node.isRequired,
};

export { ChatLayout };
