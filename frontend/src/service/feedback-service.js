import axios from "axios";

import { getOrgId } from "./organization-service";

// Global state to store current context (initialized with fallback values)
let appContext = {
  organizationId: null,
  projectId: null,
  chatId: null,
};

// Initialize context with values from localStorage on module load
try {
  const storedOrgId = localStorage.getItem("currentOrganizationId");
  const storedProjectId = localStorage.getItem("currentProjectId");
  const storedChatId = localStorage.getItem("currentChatId");

  if (storedOrgId) appContext.organizationId = storedOrgId;
  if (storedProjectId) appContext.projectId = storedProjectId;
  if (storedChatId) appContext.chatId = storedChatId;

  // Also try to get from session storage
  try {
    const visitranState = JSON.parse(
      sessionStorage.getItem("visitran-state") || "{}"
    );
    if (visitranState.currentOrganization && !appContext.organizationId) {
      appContext.organizationId = visitranState.currentOrganization;
    }
  } catch (e) {
    console.error("Error parsing visitran-state from session storage", e);
  }
} catch (e) {
  console.error("Error initializing context from storage:", e);
}

/**
 * Set the current application context
 * This should be called when app initializes or when context changes
 * For example: when user switches organization/project
 *
 * @param {Object} context - Context object with organizationId, projectId, chatId
 */
export const setAppContext = (context) => {
  appContext = { ...appContext, ...context };
};

/**
 * Get a valid organization ID for API calls
 * Uses a strict hierarchy of sources without regex parsing
 *
 * @param {string} routeOrgId - Optional organization ID from route params
 * @return {string|null} - Valid organization ID or null if not available
 */
const getValidOrganizationId = (routeOrgId = null) => {
  // Priority 1: Use parameter if provided
  if (routeOrgId) {
    return routeOrgId;
  }

  // Priority 2: Use app context
  if (appContext.organizationId) {
    return appContext.organizationId;
  }

  // Priority 3: Use getOrgId helper
  const orgId = getOrgId();
  if (orgId) {
    return orgId;
  }

  // Priority 4: Check session storage state
  try {
    const visitranState = JSON.parse(
      sessionStorage.getItem("visitran-state") || "{}"
    );
    if (visitranState.currentOrganization) {
      return visitranState.currentOrganization;
    }
  } catch (e) {
    console.error("Error parsing state from session storage", e);
  }

  // Priority 5: Check URL parameters
  try {
    const urlParams = new URLSearchParams(window.location.search);
    const urlOrgId =
      urlParams.get("org") ||
      urlParams.get("org_id") ||
      urlParams.get("organization");
    if (urlOrgId) {
      return urlOrgId;
    }
  } catch (e) {
    console.error("Error parsing URL parameters", e);
  }

  return null;
};

/**
 * Get valid project and chat IDs for API calls
 * Uses a strict hierarchy of sources without regex parsing
 *
 * @return {Object} - Object with projectId and chatId or null values
 */
const getValidProjectAndChatIds = () => {
  let projectId = null;
  let chatId = null;

  // Priority 1: Use app context
  if (appContext.projectId) projectId = appContext.projectId;
  if (appContext.chatId) chatId = appContext.chatId;

  // Priority 2: Try localStorage
  if (!projectId) projectId = localStorage.getItem("currentProjectId");
  if (!chatId) chatId = localStorage.getItem("currentChatId");

  // Priority 3: Try URL path extraction
  if (!projectId || !chatId) {
    try {
      // Extract from URL path using regex (last resort)
      const pathMatch = window.location.pathname.match(
        /\/project\/([^/]+)(?:\/chat\/([^/]+))?/
      );
      if (pathMatch) {
        if (!projectId && pathMatch[1]) projectId = pathMatch[1];
        if (!chatId && pathMatch[2]) chatId = pathMatch[2];
      }
    } catch (e) {
      console.error("Error extracting project and chat IDs from URL:", e);
    }
  }

  // Priority 4: Check URL parameters
  try {
    const urlParams = new URLSearchParams(window.location.search);
    if (!projectId) {
      const urlProjectId =
        urlParams.get("project") || urlParams.get("project_id");
      if (urlProjectId) projectId = urlProjectId;
    }
    if (!chatId) {
      const urlChatId = urlParams.get("chat") || urlParams.get("chat_id");
      if (urlChatId) chatId = urlChatId;
    }
  } catch (e) {
    console.error("Error parsing URL parameters for project/chat IDs", e);
  }

  return { projectId, chatId };
};

/**
 * Submit feedback for an AI response
 *
 * @param {string} chatMessageId - UUID of the chat message
 * @param {boolean} isPositive - Whether feedback is positive (true) or negative (false)
 * @param {string} comment - Optional comment providing more details about the feedback
 * @param {string} routeOrgId - Optional organization ID from the route
 * @return {Promise} - API response
 */
export const submitFeedback = async (
  chatMessageId,
  isPositive,
  comment = "",
  routeOrgId = null
) => {
  // Validate chat message ID
  if (!chatMessageId) {
    throw new Error("Missing chat message ID for feedback submission");
  }

  // Get required IDs for API call
  const orgId = getValidOrganizationId(routeOrgId);
  const { projectId, chatId } = getValidProjectAndChatIds();

  // Validate required IDs
  if (!orgId) {
    throw new Error("Cannot submit feedback: No organization ID available");
  }

  if (!projectId) {
    throw new Error("Cannot submit feedback: Missing project ID");
  }

  if (!chatId) {
    throw new Error("Cannot submit feedback: Missing chat ID");
  }

  try {
    // IMPORTANT: Remove 'org_' prefix if present in organization ID for URL path
    // Backend expects orgId without the prefix in URL but with prefix in header
    const cleanOrgId = orgId.replace(/^org_/, "");

    // Construct the URL with the correct format and clean org ID
    const apiUrl = `/api/v1/visitran/${cleanOrgId}/project/${projectId}/chat/${chatId}/chat-message/${chatMessageId}/feedback/`;

    const response = await axios.post(
      apiUrl,
      {
        feedback_positive: isPositive,
        feedback_comment: comment || "",
      },
      {
        headers: {
          "X-Organization": orgId,
        },
      }
    );

    return response.data;
  } catch (error) {
    // Propagate backend error message if available
    if (error.response?.data?.message) {
      throw new Error(error.response.data.message);
    }
    throw error;
  }
};

/**
 * Get the current feedback status for a chat message
 *
 * @param {string} chatMessageId - UUID of the chat message
 * @return {Promise} - API response with feedback status
 */
export const getFeedbackStatus = async (chatMessageId) => {
  // Validate chat message ID
  if (!chatMessageId) {
    throw new Error("Missing chat message ID for getting feedback status");
  }

  // Get required IDs for API call
  const orgId = getValidOrganizationId();
  const { projectId, chatId } = getValidProjectAndChatIds();

  // If we don't have required IDs, return empty feedback status
  if (!orgId || !projectId || !chatId) {
    console.warn("Missing required IDs for feedback status:", {
      orgId,
      projectId,
      chatId,
    });
    return { has_feedback: false };
  }

  try {
    // IMPORTANT: Remove 'org_' prefix if present in organization ID for URL path
    // Backend expects orgId without the prefix in URL but with prefix in header
    const cleanOrgId = orgId.replace(/^org_/, "");

    // Construct the URL with the correct format and clean org ID
    const apiUrl = `/api/v1/visitran/${cleanOrgId}/project/${projectId}/chat/${chatId}/chat-message/${chatMessageId}/feedback/`;

    const response = await axios.get(apiUrl, {
      headers: {
        "X-Organization": orgId,
      },
    });

    return response.data;
  } catch (error) {
    // Silently fail on 404 (no feedback yet)
    if (error.response?.status === 404) {
      return { has_feedback: false };
    }

    // Propagate other errors
    if (error.response?.data?.message) {
      throw new Error(error.response.data.message);
    }
    throw error;
  }
};
