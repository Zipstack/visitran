import Cookies from "js-cookie";

import { useAxiosPrivate } from "../service/axios-service";
import { orgStore } from "../store/org-store";

/**
 * AI Context Rules API Service
 * Handles API calls for personal and project-specific AI context rules
 */

export function useAIContextRulesService() {
  const axiosPrivate = useAxiosPrivate();
  const csrfToken = Cookies.get("csrftoken");
  const { selectedOrgId } = orgStore();
  const orgId = selectedOrgId || "default_org";

  // Personal AI Context Rules
  const getUserAIContextRules = async () => {
    try {
      const url = `/api/v1/visitran/${orgId}/ai-context/user/ai-context-rules/`;
      const response = await axiosPrivate.get(url, {
        headers: {
          "Content-Type": "application/json",
          "X-CSRFToken": csrfToken,
        },
      });
      return response.data;
    } catch (error) {
      console.error("Error fetching user AI context rules:", error);
      throw error;
    }
  };

  const updateUserAIContextRules = async (contextRules) => {
    try {
      const url = `/api/v1/visitran/${orgId}/ai-context/user/ai-context-rules/`;
      const response = await axiosPrivate.put(
        url,
        { context_rules: contextRules },
        {
          headers: {
            "Content-Type": "application/json",
            "X-CSRFToken": csrfToken,
          },
        }
      );
      return response.data;
    } catch (error) {
      console.error("Error updating user AI context rules:", error);
      throw error;
    }
  };

  // Project AI Context Rules
  const getProjectAIContextRules = async (projectId) => {
    try {
      const url = `/api/v1/visitran/${orgId}/ai-context/project/${projectId}/ai-context-rules/`;
      const response = await axiosPrivate.get(url, {
        headers: {
          "Content-Type": "application/json",
          "X-CSRFToken": csrfToken,
        },
      });
      return response.data;
    } catch (error) {
      console.error("Error fetching project AI context rules:", error);
      throw error;
    }
  };

  const updateProjectAIContextRules = async (projectId, contextRules) => {
    try {
      const url = `/api/v1/visitran/${orgId}/ai-context/project/${projectId}/ai-context-rules/`;
      const response = await axiosPrivate.put(
        url,
        { context_rules: contextRules },
        {
          headers: {
            "Content-Type": "application/json",
            "X-CSRFToken": csrfToken,
          },
        }
      );
      return response.data;
    } catch (error) {
      console.error("Error updating project AI context rules:", error);
      throw error;
    }
  };

  return {
    getUserAIContextRules,
    updateUserAIContextRules,
    getProjectAIContextRules,
    updateProjectAIContextRules,
  };
}
