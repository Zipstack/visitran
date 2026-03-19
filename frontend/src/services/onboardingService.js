import Cookies from "js-cookie";

import { useAxiosPrivate } from "../service/axios-service";
import { orgStore } from "../store/org-store";

/**
 * Onboarding Service - Handles all onboarding-related API calls
 * @return {Object} Object containing onboarding API methods
 */
export function useOnboardingService() {
  const axiosPrivate = useAxiosPrivate();
  const csrfToken = Cookies.get("csrftoken");
  const { selectedOrgId } = orgStore();
  const orgId = selectedOrgId || "default_org";
  const baseUrl = "/onboarding";

  /**
   * Get onboarding status for a project
   * @param {string} projectId - Project UUID
   * @return {Promise} API response with onboarding status
   */
  const getOnboardingStatus = async (projectId) => {
    try {
      const response = await axiosPrivate.get(
        `/api/v1/visitran/${orgId}/project/${projectId}${baseUrl}/status/`,
        {
          headers: {
            "Content-Type": "application/json",
            "X-CSRFToken": csrfToken,
          },
        }
      );
      return {
        success: true,
        data: response.data,
      };
    } catch (error) {
      console.error("Error fetching onboarding status:", error);
      return {
        success: false,
        error: error.response?.data || error.message,
      };
    }
  };

  /**
   * Get onboarding template by ID
   * @param {string} templateId - Template identifier
   * @return {Promise} API response with template data
   */
  const getOnboardingTemplate = async (templateId) => {
    try {
      const response = await axiosPrivate.get(
        `/api/v1/visitran${baseUrl}/templates/${templateId}/`,
        {
          headers: {
            "Content-Type": "application/json",
            "X-CSRFToken": csrfToken,
          },
        }
      );
      return {
        success: true,
        data: response.data,
      };
    } catch (error) {
      console.error("Error fetching onboarding template:", error);
      return {
        success: false,
        error: error.response?.data || error.message,
      };
    }
  };

  /**
   * Start onboarding session
   * @param {string} projectId - Project UUID
   * @param {string} templateId - Template identifier
   * @return {Promise} API response with session data
   */
  const startOnboarding = async (
    projectId,
    templateId = "jaffle_shop_starter"
  ) => {
    try {
      const response = await axiosPrivate.post(
        `/api/v1/visitran/${orgId}/project/${projectId}${baseUrl}/start/`,
        { template_id: templateId },
        {
          headers: {
            "Content-Type": "application/json",
            "X-CSRFToken": csrfToken,
          },
        }
      );
      return {
        success: true,
        data: response.data,
      };
    } catch (error) {
      console.error("Error starting onboarding:", error);
      return {
        success: false,
        error: error.response?.data || error.message,
      };
    }
  };

  /**
   * Complete a task in onboarding
   * @param {string} projectId - Project UUID
   * @param {string} taskId - Task identifier
   * @param {object} taskData - Task completion data
   * @return {Promise} API response
   */
  const completeTask = async (projectId, taskId, taskData = {}) => {
    try {
      const response = await axiosPrivate.post(
        `/api/v1/visitran/${orgId}/project/${projectId}${baseUrl}/complete-task/`,
        {
          task_id: taskId,
          task_data: taskData,
        },
        {
          headers: {
            "Content-Type": "application/json",
            "X-CSRFToken": csrfToken,
          },
        }
      );
      return {
        success: true,
        data: response.data,
      };
    } catch (error) {
      console.error("Error completing task:", error);
      return {
        success: false,
        error: error.response?.data || error.message,
      };
    }
  };

  /**
   * Skip a task in onboarding
   * @param {string} projectId - Project UUID
   * @param {string} taskId - Task identifier
   * @return {Promise} API response
   */
  const skipTask = async (projectId, taskId) => {
    try {
      const response = await axiosPrivate.post(
        `/api/v1/visitran/${orgId}/project/${projectId}${baseUrl}/skip-task/`,
        { task_id: taskId },
        {
          headers: {
            "Content-Type": "application/json",
            "X-CSRFToken": csrfToken,
          },
        }
      );
      return {
        success: true,
        data: response.data,
      };
    } catch (error) {
      console.error("Error skipping task:", error);
      return {
        success: false,
        error: error.response?.data || error.message,
      };
    }
  };

  /**
   * Reset onboarding session
   * @param {string} projectId - Project UUID
   * @return {Promise} API response
   */
  const resetOnboarding = async (projectId) => {
    try {
      const response = await axiosPrivate.post(
        `/api/v1/visitran/${orgId}/project/${projectId}${baseUrl}/reset/`,
        {},
        {
          headers: {
            "Content-Type": "application/json",
            "X-CSRFToken": csrfToken,
          },
        }
      );
      return {
        success: true,
        data: response.data,
      };
    } catch (error) {
      console.error("Error resetting onboarding:", error);
      return {
        success: false,
        error: error.response?.data || error.message,
      };
    }
  };

  /**
   * Toggle onboarding for project (admin only)
   * @param {string} projectId - Project UUID
   * @return {Promise} API response
   */
  const toggleProjectOnboarding = async (projectId) => {
    try {
      const response = await axiosPrivate.post(
        `/api/v1/visitran/${orgId}/project/${projectId}${baseUrl}/toggle/`,
        {},
        {
          headers: {
            "Content-Type": "application/json",
            "X-CSRFToken": csrfToken,
          },
        }
      );
      return {
        success: true,
        data: response.data,
      };
    } catch (error) {
      console.error("Error toggling project onboarding:", error);
      return {
        success: false,
        error: error.response?.data || error.message,
      };
    }
  };

  /**
   * Mark onboarding as complete
   * @param {string} projectId - Project UUID
   * @return {Promise} API response
   */
  const markOnboardingComplete = async (projectId) => {
    try {
      const response = await axiosPrivate.post(
        `/api/v1/visitran/${orgId}/project/${projectId}${baseUrl}/mark-complete/`,
        {},
        {
          headers: {
            "Content-Type": "application/json",
            "X-CSRFToken": csrfToken,
          },
        }
      );
      return {
        success: true,
        data: response.data,
      };
    } catch (error) {
      console.error("Error marking onboarding as complete:", error);
      return {
        success: false,
        error: error.response?.data || error.message,
      };
    }
  };

  return {
    getOnboardingStatus,
    getOnboardingTemplate,
    startOnboarding,
    completeTask,
    skipTask,
    resetOnboarding,
    toggleProjectOnboarding,
    markOnboardingComplete,
  };
}
