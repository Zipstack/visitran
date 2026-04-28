import { create } from "zustand";
import axios from "axios";

/**
 * Feature Flags Store
 *
 * Manages feature flags from the backend, including:
 * - Direct execution mode (YAML-to-Ibis architecture)
 * - Other feature toggles
 */

const INITIAL_STATE = {
  // Direct execution feature flags
  enableDirectExecution: false,
  executionMode: "legacy", // "legacy" | "direct" | "parallel"
  suppressPythonFiles: false,

  // Loading state
  isLoading: false,
  isLoaded: false,
  error: null,
};

const useFeatureFlagsStore = create((set, get) => ({
  ...INITIAL_STATE,

  /**
   * Fetch feature flags from the backend
   * @param {string} orgId - Organization ID
   */
  fetchFeatureFlags: async (orgId) => {
    if (get().isLoaded) return; // Already loaded

    set({ isLoading: true, error: null });

    try {
      const response = await axios.get(
        `/api/v1/visitran/${orgId || "default_org"}/feature-flags`
      );

      const flags = response.data;
      set({
        enableDirectExecution: flags.enable_direct_execution || false,
        executionMode: flags.execution_mode || "legacy",
        suppressPythonFiles: flags.suppress_python_files || false,
        isLoading: false,
        isLoaded: true,
      });
    } catch (error) {
      console.warn("Failed to fetch feature flags, using defaults:", error);
      set({
        isLoading: false,
        isLoaded: true,
        error: error.message,
      });
    }
  },

  /**
   * Check if direct execution is enabled
   * @returns {boolean}
   */
  isDirectExecutionEnabled: () => {
    const { enableDirectExecution, executionMode } = get();
    return enableDirectExecution && (executionMode === "direct" || executionMode === "parallel");
  },

  /**
   * Reset feature flags to defaults
   */
  reset: () => {
    set(INITIAL_STATE);
  },
}));

export { useFeatureFlagsStore };
