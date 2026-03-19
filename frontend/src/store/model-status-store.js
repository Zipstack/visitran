import { create } from "zustand";
import { persist } from "zustand/middleware";

/**
 * Model Status Store
 * Tracks the run status of no_code models (success, error, pending)
 * Persists to localStorage so status survives page refresh
 */

// Status values: "success" | "error" | "pending" | null
const DEFAULT_STATE = {
  // Map of projectId -> { modelName: status }
  modelStatuses: {},
};

const useModelStatusStore = create(
  persist(
    (set, get) => ({
      ...DEFAULT_STATE,

      /**
       * Set status for a single model
       * @param {string} projectId - Project ID
       * @param {string} modelName - Model name
       * @param {string} status - "success" | "error" | "pending"
       * @return {void}
       */
      setModelStatus: (projectId, modelName, status) =>
        set((state) => ({
          modelStatuses: {
            ...state.modelStatuses,
            [projectId]: {
              ...(state.modelStatuses[projectId] || {}),
              [modelName]: status,
            },
          },
        })),

      /**
       * Set status for multiple models at once
       * @param {string} projectId - Project ID
       * @param {Array<string>} modelNames - Array of model names
       * @param {string} status - "success" | "error" | "pending"
       * @return {void}
       */
      setMultipleModelStatuses: (projectId, modelNames, status) =>
        set((state) => {
          const currentProjectStatuses = state.modelStatuses[projectId] || {};
          const updatedStatuses = { ...currentProjectStatuses };
          modelNames.forEach((name) => {
            updatedStatuses[name] = status;
          });
          return {
            modelStatuses: {
              ...state.modelStatuses,
              [projectId]: updatedStatuses,
            },
          };
        }),

      /**
       * Get status for a model
       * @param {string} projectId - Project ID
       * @param {string} modelName - Model name
       * @return {string|null} - Status or null if not set
       */
      getModelStatus: (projectId, modelName) => {
        const state = get();
        return state.modelStatuses[projectId]?.[modelName] || null;
      },

      /**
       * Get all statuses for a project
       * @param {string} projectId - Project ID
       * @return {Object} - Map of modelName -> status
       */
      getProjectStatuses: (projectId) => {
        const state = get();
        return state.modelStatuses[projectId] || {};
      },

      /**
       * Clear status for a model (e.g., when model is deleted)
       * @param {string} projectId - Project ID
       * @param {string} modelName - Model name
       * @return {void}
       */
      clearModelStatus: (projectId, modelName) =>
        set((state) => {
          const projectStatuses = { ...(state.modelStatuses[projectId] || {}) };
          delete projectStatuses[modelName];
          return {
            modelStatuses: {
              ...state.modelStatuses,
              [projectId]: projectStatuses,
            },
          };
        }),

      /**
       * Clear all statuses for a project
       * @param {string} projectId - Project ID
       * @return {void}
       */
      clearProjectStatuses: (projectId) =>
        set((state) => {
          // eslint-disable-next-line no-unused-vars
          const { [projectId]: removed, ...rest } = state.modelStatuses;
          return { modelStatuses: rest };
        }),

      /**
       * Reset the entire store
       * @return {void}
       */
      reset: () => set(DEFAULT_STATE),
    }),
    {
      name: "visitran-model-status", // localStorage key
      partialize: (state) => ({ modelStatuses: state.modelStatuses }),
    }
  )
);

export { useModelStatusStore };
