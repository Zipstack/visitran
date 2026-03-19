import { create } from "zustand";

/**
 * Store for managing lineage tab opening from NoCodeModel bottom panel.
 * When a user clicks "Open as Tab" in the bottom panel lineage,
 * this store is used to signal IdeComponent to open a new lineage tab.
 */
const useLineageTabStore = create((set) => ({
  pendingLineageTab: null,

  /**
   * Set a pending lineage tab to be opened.
   * @param {Object} tabData - { modelName, key }
   */
  setPendingLineageTab: (tabData) => {
    set({ pendingLineageTab: tabData });
  },

  /**
   * Clear the pending lineage tab after it has been opened.
   */
  clearPendingLineageTab: () => {
    set({ pendingLineageTab: null });
  },
}));

export { useLineageTabStore };
