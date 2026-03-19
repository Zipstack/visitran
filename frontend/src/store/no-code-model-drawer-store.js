import { create } from "zustand";

import { DRAWER_TYPES } from "../common/constants";

const DEFAULT_DRAWER_STATE = {
  isRightDrawerOpen: true,
  rightDrawerType: DRAWER_TYPES.CHAT_AI,
};

const CLOSED_DRAWER_STATE = {
  isRightDrawerOpen: false,
  rightDrawerType: null,
};

const VALID_DRAWER_TYPES = new Set(Object.values(DRAWER_TYPES));

const useNoCodeModelDrawerStore = create((set, get) => ({
  rightDrawerStatus: { ...DEFAULT_DRAWER_STATE },

  // Initialize AI drawer to open by default
  initializeAIDrawer: () => {
    set({ rightDrawerStatus: { ...DEFAULT_DRAWER_STATE } });
  },

  handleRightDrawer: (type) => {
    // close on invalid or no type
    if (!VALID_DRAWER_TYPES.has(type)) {
      set({ rightDrawerStatus: { ...CLOSED_DRAWER_STATE } });
      return;
    }

    const { rightDrawerStatus: { isRightDrawerOpen, rightDrawerType } = {} } =
      get();

    // toggle: close if same type is already open
    if (isRightDrawerOpen && rightDrawerType === type) {
      set({ rightDrawerStatus: { ...CLOSED_DRAWER_STATE } });
      return;
    }

    // otherwise open with the new type
    set({
      rightDrawerStatus: {
        isRightDrawerOpen: true,
        rightDrawerType: type,
      },
    });
  },
}));

export { useNoCodeModelDrawerStore };
