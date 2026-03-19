import { create } from "zustand";

const DEFAULT_STATE = {
  refreshModels: false,
};

const useRefreshModelsStore = create((set) => ({
  ...DEFAULT_STATE,
  setRefreshModels: (value) => set({ refreshModels: value }),
}));

export { useRefreshModelsStore };
