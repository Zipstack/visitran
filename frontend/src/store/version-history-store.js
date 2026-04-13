import { create } from "zustand";

const DEFAULT_STATE = {
  versionHistory: [],
  selectedVersions: [],
  isCommitModalOpen: false,
  isLoading: false,
  gitConfig: null,
  isVersioningEnabled: false,
  saveCounter: 0,
};

const useVersionHistoryStore = create((set) => ({
  ...DEFAULT_STATE,
  setVersionHistory: (value) => set({ versionHistory: value }),
  setSelectedVersions: (value) => set({ selectedVersions: value }),
  openCommitModal: () => set({ isCommitModalOpen: true }),
  closeCommitModal: () => set({ isCommitModalOpen: false }),
  setIsLoading: (value) => set({ isLoading: value }),
  setGitConfig: (value) =>
    set({ gitConfig: value, isVersioningEnabled: !!value }),
  setIsVersioningEnabled: (value) => set({ isVersioningEnabled: value }),
  notifySave: () => set((s) => ({ saveCounter: s.saveCounter + 1 })),
  clearState: () => set({ ...DEFAULT_STATE }),
}));

export { useVersionHistoryStore };
