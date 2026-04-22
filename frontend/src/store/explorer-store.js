import { create } from "zustand";

/**
 * Explorer Store
 * Holds the shared response of explorerSvc.getExplorer(projectId)
 * so multiple consumers (explorer tree, chat autocomplete) don't refetch.
 * Owner of writes: frontend/src/ide/explorer/explorer-component.jsx
 */
const useExplorerStore = create((set) => ({
  // res.data.children from /explorer API — array where [0]=models, [1]=seeds
  explorerData: null,
  setExplorerData: (data) => set({ explorerData: data }),
  clearExplorerData: () => set({ explorerData: null }),
}));

export { useExplorerStore };
