import { create } from "zustand";

/**
 * Explorer Store
 * Holds the shared responses of explorerSvc.getExplorer(projectId) and
 * explorerSvc.getDbExplorer(projectId) so multiple consumers (explorer
 * tree, chat autocomplete) don't refetch.
 * Owner of writes: frontend/src/ide/explorer/explorer-component.jsx
 */
const useExplorerStore = create((set) => ({
  // res.data.children from /explorer API — array where [0]=models, [1]=seeds
  explorerData: null,
  // res.data from /db_explorer API — single DB tree object
  dbExplorerData: null,
  setExplorerData: (data) => set({ explorerData: data }),
  setDbExplorerData: (data) => set({ dbExplorerData: data }),
  clearExplorerData: () => set({ explorerData: null, dbExplorerData: null }),
}));

export { useExplorerStore };
