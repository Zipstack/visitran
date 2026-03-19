import { create } from "zustand";

const STORE_VARIABLES = {
  permissionDetails: {},
};

const usePermissionStore = create((setState, getState) => ({
  ...STORE_VARIABLES,
  setPermissionDetails: (details) => {
    setState(() => {
      return {
        permissionDetails: { ...getState().permissionDetails, ...details },
      };
    });
  },
}));

export { usePermissionStore };
