import { create } from "zustand";
import { persist } from "zustand/middleware";

const STORE_VARIABLES = {
  userDetails: {},
};
const useUserStore = create(
  persist(
    (setState, getState) => ({
      ...STORE_VARIABLES,
      setUserDetails: (details) => {
        setState(() => {
          return { userDetails: details };
        });
      },
      updateUserDetails: (details) => {
        setState(() => {
          return { userDetails: { ...getState().userDetails, ...details } };
        });
      },
    }),
    {
      name: "user-storage",
      partialize: (state) => ({
        userDetails: state.userDetails || {},
      }),
    }
  )
);

export { useUserStore };
