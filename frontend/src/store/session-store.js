import { create } from "zustand";
import { persist } from "zustand/middleware";

const STORE_VARIABLES = {
  sessionDetails: {},
  showSessionExpiredModal: false,
};

const useSessionStore = create(
  persist(
    (setState) => ({
      ...STORE_VARIABLES,
      setSessionDetails: (details) => {
        setState((state) => ({
          ...state,
          sessionDetails: details,
        }));
      },
      updateSessionDetails: (details) => {
        setState((state) => ({
          ...state,
          sessionDetails: {
            ...state.sessionDetails,
            ...details,
          },
        }));
      },
      setShowSessionExpiredModal: (show) => {
        setState((state) => ({
          ...state,
          showSessionExpiredModal: show,
        }));
      },
    }),
    {
      name: "session-storage", // key in localStorage
      partialize: (state) => ({
        sessionDetails: state.sessionDetails,
      }),
    }
  )
);

export { useSessionStore };
