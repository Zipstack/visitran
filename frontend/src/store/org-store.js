import { create } from "zustand";
import { persist } from "zustand/middleware";

const orgStore = create(
  persist(
    (set) => ({
      selectedOrgId: null,
      setOrgId: (id) => set({ selectedOrgId: id }),
    }),
    {
      name: "orgid", // name of the key in localStorage or sessionStorage
      getStorage: () => localStorage,
    }
  )
);

export { orgStore };
