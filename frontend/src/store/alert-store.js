import { create } from "zustand";
const STORE_VARIABLES = {
  alert: false,
};
const alertStore = create((setState) => ({
  ...STORE_VARIABLES,
  setAlert: (flag) => {
    setState(() => {
      return { alert: flag };
    });
  },
}));
export { alertStore };
