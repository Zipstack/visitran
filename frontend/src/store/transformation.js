import { create } from "zustand";
const STORE_VARIABLES = {
  transformations: "",
};
const transformStore = create((setState) => ({
  ...STORE_VARIABLES,
  setTransformations: (transformationType) => {
    setState(() => {
      return { transformations: transformationType };
    });
  },
}));
export { transformStore };
