import { create } from "zustand";

// Default transform IDs (all null)
const DEFAULT_STATE = {
  SYNTHESIZE: null,
  JOIN: null,
  FILTER: null,
  GROUPS_AND_AGGREGATION: null,
  UNION: null,
  PREVIEW: null,
  DISTINCT: null,
  TRANSPOSE: null,
  FIND_AND_REPLACE: null,
  COMBINE_COLUMNS: null,
  RENAME_COLUMN: null,
  PIVOT: null,
  WINDOW: null,
};

// Mapping from transform types to our state keys
const TYPE_TO_KEY = {
  synthesize: "SYNTHESIZE",
  join: "JOIN",
  filter: "FILTER",
  groups_and_aggregation: "GROUPS_AND_AGGREGATION",
  union: "UNION",
  distinct: "DISTINCT",
  transpose: "TRANSPOSE",
  find_and_replace: "FIND_AND_REPLACE",
  combine_columns: "COMBINE_COLUMNS",
  rename_column: "RENAME_COLUMN",
  pivot: "PIVOT",
  window: "WINDOW",
};

const useTransformIdStore = create((set) => ({
  // Initialize with a fresh copy of defaults
  transformIds: { ...DEFAULT_STATE },

  // Populate transformIds based on incoming spec
  setTransformIds: (spec) => {
    // Destructure spec with safe defaults
    const { transform_order: order = [], transform = {} } = spec || {};

    // Start from clean slate
    const newState = { ...DEFAULT_STATE };

    // For each ID in order, map its type to the correct key
    order.forEach((id) => {
      const type = transform[id]?.type;
      const key = TYPE_TO_KEY[type];
      if (key) {
        newState[key] = id;
      }
    });

    // Push the updated IDs into the store
    set({ transformIds: newState });
  },
}));

export { useTransformIdStore };
