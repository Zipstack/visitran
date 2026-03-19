// Default fallback specs when no ID is provided
const DEFAULT_SPECS = {
  synthesize: { columns: [] },
  join: { tables: [] },
  filter: { criteria: [] },
  groups_and_aggregation: {
    group: [],
    aggregate_columns: [],
    filter: { criteria: [] },
    having: { criteria: [] },
  },
  union: { ignore_duplicate: false, tables: [] },
  preview: { preview_at: null, preview_enabled: false },
  distinct: { columns: [] },
  transpose: { state: false, columnList: [] },
  find_and_replace: { replacements: [] },
  combine_columns: { columns: [] },
  rename_column: { mappings: [] },
  pivot: {
    column: "",
    row: "",
    summerize_by: { aggregator: "", summerize_column: "" },
  },
  window: { columns: [] },
};

// Factory: returns a getter that extracts the given type or falls back
const createSpecGetter =
  (type) =>
  (transformSpec = {}, id) =>
    id && transformSpec[id]?.[type]
      ? transformSpec[id]?.[type]
      : { ...DEFAULT_SPECS[type] };

// Individual getters for each transform operation

const getSynthesizeSpec = createSpecGetter("synthesize");
const getJoinSpec = createSpecGetter("join");
const getFilterSpec = createSpecGetter("filter");
const getGroupAndAggregationSpec = createSpecGetter("groups_and_aggregation");
const getUnionSpec = createSpecGetter("union");
const getPreviewSpec = createSpecGetter("preview");
const getDistinctSpec = createSpecGetter("distinct");
const getTransposeSpec = createSpecGetter("transpose");
const getFindAndReplaceSpec = createSpecGetter("find_and_replace");
const getCombineColumnsSpec = createSpecGetter("combine_columns");
const getRenameColumnSpec = createSpecGetter("rename_column");
const getPivotSpec = createSpecGetter("pivot");
const getWindowSpec = createSpecGetter("window");

const getTransformId = (transformSpec = {}, type) =>
  Object.keys(transformSpec).find((id) => transformSpec[id]?.type === type);

const transformationTypes = {
  SYNTHESIZE: "synthesize",
  JOIN: "join",
  FILTER: "filter",
  GROUPS_AND_AGGREGATION: "groups_and_aggregation",
  UNION: "unions",
  PREVIEW: "preview",
  DISTINCT: "distinct",
  TRANSPOSE: "transpose",
  FIND_AND_REPLACE: "find_and_replace",
  COMBINE_COLUMNS: "combine_columns",
  RENAME_COLUMN: "rename_column",
  PIVOT: "pivot",
  SORT: "sort",
  WINDOW: "window",
};

export {
  getSynthesizeSpec,
  getJoinSpec,
  getFilterSpec,
  getGroupAndAggregationSpec,
  getUnionSpec,
  getPreviewSpec,
  getDistinctSpec,
  getTransposeSpec,
  getFindAndReplaceSpec,
  getCombineColumnsSpec,
  getRenameColumnSpec,
  getPivotSpec,
  getWindowSpec,
  getTransformId,
  transformationTypes,
};
