import { useState, useEffect, useRef } from "react";
import {
  PlusOutlined,
  DeleteOutlined,
  FilterOutlined,
} from "@ant-design/icons";
import {
  Typography,
  Button,
  Select,
  Checkbox,
  Input,
  Popconfirm,
  Space,
} from "antd";
import PropTypes from "prop-types";
import axios from "axios";
import { AnimatePresence, motion } from "framer-motion";

import { ToolbarItem } from "../toolbar-item.jsx";
import { useProjectStore } from "../../../../store/project-store.js";
import {
  generateKey,
  useEscapeKey,
  getOperators,
} from "../../../../common/helpers.js";
import { orgStore } from "../../../../store/org-store.js";
import { MergeDarkIcon, MergeLightIcon } from "../../../../base/icons/index.js";
import { useNotificationService } from "../../../../service/notification-service.js";
import { useTransformIdStore } from "../../../../store/transform-id-store.js";
import {
  getUnionSpec,
  transformationTypes,
} from "../../no-code-model/helper.js";

const { Title, Text } = Typography;

/**
 * Branch-based Merge/Union Component
 *
 * Flow:
 * 1. Select source table (default from spec)
 * 2. Select merge table
 * 3. Add column mappings: source_col → merge_col/literal → alias
 * 4. Add filters
 * @return {JSX.Element} The branch-based merge/union component
 */
const Merge = ({
  disabled,
  spec,
  updateSpec,
  allColumns,
  step,
  modelName,
  isDarkTheme,
  saveTransformation,
  handleDeleteTransformation,
  isLoading,
  handleGetColumns,
}) => {
  const [open, setOpen] = useState(false);
  const { selectedOrgId } = orgStore();
  const { dbConfigDetails, projectId } = useProjectStore();

  const isSchemaExists = dbConfigDetails?.is_schema_exists ?? false;
  const [ignoreDupe, setIgnoreDupe] = useState(false);
  const [isModified, setIsModified] = useState(false);
  const { notify } = useNotificationService();
  const [label, setLabel] = useState("Merge");
  const [allTables, setAllTables] = useState([]);
  const { transformIds } = useTransformIdStore();
  const [mergeWarning, setMergeWarning] = useState("");
  const warningTimeoutRef = useRef(null);

  const [loading, setLoading] = useState({});
  const defaultSource = spec?.source?.table_name;
  const defaultSchema = spec?.source?.schema_name || "default";

  const NULL_OPERATORS = ["NULL", "NOTNULL", "TRUE", "FALSE"];
  const MAX_FILTERS_PER_BRANCH = 5;

  // Branches state
  const [branches, setBranches] = useState([]);

  // Source-level filters (common filters applied to source table)
  const [sourceFilters, setSourceFilters] = useState([]);

  // Column options: { branchId: { source: [...], merge: [...], sourceDesc: {...}, mergeDesc: {...} } }
  const [columnOptions, setColumnOptions] = useState({});

  // Source columns (fetched once at top level)
  const [sourceColumns, setSourceColumns] = useState([]);
  const [sourceColumnDesc, setSourceColumnDesc] = useState({});

  // Validation errors state
  const [validationErrors, setValidationErrors] = useState({});

  // Track if there's an existing saved transformation (for Delete button state)
  const [hasExistingTransformation, setHasExistingTransformation] =
    useState(false);

  const handleOpenChange = (value) => {
    if (value) {
      handleGetColumns(transformIds?.UNION, transformationTypes?.UNION);
    }
    setOpen(value);
  };

  useEscapeKey(open, () => setOpen(false));

  const getAllTables = async () => {
    setLoading({ ...loading, tables: true });
    try {
      const requestOptions = {
        method: "GET",
        url: `/api/v1/visitran/${
          selectedOrgId || "default_org"
        }/project/${projectId}/schemas/tables?model=${modelName}`,
      };
      const res = await axios(requestOptions);
      const tables = res?.data?.table_names;
      setAllTables(tables);
    } catch (error) {
      console.error(error);
      notify({ error });
    } finally {
      setLoading({ ...loading, tables: false });
    }
  };

  useEffect(() => {
    if (open) {
      getAllTables();
      // Fetch source columns at top level
      if (defaultSchema && defaultSource) {
        fetchSourceColumns(defaultSchema, defaultSource);
      }
    }
  }, [defaultSource, defaultSchema, open]);

  // Fetch source columns at top level
  const fetchSourceColumns = async (schema, table) => {
    try {
      const requestOptions = {
        method: "GET",
        url: `/api/v1/visitran/${
          selectedOrgId || "default_org"
        }/project/${projectId}/schema/${schema}/table/${table}/columns`,
      };
      const res = await axios(requestOptions);
      const data = res.data.column_names;
      const columnDescription = res.data.column_description || {};

      const options = data.map((el) => {
        const dataType = columnDescription[el]?.data_type || "String";
        const dbType = columnDescription[el]?.column_dbtype || "String";
        return {
          value: el,
          label: el,
          dataType: dataType,
          dbType: dbType,
        };
      });

      setSourceColumns(options);
      setSourceColumnDesc(columnDescription);
    } catch (error) {
      console.error("Error fetching source columns:", error);
      notify({ error });
    }
  };

  // Fetch columns for a table
  const fetchColumns = async (schema, table, branchId, type) => {
    try {
      const requestOptions = {
        method: "GET",
        url: `/api/v1/visitran/${
          selectedOrgId || "default_org"
        }/project/${projectId}/schema/${schema}/table/${table}/columns`,
      };
      const res = await axios(requestOptions);
      const data = res.data.column_names;
      const columnDescription = res.data.column_description || {};

      const options = data.map((el) => {
        const dataType = columnDescription[el]?.data_type || "String";
        const dbType = columnDescription[el]?.column_dbtype || "String";
        return {
          value: el,
          label: el,
          dataType: dataType,
          dbType: dbType,
        };
      });

      setColumnOptions((prev) => ({
        ...prev,
        [branchId]: {
          ...prev[branchId],
          [type]: options,
          [`${type}Desc`]: columnDescription,
        },
      }));
    } catch (error) {
      console.error(error);
    }
  };

  // Source-level filter handlers
  const handleAddSourceFilter = () => {
    if (sourceFilters.length >= MAX_FILTERS_PER_BRANCH) {
      setMergeWarning(`Maximum ${MAX_FILTERS_PER_BRANCH} filters allowed`);
      if (warningTimeoutRef.current) clearTimeout(warningTimeoutRef.current);
      warningTimeoutRef.current = setTimeout(() => setMergeWarning(""), 3000);
      return;
    }

    const newFilter = {
      id: generateKey(),
      column: null,
      column_type: null,
      operator: null,
      rhs_type: "VALUE",
      rhs_value: null,
      rhs_column: null,
      logical_operator: sourceFilters.length > 0 ? "AND" : null,
    };

    setSourceFilters([...sourceFilters, newFilter]);
    setIsModified(true);
  };

  const handleSourceFilterChange = (
    filterId,
    field,
    value,
    additionalUpdates = {}
  ) => {
    setSourceFilters(
      sourceFilters.map((filter) =>
        filter.id === filterId
          ? { ...filter, [field]: value, ...additionalUpdates }
          : filter
      )
    );
    setIsModified(true);
  };

  const handleDeleteSourceFilter = (filterId) => {
    setSourceFilters(sourceFilters.filter((f) => f.id !== filterId));
    setIsModified(true);
  };

  // Add branch
  const handleAddBranch = () => {
    // Auto-fill columns from first branch if it exists
    const firstBranch = branches[0];
    const autoColumns = firstBranch
      ? firstBranch.columns.map((col) => ({
          id: generateKey(),
          source_column: col.source_column,
          merge_column: null,
          alias: col.alias,
          expression_type: "COLUMN",
          literal_value: null,
          literal_type: "String",
          cast_type: col.cast_type || null,
        }))
      : [];

    const newBranch = {
      id: generateKey(),
      branch_id: branches.length + 1,
      source_table: defaultSource,
      source_schema: defaultSchema,
      merge_table: null,
      merge_schema: defaultSchema, // Set default schema
      columns: autoColumns,
      filters: [],
    };
    setBranches([...branches, newBranch]);

    setIsModified(true);
  };

  // Delete branch
  const handleDeleteBranch = (id) => {
    setBranches(branches.filter((branch) => branch.id !== id));
    setIsModified(true);
  };

  // Update branch table (handles combined "schema.table" format)
  const handleBranchTableChange = (branchId, value) => {
    // Split "schema.table" format
    const [schema, table] = value.split(".");

    if (!table) return; // Invalid format

    setBranches(
      branches.map((branch) =>
        branch.id === branchId
          ? {
              ...branch,
              merge_table: table,
              merge_schema: schema,
              // Reset columns and filters when table changes
              columns: branch.columns.map((col) => ({
                ...col,
                merge_column: null,
              })),
              filters: [],
            }
          : branch
      )
    );

    // Fetch columns for new merge table
    if (schema && table) {
      fetchColumns(schema, table, branchId, "merge");
    }

    setIsModified(true);
  };

  // // Helper to get abbreviated data type
  // const getDataTypeAbbr = (dataType) => {
  //   const typeMap = {
  //     String: "S",
  //     Number: "N",
  //     Integer: "N",
  //     Float: "N",
  //     Double: "N",
  //     Date: "DT",
  //     DateTime: "DT",
  //     Timestamp: "DT",
  //     Time: "T",
  //     Boolean: "B",
  //     Bool: "B",
  //   };
  //   return typeMap[dataType] || "S";
  // };

  // Add column mapping
  const handleAddColumnMapping = (branchId) => {
    // Get alias from previous branch's same column index if available
    const currentBranch = branches.find((b) => b.id === branchId);
    const currentIndex = currentBranch?.columns.length || 0;

    // Try to get alias from first branch (branch index 0)
    const firstBranch = branches[0];
    const autoAlias = firstBranch?.columns[currentIndex]?.alias || null;

    const newMapping = {
      id: generateKey(),
      source_column: null,
      merge_column: null,
      alias: autoAlias,
      expression_type: "COLUMN",
      literal_value: null,
      literal_type: "String",
      cast_type: null,
    };

    setBranches(
      branches.map((branch) =>
        branch.id === branchId
          ? { ...branch, columns: [...branch.columns, newMapping] }
          : branch
      )
    );
    setIsModified(true);
  };

  // Update column mapping
  const handleColumnMappingChange = (branchId, mappingId, field, value) => {
    setBranches((prevBranches) =>
      prevBranches.map((branch) =>
        branch.id === branchId
          ? {
              ...branch,
              columns: branch.columns.map((col) =>
                col.id === mappingId ? { ...col, [field]: value } : col
              ),
            }
          : branch
      )
    );
    setIsModified(true);
  };

  // Delete column mapping
  const handleDeleteColumnMapping = (branchId, mappingId) => {
    setBranches(
      branches.map((branch) =>
        branch.id === branchId
          ? {
              ...branch,
              columns: branch.columns.filter((col) => col.id !== mappingId),
            }
          : branch
      )
    );
    setIsModified(true);
  };

  // Add filter
  const handleAddFilter = (branchId) => {
    const branch = branches.find((b) => b.id === branchId);
    if (!branch) return;

    if (branch.filters.length >= MAX_FILTERS_PER_BRANCH) {
      setMergeWarning(
        `Maximum ${MAX_FILTERS_PER_BRANCH} filters allowed per branch`
      );
      if (warningTimeoutRef.current) clearTimeout(warningTimeoutRef.current);
      warningTimeoutRef.current = setTimeout(() => setMergeWarning(""), 3000);
      return;
    }

    const newFilter = {
      id: generateKey(),
      column: null,
      column_type: null,
      operator: null,
      rhs_type: "VALUE",
      rhs_value: null,
      rhs_column: null,
      logical_operator: branch.filters.length > 0 ? "AND" : null,
    };

    setBranches(
      branches.map((b) =>
        b.id === branchId ? { ...b, filters: [...b.filters, newFilter] } : b
      )
    );
    setIsModified(true);
  };

  // Update filter
  const handleFilterChange = (
    branchId,
    filterId,
    field,
    value,
    additionalUpdates = {}
  ) => {
    setBranches(
      branches.map((branch) =>
        branch.id === branchId
          ? {
              ...branch,
              filters: branch.filters.map((filter) =>
                filter.id === filterId
                  ? { ...filter, [field]: value, ...additionalUpdates }
                  : filter
              ),
            }
          : branch
      )
    );
    setIsModified(true);
  };

  // Delete filter
  const handleDeleteFilter = (branchId, filterId) => {
    setBranches(
      branches.map((branch) =>
        branch.id === branchId
          ? {
              ...branch,
              filters: branch.filters.filter((f) => f.id !== filterId),
            }
          : branch
      )
    );
    setIsModified(true);
  };

  // Get merge columns filtered by source column datatype
  const getMergeColumnsByType = (branchId, sourceColumn) => {
    const options = columnOptions[branchId];
    if (!options || !sourceColumn) return [];

    // Get source column data type from top-level sourceColumnDesc
    const sourceDataType = sourceColumnDesc[sourceColumn]?.data_type;

    if (!sourceDataType) return options.merge || [];

    // Filter merge columns by same datatype
    return (options.merge || []).filter(
      (col) => col.dataType === sourceDataType
    );
  };

  // Format validation errors into readable messages
  const getValidationErrorMessages = () => {
    const messages = [];

    // Handle source filter errors
    if (validationErrors._sourceFilters) {
      validationErrors._sourceFilters.forEach((filterError) => {
        const filterNum = filterError.index + 1;
        const errs = filterError.errors;
        if (errs.column) {
          messages.push(`Source Filter ${filterNum}: ${errs.column}`);
        }
        if (errs.operator) {
          messages.push(`Source Filter ${filterNum}: ${errs.operator}`);
        }
        if (errs.value) {
          messages.push(`Source Filter ${filterNum}: ${errs.value}`);
        }
      });
    }

    Object.keys(validationErrors)
      .filter((key) => key !== "_sourceFilters")
      .forEach((branchId, branchIndex) => {
        const branchErrors = validationErrors[branchId];
        const branchNum = branchIndex + 1;

        if (branchErrors.table) {
          messages.push(`Branch ${branchNum}: ${branchErrors.table}`);
        }
        if (branchErrors.columnCount) {
          messages.push(`Branch ${branchNum}: ${branchErrors.columnCount}`);
        }
        if (branchErrors.columns) {
          messages.push(`Branch ${branchNum}: ${branchErrors.columns}`);
        }
        if (branchErrors.columnMappings) {
          Object.keys(branchErrors.columnMappings).forEach(
            (colId, colIndex) => {
              const colErrors = branchErrors.columnMappings[colId];
              if (colErrors.merge_column) {
                messages.push(
                  `Branch ${branchNum}, Column ${
                    colIndex + 1
                  }: Merge column is required`
                );
              }
              if (colErrors.literal_value) {
                messages.push(
                  `Branch ${branchNum}, Column ${
                    colIndex + 1
                  }: Literal value is required`
                );
              }
              if (colErrors.alias) {
                messages.push(
                  `Branch ${branchNum}, Column ${colIndex + 1}: ${
                    colErrors.alias
                  }`
                );
              }
            }
          );
        }
        // Handle branch filter errors
        if (branchErrors.filters) {
          branchErrors.filters.forEach((filterError) => {
            const filterNum = filterError.index + 1;
            const errs = filterError.errors;
            if (errs.column) {
              messages.push(
                `Branch ${branchNum}, Filter ${filterNum}: ${errs.column}`
              );
            }
            if (errs.operator) {
              messages.push(
                `Branch ${branchNum}, Filter ${filterNum}: ${errs.operator}`
              );
            }
            if (errs.value) {
              messages.push(
                `Branch ${branchNum}, Filter ${filterNum}: ${errs.value}`
              );
            }
          });
        }
      });

    return messages;
  };

  // Serialize for backend
  const serializeForBackend = () => {
    // Collect all unique output columns
    const outputColumnsSet = new Set();
    branches.forEach((branch) => {
      branch.columns.forEach((col) => {
        // Use source column as default if alias is empty
        const columnName =
          col.alias || col.source_column || col.merge_column || "column";
        outputColumnsSet.add(columnName);
      });
    });

    const output_columns = Array.from(outputColumnsSet).map((name) => ({
      column_name: name,
      data_type: "String", // Could be inferred from source column
    }));

    // Build source table branch (branch 0) - select from source table
    // Use first branch's source_column mappings to determine which columns to select from source
    const sourceBranch =
      branches.length > 0
        ? {
            branch_id: 0,
            table: defaultSource,
            schema: defaultSchema,
            columns: branches[0].columns.map((col) => ({
              output_column: col.alias || col.source_column, // Use source column as default if alias is empty
              expression_type: "COLUMN",
              column_name: col.source_column, // Select source column from source table
              literal_value: null,
              literal_type: null,
            })),
            filters: undefined, // Source filters are applied at top level
          }
        : null;

    // Build user-created branches (renumber them starting from 1)
    const userBranches = branches.map((branch, idx) => ({
      branch_id: idx + 1, // Start from 1 since source is 0
      table: branch.merge_table,
      schema: branch.merge_schema,
      columns: branch.columns.map((col) => {
        // Use source column as default if alias is empty
        const defaultAlias = col.source_column || col.merge_column || "column";
        const columnData = {
          output_column: col.alias || defaultAlias,
          expression_type: col.expression_type,
          column_name:
            col.expression_type === "COLUMN" ? col.merge_column : null,
          literal_value:
            col.expression_type === "LITERAL" ? col.literal_value : null,
          literal_type:
            col.expression_type === "LITERAL" ? col.literal_type : null,
        };
        // Add cast_type if specified
        if (col.cast_type) {
          columnData.cast_type = col.cast_type;
        }
        return columnData;
      }),
      filters:
        branch.filters.length > 0
          ? branch.filters.map(({ id, ...filter }) => filter)
          : undefined,
    }));

    // Prepend source branch to user branches
    const allBranches = sourceBranch
      ? [sourceBranch, ...userBranches]
      : userBranches;

    const serialized = {
      ignore_duplicate: ignoreDupe,
      output_columns: output_columns,
      source_filters:
        sourceFilters.length > 0
          ? sourceFilters.map(({ id, ...filter }) => filter)
          : undefined,
      branches: allBranches,
    };

    return serialized;
  };

  // Save
  const handleSave = async () => {
    // Clear previous validation errors
    setValidationErrors({});

    // If no branches, delete the UNION transformation (save as empty = remove)
    if (branches.length === 0) {
      try {
        const body = {
          step_id: transformIds?.UNION,
        };
        const result = await handleDeleteTransformation(body);

        if (result?.status === "success") {
          setOpen(false);
          setIsModified(false);
          updateLabel(0);
          updateSpec(result?.spec);
          setHasExistingTransformation(false);
        } else {
          notify({
            type: "error",
            message: "Delete Failed",
            description: result?.message || "Failed to remove transformation",
          });
        }
        return;
      } catch (error) {
        console.error("Error during delete:", error);
        notify({
          type: "error",
          message: "Delete Failed",
          description: error?.message || "Failed to remove transformation",
        });
        return;
      }
    }

    // Note: Source table + 1 branch = 2 tables for UNION, so minimum 1 branch is OK

    // Validation - collect all errors
    const errors = {};

    // Validate source filters
    const sourceFilterErrors = [];
    for (let i = 0; i < sourceFilters.length; i++) {
      const filter = sourceFilters[i];
      const filterErrors = {};

      if (!filter.column) {
        filterErrors.column = "Column is required";
      }
      if (!filter.operator) {
        filterErrors.operator = "Operator is required";
      }
      // Check if value is required (not for NULL/NOTNULL/TRUE/FALSE operators)
      if (
        filter.operator &&
        !NULL_OPERATORS.includes(filter.operator) &&
        !filter.rhs_value
      ) {
        filterErrors.value = "Value is required";
      }

      if (Object.keys(filterErrors).length > 0) {
        sourceFilterErrors.push({ index: i, errors: filterErrors });
      }
    }
    if (sourceFilterErrors.length > 0) {
      errors._sourceFilters = sourceFilterErrors;
    }

    for (const branch of branches) {
      const branchErrors = {};

      if (!branch.merge_table) {
        branchErrors.table = "Please select a merge table";
      }
      if (branch.columns.length === 0) {
        branchErrors.columns = "Please add at least one column mapping";
      } else {
        // Validate each column mapping
        const columnErrors = {};
        for (const col of branch.columns) {
          const colErrors = {};
          if (col.expression_type === "COLUMN" && !col.merge_column) {
            colErrors.merge_column = "Required";
          }
          if (col.expression_type === "LITERAL" && !col.literal_value) {
            colErrors.literal_value = "Required";
          }
          if (Object.keys(colErrors).length > 0) {
            columnErrors[col.id] = colErrors;
          }
        }
        if (Object.keys(columnErrors).length > 0) {
          branchErrors.columnMappings = columnErrors;
        }
      }

      // Validate branch filters
      const branchFilterErrors = [];
      for (let i = 0; i < (branch.filters || []).length; i++) {
        const filter = branch.filters[i];
        const filterErrors = {};

        if (!filter.column) {
          filterErrors.column = "Column is required";
        }
        if (!filter.operator) {
          filterErrors.operator = "Operator is required";
        }
        // Check if value is required (not for NULL/NOTNULL/TRUE/FALSE operators)
        if (
          filter.operator &&
          !NULL_OPERATORS.includes(filter.operator) &&
          !filter.rhs_value
        ) {
          filterErrors.value = "Value is required";
        }

        if (Object.keys(filterErrors).length > 0) {
          branchFilterErrors.push({ index: i, errors: filterErrors });
        }
      }
      if (branchFilterErrors.length > 0) {
        branchErrors.filters = branchFilterErrors;
      }

      if (Object.keys(branchErrors).length > 0) {
        errors[branch.id] = branchErrors;
      }
    }

    // Validate alias consistency across branches
    if (branches.length > 1) {
      const firstBranchAliases = branches[0].columns.map((col) => col.alias);
      const firstBranchCount = firstBranchAliases.length;

      for (let i = 1; i < branches.length; i++) {
        const currentBranchAliases = branches[i].columns.map(
          (col) => col.alias
        );
        const currentBranchCount = currentBranchAliases.length;

        // Check same number of columns
        if (currentBranchCount !== firstBranchCount) {
          if (!errors[branches[i].id]) errors[branches[i].id] = {};
          errors[
            branches[i].id
          ].columnCount = `Must have ${firstBranchCount} columns (same as Branch 1), currently has ${currentBranchCount}`;
        } else {
          // Only check alias consistency if column counts match
          // (order matters for UNION)
          for (let j = 0; j < firstBranchCount; j++) {
            if (firstBranchAliases[j] !== currentBranchAliases[j]) {
              if (!errors[branches[i].id]) errors[branches[i].id] = {};
              if (!errors[branches[i].id].columnMappings)
                errors[branches[i].id].columnMappings = {};
              if (
                !errors[branches[i].id].columnMappings[
                  branches[i].columns[j].id
                ]
              )
                errors[branches[i].id].columnMappings[
                  branches[i].columns[j].id
                ] = {};
              errors[branches[i].id].columnMappings[
                branches[i].columns[j].id
              ].alias = `Must match Branch 1: "${firstBranchAliases[j]}"`;
            }
          }
        }
      }
    }

    // If there are validation errors, set them and return
    if (Object.keys(errors).length > 0) {
      setValidationErrors(errors);
      return;
    }

    let result = {};
    const body = {
      type: "union",
      union: serializeForBackend(),
    };

    try {
      result = await saveTransformation(body, transformIds?.UNION);

      if (result?.status === "success") {
        updateLabel(branches.length);
        setOpen(false);
        updateSpec(result?.spec);
        setIsModified(false);
        setHasExistingTransformation(true);
      } else {
        console.error("Save failed with result:", result);
        const errorMessage =
          result?.message || result?.error || "Save failed with unknown error";
        notify({
          type: "error",
          message: "Save Failed",
          description: errorMessage,
        });
        setOpen(true);
      }
    } catch (error) {
      console.error("Error during save:", error);
      notify({
        type: "error",
        message: "Save Failed",
        description:
          error?.response?.data?.message ||
          error?.message ||
          "Failed to save transformation",
      });
      setOpen(true);
    }
  };

  const updateLabel = (count) => {
    const plural = count > 1 ? "s" : "";
    const newLabel = count ? `Merged by ${count} branch${plural}` : "Merge";
    setLabel(newLabel);
  };

  // Handle delete transformation
  const handleDelete = async () => {
    try {
      const body = {
        step_id: transformIds?.UNION,
      };
      const result = await handleDeleteTransformation(body);

      if (result?.status === "success") {
        updateSpec(result?.spec);
        updateLabel(0);
        setOpen(false);
        setBranches([]);
        setSourceFilters([]);
        setIsModified(false);
        setHasExistingTransformation(false);
      }
    } catch (error) {
      console.error(error);
      notify({
        type: "error",
        message: "Delete Failed",
        description: error?.message || "Failed to delete transformation",
      });
    }
  };

  // Load from spec
  useEffect(() => {
    const unionSpec = getUnionSpec(spec?.transform, transformIds?.UNION);

    if (unionSpec && unionSpec.branches && unionSpec.output_columns) {
      // New branch-based format
      setIgnoreDupe(unionSpec.ignore_duplicate || false);

      // Load source-level filters
      if (unionSpec.source_filters) {
        setSourceFilters(
          unionSpec.source_filters.map((filter) => ({
            ...filter,
            id: generateKey(),
          }))
        );
      } else {
        setSourceFilters([]);
      }

      // Get source columns from branch 0 (if exists)
      const branch0 = unionSpec.branches.find((b) => b.branch_id === 0);
      const sourceColumnMapping = branch0 ? branch0.columns : [];

      // Load user branches (skip branch 0 which is auto-generated source branch)
      const userBranches = unionSpec.branches.filter(
        (branch) => branch.branch_id !== 0
      );

      const loadedBranches = userBranches.map((branch) => ({
        id: generateKey(),
        branch_id: branch.branch_id,
        source_table: defaultSource,
        source_schema: defaultSchema,
        merge_table: branch.table,
        merge_schema: branch.schema,
        columns: branch.columns.map((col, colIdx) => ({
          id: generateKey(),
          // Reconstruct source_column from branch 0's column_name
          source_column: sourceColumnMapping[colIdx]?.column_name || null,
          merge_column:
            col.expression_type === "COLUMN" ? col.column_name : null,
          alias: col.output_column,
          expression_type: col.expression_type,
          literal_value: col.literal_value,
          literal_type: col.literal_type || "String",
          cast_type: col.cast_type || null,
        })),
        filters: (branch.filters || []).map((filter) => ({
          ...filter,
          id: generateKey(),
        })),
      }));

      setBranches(loadedBranches);

      // Fetch columns for each loaded branch's merge table
      loadedBranches.forEach((branch) => {
        if (branch.merge_table && branch.merge_schema) {
          fetchColumns(
            branch.merge_schema,
            branch.merge_table,
            branch.id,
            "merge"
          );
        }
      });

      // Update label with user branch count (not including branch 0)
      updateLabel(userBranches.length);
      setHasExistingTransformation(true);
    } else {
      // Empty or legacy format
      setBranches([]);
      setSourceFilters([]);
      setHasExistingTransformation(false);
    }
  }, [spec, transformIds?.UNION]);

  // Render a single branch
  const renderBranch = (branch, index) => {
    const branchOptions = columnOptions[branch.id] || {};
    const mergeColumns = branchOptions.merge || [];
    const mergeDesc = branchOptions.mergeDesc || {};
    // const branchErrors = validationErrors[branch.id] || {};

    return (
      <div
        key={branch.id}
        style={{
          marginBottom: 16,
          padding: 16,
          border: `1px solid ${isDarkTheme ? "#434343" : "#d9d9d9"}`,
          borderRadius: 8,
        }}
      >
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            marginBottom: 16,
          }}
        >
          <Title level={5} style={{ margin: 0 }}>
            Branch {index + 1}
          </Title>
          <Button
            type="text"
            danger
            icon={<DeleteOutlined />}
            onClick={() => handleDeleteBranch(branch.id)}
          />
        </div>

        {/* Merge Table Selection - Combined Schema/Table dropdown */}
        <div style={{ marginBottom: 16 }}>
          <Text strong>Merge Table:</Text>
          <Select
            placeholder="Schema/Table"
            value={
              branch.merge_schema && branch.merge_table
                ? `${branch.merge_schema}.${branch.merge_table}`
                : undefined
            }
            onChange={(value) => handleBranchTableChange(branch.id, value)}
            style={{ width: "100%", marginTop: 4 }}
            showSearch
            filterOption={(input, option) =>
              option.children.toLowerCase().indexOf(input.toLowerCase()) >= 0
            }
            allowClear
          >
            {allTables
              ?.map((table) => {
                // Exclude source table from options
                const sourceTableName = isSchemaExists
                  ? `${defaultSchema}.${defaultSource}`
                  : defaultSource;
                if (table === sourceTableName) {
                  return null;
                }
                return (
                  <Select.Option key={table} value={table}>
                    {table}
                  </Select.Option>
                );
              })
              .filter(Boolean)}
          </Select>
        </div>

        {/* Column Mappings */}
        <div style={{ marginBottom: 16 }}>
          <div
            style={{
              display: "flex",
              justifyContent: "space-between",
              marginBottom: 8,
            }}
          >
            <Text strong>Column Mappings:</Text>
            <Button
              type="dashed"
              size="small"
              icon={<PlusOutlined />}
              onClick={() => handleAddColumnMapping(branch.id)}
              disabled={!branch.merge_table}
            >
              Add Mapping
            </Button>
          </div>

          {branch.columns.length === 0 && (
            <Text type="secondary" style={{ fontStyle: "italic" }}>
              No column mappings. Click &quot;Add Mapping&quot; to start.
            </Text>
          )}

          {branch.columns.map((col) => {
            // const colErrors = branchErrors.columnMappings?.[col.id] || {};
            return (
              <div
                key={col.id}
                style={{
                  display: "flex",
                  gap: 8,
                  marginBottom: 8,
                  alignItems: "flex-end",
                  background: isDarkTheme ? "#1f1f1f" : "#fafafa",
                  padding: 8,
                  borderRadius: 4,
                }}
              >
                {/* Source Column (from top-level source table) */}
                <div style={{ width: 180 }}>
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    Source Column
                  </Text>
                  <Select
                    placeholder="From source table"
                    value={col.source_column}
                    onChange={(value) =>
                      handleColumnMappingChange(
                        branch.id,
                        col.id,
                        "source_column",
                        value
                      )
                    }
                    style={{ width: "100%" }}
                    size="small"
                    showSearch
                    optionFilterProp="children"
                  >
                    {sourceColumns.map((opt) => (
                      <Select.Option
                        key={opt.value}
                        value={opt.value}
                        title={`${opt.label} (${opt.dataType})`}
                      >
                        {opt.label}
                      </Select.Option>
                    ))}
                  </Select>
                </div>

                {/* Merge Column or Literal */}
                <div style={{ width: 180 }}>
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    {col.expression_type === "LITERAL"
                      ? "Literal Value"
                      : "Merge Column"}
                  </Text>
                  {col.expression_type === "COLUMN" ? (
                    <>
                      <Select
                        placeholder="From merge table"
                        value={col.merge_column}
                        onChange={(value) =>
                          handleColumnMappingChange(
                            branch.id,
                            col.id,
                            "merge_column",
                            value
                          )
                        }
                        style={{ width: "100%" }}
                        size="small"
                        disabled={!branch.merge_table}
                        showSearch
                        optionFilterProp="children"
                      >
                        {getMergeColumnsByType(
                          branch.id,
                          col.source_column
                        ).map((opt) => (
                          <Select.Option
                            key={opt.value}
                            value={opt.value}
                            title={`${opt.label} (${opt.dataType})`}
                          >
                            {opt.label}
                          </Select.Option>
                        ))}
                      </Select>
                    </>
                  ) : (
                    <>
                      <Input
                        placeholder="Enter literal value"
                        value={col.literal_value}
                        onChange={(e) =>
                          handleColumnMappingChange(
                            branch.id,
                            col.id,
                            "literal_value",
                            e.target.value
                          )
                        }
                        size="small"
                      />
                    </>
                  )}
                </div>

                {/* Alias (Output column name) */}
                <div style={{ width: 180 }}>
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    Alias (AS)
                  </Text>
                  <Input
                    placeholder="Output name"
                    value={col.alias}
                    onChange={(e) =>
                      handleColumnMappingChange(
                        branch.id,
                        col.id,
                        "alias",
                        e.target.value
                      )
                    }
                    size="small"
                    title={col.alias}
                  />
                </div>

                {/* Type */}
                <div style={{ width: 90 }}>
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    Type
                  </Text>
                  <Select
                    value={col.expression_type}
                    onChange={(value) => {
                      handleColumnMappingChange(
                        branch.id,
                        col.id,
                        "expression_type",
                        value
                      );
                      // Reset values
                      handleColumnMappingChange(
                        branch.id,
                        col.id,
                        "merge_column",
                        null
                      );
                      handleColumnMappingChange(
                        branch.id,
                        col.id,
                        "literal_value",
                        null
                      );
                    }}
                    style={{ width: "100%" }}
                    size="small"
                  >
                    <Select.Option value="COLUMN">COLUMN</Select.Option>
                    <Select.Option value="LITERAL">LITERAL</Select.Option>
                  </Select>
                </div>

                {/* Cast To */}
                <div style={{ width: 90 }}>
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    Cast To
                  </Text>
                  <Select
                    placeholder="None"
                    value={col.cast_type}
                    onChange={(value) =>
                      handleColumnMappingChange(
                        branch.id,
                        col.id,
                        "cast_type",
                        value
                      )
                    }
                    style={{ width: "100%" }}
                    size="small"
                    allowClear
                  >
                    <Select.Option value="VARCHAR">VARCHAR</Select.Option>
                    <Select.Option value="INTEGER">INTEGER</Select.Option>
                    <Select.Option value="BIGINT">BIGINT</Select.Option>
                    <Select.Option value="FLOAT">FLOAT</Select.Option>
                    <Select.Option value="DECIMAL">DECIMAL</Select.Option>
                    <Select.Option value="BOOLEAN">BOOLEAN</Select.Option>
                    <Select.Option value="DATE">DATE</Select.Option>
                    <Select.Option value="TIMESTAMP">TIMESTAMP</Select.Option>
                  </Select>
                </div>

                <Button
                  type="text"
                  danger
                  icon={<DeleteOutlined />}
                  onClick={() => handleDeleteColumnMapping(branch.id, col.id)}
                  size="small"
                  title="Delete"
                  style={{ marginBottom: 0 }}
                />
              </div>
            );
          })}
        </div>

        {/* Filters */}
        <div>
          <div
            style={{
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
              marginBottom: 8,
            }}
          >
            <Text strong>Filters</Text>
            <Button
              type="text"
              size="small"
              icon={<FilterOutlined />}
              onClick={() => handleAddFilter(branch.id)}
              title="Add Filter"
            />
          </div>
          {branch.filters.length > 0 && (
            <div style={{ paddingLeft: 16 }}>
              {branch.filters.map((filter, fIdx) => (
                <div
                  key={filter.id}
                  style={{
                    marginBottom: 8,
                    display: "flex",
                    gap: 8,
                    alignItems: "center",
                  }}
                >
                  {fIdx > 0 && (
                    <Select
                      value={
                        branch.filters[fIdx - 1]?.logical_operator || "AND"
                      }
                      onChange={(value) =>
                        handleFilterChange(
                          branch.id,
                          branch.filters[fIdx - 1].id,
                          "logical_operator",
                          value
                        )
                      }
                      style={{ width: 80 }}
                      size="small"
                    >
                      <Select.Option value="AND">AND</Select.Option>
                      <Select.Option value="OR">OR</Select.Option>
                    </Select>
                  )}
                  <Select
                    placeholder="Column"
                    value={filter.column}
                    onChange={(value) => {
                      const dataType = mergeDesc[value]?.data_type || "String";
                      // Update both column and column_type in single state update
                      handleFilterChange(
                        branch.id,
                        filter.id,
                        "column",
                        value,
                        {
                          column_type: dataType,
                        }
                      );
                    }}
                    style={{ width: 150 }}
                    size="small"
                  >
                    {mergeColumns.map((opt) => (
                      <Select.Option key={opt.value} value={opt.value}>
                        {opt.label}
                      </Select.Option>
                    ))}
                  </Select>
                  <Select
                    placeholder="Operator"
                    value={filter.operator}
                    onChange={(value) =>
                      handleFilterChange(
                        branch.id,
                        filter.id,
                        "operator",
                        value
                      )
                    }
                    style={{ width: 120 }}
                    size="small"
                  >
                    {getOperators(null, filter.column_type).map((op) => (
                      <Select.Option key={op.value} value={op.value}>
                        {op.label}
                      </Select.Option>
                    ))}
                  </Select>
                  {!NULL_OPERATORS.includes(filter.operator) && (
                    <Input
                      placeholder="Value"
                      value={filter.rhs_value}
                      onChange={(e) =>
                        handleFilterChange(
                          branch.id,
                          filter.id,
                          "rhs_value",
                          e.target.value
                        )
                      }
                      style={{ flex: 1 }}
                      size="small"
                    />
                  )}
                  <Button
                    type="text"
                    danger
                    icon={<DeleteOutlined />}
                    onClick={() => handleDeleteFilter(branch.id, filter.id)}
                    size="small"
                    title="Delete"
                  />
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    );
  };

  return (
    <ToolbarItem
      icon={
        isDarkTheme ? (
          <MergeDarkIcon className="toolbar-item-icon" />
        ) : (
          <MergeLightIcon className="toolbar-item-icon" />
        )
      }
      label={label}
      disabled={disabled}
      open={open}
      setOpen={setOpen}
      handleOpenChange={handleOpenChange}
      step={step}
      className={
        branches.length > 0 ? "no-code-toolbar-hidden-cols-highlight" : ""
      }
    >
      <div className="ml-10 width-600">
        <Title level={5} className="m-0 draggable-title">
          Merge
        </Title>

        {mergeWarning && (
          <AnimatePresence>
            <motion.div
              initial={{ opacity: 0, y: -10 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0 }}
              style={{
                padding: 12,
                marginBottom: 16,
                background: "#fff3cd",
                border: "1px solid #ffc107",
                borderRadius: 4,
                color: "#856404",
              }}
            >
              {mergeWarning}
            </motion.div>
          </AnimatePresence>
        )}

        {/* Source Table Section (Top Level) */}
        <div
          style={{
            marginBottom: 16,
            padding: 16,
            border: `2px solid ${isDarkTheme ? "#1890ff" : "#1890ff"}`,
            borderRadius: 8,
            background: isDarkTheme ? "#0d1117" : "#f0f8ff",
          }}
        >
          <div
            style={{
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
              marginBottom: 12,
            }}
          >
            <div>
              <Text strong style={{ fontSize: 14 }}>
                Source Table:{" "}
              </Text>
              <Text style={{ fontSize: 14 }}>
                {isSchemaExists
                  ? `${defaultSchema}.${defaultSource}`
                  : defaultSource}
              </Text>
            </div>
            <Button
              type="text"
              size="small"
              icon={<FilterOutlined />}
              onClick={handleAddSourceFilter}
              title="Add Source Filter"
            />
          </div>

          {/* Source-level filters */}
          {sourceFilters.length > 0 && (
            <div style={{ paddingLeft: 16 }}>
              <Text
                type="secondary"
                style={{ fontSize: 12, marginBottom: 8, display: "block" }}
              >
                Common filters applied to source table before merge:
              </Text>
              {sourceFilters.map((filter, fIdx) => (
                <div
                  key={filter.id}
                  style={{
                    marginBottom: 8,
                    display: "flex",
                    gap: 8,
                    alignItems: "center",
                  }}
                >
                  {fIdx > 0 && (
                    <Select
                      value={sourceFilters[fIdx - 1]?.logical_operator || "AND"}
                      onChange={(value) =>
                        handleSourceFilterChange(
                          sourceFilters[fIdx - 1].id,
                          "logical_operator",
                          value
                        )
                      }
                      style={{ width: 80 }}
                      size="small"
                    >
                      <Select.Option value="AND">AND</Select.Option>
                      <Select.Option value="OR">OR</Select.Option>
                    </Select>
                  )}
                  <Select
                    placeholder="Column"
                    value={filter.column}
                    onChange={(value) => {
                      const dataType =
                        sourceColumnDesc[value]?.data_type || "String";
                      // Update both column and column_type in single state update
                      handleSourceFilterChange(filter.id, "column", value, {
                        column_type: dataType,
                      });
                    }}
                    style={{ width: 150 }}
                    size="small"
                  >
                    {sourceColumns.map((opt) => (
                      <Select.Option key={opt.value} value={opt.value}>
                        {opt.label}
                      </Select.Option>
                    ))}
                  </Select>
                  <Select
                    placeholder="Operator"
                    value={filter.operator}
                    onChange={(value) =>
                      handleSourceFilterChange(filter.id, "operator", value)
                    }
                    style={{ width: 120 }}
                    size="small"
                  >
                    {getOperators(null, filter.column_type).map((op) => (
                      <Select.Option key={op.value} value={op.value}>
                        {op.label}
                      </Select.Option>
                    ))}
                  </Select>
                  {!NULL_OPERATORS.includes(filter.operator) && (
                    <Input
                      placeholder="Value"
                      value={filter.rhs_value}
                      onChange={(e) =>
                        handleSourceFilterChange(
                          filter.id,
                          "rhs_value",
                          e.target.value
                        )
                      }
                      style={{ flex: 1 }}
                      size="small"
                    />
                  )}
                  <Button
                    type="text"
                    danger
                    icon={<DeleteOutlined />}
                    onClick={() => handleDeleteSourceFilter(filter.id)}
                    size="small"
                    title="Delete"
                  />
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Branches Section */}
        <div style={{ marginBottom: 16 }}>
          <Title level={5} style={{ marginBottom: 12 }}>
            Merge Branches
          </Title>
          {branches.length === 0 && (
            <Text
              type="secondary"
              style={{ display: "block", marginBottom: 12 }}
            >
              No branches defined. Click &quot;Add Branch&quot; to start.
            </Text>
          )}
          {branches.map((branch, index) => renderBranch(branch, index))}
          <Button
            type="dashed"
            icon={<PlusOutlined />}
            onClick={handleAddBranch}
            block
          >
            Add Branch
          </Button>

          {/* Consolidated Validation Errors */}
          {Object.keys(validationErrors).length > 0 && (
            <div
              style={{
                marginTop: 16,
                padding: 12,
                background: isDarkTheme ? "#2a1215" : "#fff2f0",
                border: `1px solid ${isDarkTheme ? "#58181c" : "#ffccc7"}`,
                borderRadius: 4,
              }}
            >
              <Text
                type="danger"
                strong
                style={{ display: "block", marginBottom: 8 }}
              >
                Please fix the following errors:
              </Text>
              <ul
                style={{
                  margin: 0,
                  paddingLeft: 20,
                  color: isDarkTheme ? "#ff7875" : "#ff4d4f",
                }}
              >
                {getValidationErrorMessages().map((msg, idx) => (
                  <li key={idx} style={{ marginBottom: 4 }}>
                    <Text type="danger">{msg}</Text>
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>

        <div
          className="flex-end-container"
          style={{ alignItems: "center", justifyContent: "space-between" }}
        >
          <Checkbox
            checked={ignoreDupe}
            onChange={(e) => {
              setIgnoreDupe(e.target.checked);
              setIsModified(true);
            }}
          >
            Ignore Duplicates (UNION vs UNION ALL)
          </Checkbox>
          <Space>
            <Button onClick={() => setOpen(false)} disabled={isLoading}>
              Cancel
            </Button>
            <Popconfirm
              title="Delete Merge"
              description="Are you sure you want to delete this merge? This action cannot be undone."
              onConfirm={handleDelete}
              okText="Delete"
              cancelText="Cancel"
              okButtonProps={{ danger: true }}
            >
              <Button danger disabled={isLoading || !hasExistingTransformation}>
                Delete
              </Button>
            </Popconfirm>
            <Button
              type="primary"
              onClick={() => {
                handleSave();
              }}
              disabled={!isModified}
              loading={isLoading}
            >
              Save
            </Button>
          </Space>
        </div>
      </div>
    </ToolbarItem>
  );
};

Merge.propTypes = {
  disabled: PropTypes.bool,
  spec: PropTypes.object,
  updateSpec: PropTypes.func,
  allColumns: PropTypes.object,
  step: PropTypes.array,
  modelName: PropTypes.string,
  isDarkTheme: PropTypes.bool,
  saveTransformation: PropTypes.func,
  handleDeleteTransformation: PropTypes.func,
  isLoading: PropTypes.bool,
  handleGetColumns: PropTypes.func,
};

export { Merge };
