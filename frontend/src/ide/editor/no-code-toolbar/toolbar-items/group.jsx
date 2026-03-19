import React, { useEffect, useState, useMemo, useCallback } from "react";
import {
  Select,
  Button,
  Space,
  Typography,
  Input,
  Card,
  Popconfirm,
  Modal,
  Tooltip,
  Alert,
} from "antd";
import {
  DeleteOutlined,
  PlusOutlined,
  FunctionOutlined,
  WarningOutlined,
} from "@ant-design/icons";
import PropTypes from "prop-types";
import isEqual from "lodash/isEqual.js";
import difference from "lodash/difference.js";
import cloneDeep from "lodash/cloneDeep.js";

import {
  generateKey,
  removeIdFromObjects,
  getOperators,
  useEscapeKey,
  extractFormulaExpression,
} from "../../../../common/helpers.js";
import { ToolbarItem } from "../toolbar-item.jsx";
import { orgStore } from "../../../../store/org-store.js";
import { useAxiosPrivate } from "../../../../service/axios-service.js";
import { HelperText } from "../../../../widgets/helper-text/index.js";
import {
  AggregateDarkIcon,
  AggregateLightIcon,
} from "../../../../base/icons/index.js";
import { useTransformIdStore } from "../../../../store/transform-id-store.js";
import {
  getGroupAndAggregationSpec,
  transformationTypes,
} from "../../no-code-model/helper.js";
import "./group.css";
import { useNotificationService } from "../../../../service/notification-service.js";
import { FormulaEditor } from "../formula-editor.jsx";

// Constants for aggregate functions
const NUMERIC_FUNCTIONS = ["STD", "STDDEV", "SUM", "AVG", "MEAN", "VARIANCE"];
const DYNAMIC_DATATYPE_FUNCTIONS = ["MIN", "MAX"];
const STAR_ALLOWED_FUNCTIONS = ["COUNT"];
const STAR_COLUMN_OPTION = { value: "*", label: "* (All)" };
const HAVING_FUNCTIONS = [
  { value: "SUM", label: "SUM" },
  { value: "AVG", label: "AVG" },
  { value: "MIN", label: "MIN" },
  { value: "MAX", label: "MAX" },
  { value: "COUNT", label: "COUNT" },
  { value: "COUNT_DISTINCT", label: "COUNT DISTINCT" },
];
const NULL_OPERATORS = ["NULL", "NOTNULL", "TRUE", "FALSE"];

const Group = React.memo(
  ({
    allColumns,
    spec,
    updateSpec,
    isLoading,
    disabled,
    step,
    dataType,
    isDarkTheme,
    saveTransformation,
    handleDeleteTransformation,
    handleGetColumns,
  }) => {
    const axiosPrivate = useAxiosPrivate();
    const { selectedOrgId } = orgStore();
    const { transformIds } = useTransformIdStore();
    const { notify } = useNotificationService();

    // State variables
    const [aggregateOptions, setAggregateOptions] = useState([]);
    const [aggregateList, setAggregateList] = useState([
      { function: null, column: null, alias: "" },
    ]);
    const [formulaAggregateList, setFormulaAggregateList] = useState([]);
    const [groupByColumns, setGroupByColumns] = useState([]);
    const [open, setOpen] = useState(false);
    const [validationStates, setValidationStates] = useState({
      agg: {},
      formulaAgg: {},
      having: {},
      filter: {},
      filteronagg: false,
    });
    const [label, setLabel] = useState("Aggregator");
    const [groupedColumnCount, setGroupedColumnCount] = useState(0);
    const [isModified, setIsModified] = useState(false);
    const [havingList, setHavingList] = useState([]);
    const [conditionTypeIndex, setConditionTypeIndex] = useState([]);
    const [columnOptions, setColumnOptions] = useState([]);
    const [havingColumnOptions, setHavingColumnOptions] = useState([]);
    const [filterList, setFilterList] = useState([]);

    // Formula editor modal state
    const [formulaModal, setFormulaModal] = useState({
      open: false,
      index: null,
      formula: "",
    });
    const [formulaValidation, setFormulaValidation] = useState({
      isValid: true,
      errors: [],
    });
    const [clearFormula, setClearFormula] = useState(false);

    const schema = spec?.destination?.schema_name || "default";
    const sourceTable = spec?.source?.table_name;

    /**
     * Fetches available aggregation options from the API
     */
    const getAggregateOptions = useCallback(async () => {
      try {
        const requestOptions = {
          method: "GET",
          url: `/api/v1/visitran/${
            selectedOrgId || "default_org"
          }/aggregations`,
        };
        const res = await axiosPrivate(requestOptions);
        setAggregateOptions(res.data?.aggregations || []);
      } catch (error) {
        console.error(error);
        notify({ error });
      }
    }, [selectedOrgId]);

    /**
     * Updates the label based on the number of grouped columns
     */
    const updateLabel = useCallback((newGroupedColumnCount) => {
      const plural = newGroupedColumnCount > 1 ? "s" : "";
      const newLabel = newGroupedColumnCount
        ? `Aggregated by ${newGroupedColumnCount} column${plural}`
        : "Aggregator";
      setLabel(newLabel);
      setGroupedColumnCount(newGroupedColumnCount);
    }, []);

    /**
     * Validates a list of items based on type (agg, having, or filter)
     */
    const validateList = useCallback(
      (list, type) => {
        const errStates = {};

        if (type === "agg") {
          list.forEach((row, index) => {
            const rowErrors = [];
            if (!row.function) rowErrors.push("function");
            if (!row.column) rowErrors.push("column");
            if (!row.alias?.trim()) rowErrors.push("alias");
            if (rowErrors.length > 0) {
              errStates[index] = rowErrors;
            }
          });
        } else if (type === "formulaAgg") {
          list.forEach((row, index) => {
            const rowErrors = [];
            if (!row.formula?.trim()) rowErrors.push("formula");
            if (!row.alias?.trim()) rowErrors.push("alias");
            // Formula must start with =
            if (row.formula?.trim() && !row.formula.trim().startsWith("=")) {
              rowErrors.push("formula_format");
            }
            if (rowErrors.length > 0) {
              errStates[index] = rowErrors;
            }
          });
        } else if (type === "having") {
          list.forEach((row, index) => {
            const rowErrors = [];
            if (!row.condition?.lhs?.column?.column_name)
              rowErrors.push("column_name");
            if (!row.condition?.lhs?.column?.function)
              rowErrors.push("function");
            if (!row.condition?.operator) rowErrors.push("operator");
            if (!row.condition?.rhs?.value?.length) rowErrors.push("value");
            if (index > 0 && !row.logical_operator)
              rowErrors.push("logical_operator");
            if (rowErrors.length > 0) {
              errStates[index] = rowErrors;
            }
          });
        } else if (type === "filter") {
          list.forEach((row, index) => {
            const rowErrors = [];
            if (!row.condition?.lhs?.column?.column_name)
              rowErrors.push("column_name");
            if (!row.condition?.operator) rowErrors.push("operator");
            if (!row.condition?.rhs?.value?.length) rowErrors.push("value");
            if (index > 0 && !conditionTypeIndex[index - 1]) {
              rowErrors.push("logical_operator");
            }
            if (rowErrors.length > 0) {
              errStates[index] = rowErrors;
            }
          });
        }

        setValidationStates((prev) => ({ ...prev, [type]: errStates }));
        return Object.keys(errStates).length === 0;
      },
      [conditionTypeIndex]
    );

    /**
     * Filters columns based on the selected function
     */
    const filteringCols = useCallback(
      (funct, index, arr) => {
        let options = [];

        if (NUMERIC_FUNCTIONS.includes(funct)) {
          // Filter only numeric columns for numeric functions
          allColumns.forEach((el) => {
            if (dataType?.[el]?.data_type === "Number") {
              options.push({ value: el, label: el });
            }
          });
        } else {
          // All columns for other functions
          options = allColumns.map((el) => ({ value: el, label: el }));
        }

        // Add * option for allowed functions (prepend to list)
        if (STAR_ALLOWED_FUNCTIONS.includes(funct)) {
          options = [STAR_COLUMN_OPTION, ...options];
        }

        if (index !== undefined && arr) {
          arr[index] = options;
        }

        return options;
      },
      [allColumns, dataType]
    );

    /**
     * Returns valid aggregate list after filtering empty and duplicate entries
     */
    const getValidAggregateList = useCallback(
      (aggList = aggregateList) => {
        // Filter out incomplete entries
        const filledAggregateList = aggList.filter(
          (item) => item.function && item.column && item.alias?.trim()
        );

        // Remove duplicates based on alias
        const uniqueAliases = new Set();
        return filledAggregateList.filter((item) => {
          if (uniqueAliases.has(item.alias)) {
            return false;
          }
          uniqueAliases.add(item.alias);
          return true;
        });
      },
      [aggregateList]
    );

    /**
     * Checks if an alias name is duplicated across both aggregate lists
     */
    const isDuplicateColumnName = useCallback(
      (aliasName) => {
        const allAliases = [
          ...aggregateList.map((item) => item.alias),
          ...formulaAggregateList.map((item) => item.alias),
        ];
        const count = allAliases.filter((alias) => alias === aliasName).length;
        return count > 1;
      },
      [aggregateList, formulaAggregateList]
    );

    /**
     * Adds a new formula aggregation entry
     */
    const addFormulaAggregation = useCallback(() => {
      const isValid = validateList(formulaAggregateList, "formulaAgg");
      if (isValid) {
        setFormulaAggregateList((prev) => [
          ...prev,
          { formula: "", alias: "" },
        ]);
      }
    }, [formulaAggregateList, validateList]);

    /**
     * Updates a formula aggregate item
     */
    const updateFormulaAggregateItem = useCallback(
      (value, key, index) => {
        setFormulaAggregateList((prev) => {
          const newList = [...prev];
          if (key === "alias") {
            value = value.replace(/\s+/g, "");
          }
          newList[index] = { ...newList[index], [key]: value };
          return newList;
        });

        // Revalidate if there were errors
        if (Object.keys(validationStates.formulaAgg).length) {
          validateList(formulaAggregateList, "formulaAgg");
        }
      },
      [validationStates.formulaAgg, validateList, formulaAggregateList]
    );

    /**
     * Removes a formula aggregation
     */
    const removeFormulaAggregation = useCallback((index) => {
      setFormulaAggregateList((prev) => prev.filter((_, i) => i !== index));
      setValidationStates((prev) => {
        const newState = { ...prev };
        delete newState.formulaAgg[index];
        return newState;
      });
    }, []);

    /**
     * Returns valid formula aggregate list after filtering empty entries
     * Converts formula field to expression field (strips = prefix) for backend
     */
    const getValidFormulaAggregateList = useCallback(
      (list = formulaAggregateList) => {
        return list
          .filter(
            (item) =>
              item.formula?.trim() &&
              item.formula.trim().startsWith("=") &&
              item.alias?.trim()
          )
          .map((item) => ({
            // Strip = prefix and use expression field (like filter does)
            expression: extractFormulaExpression(item.formula),
            alias: item.alias,
          }));
      },
      [formulaAggregateList]
    );

    /**
     * Opens formula modal for editing a formula aggregate
     */
    const openFormulaModal = useCallback(
      (index) => {
        const formulaItem = formulaAggregateList[index];
        // Get formula without = prefix for the editor
        let initialFormula = formulaItem?.formula || "";
        if (initialFormula.startsWith("=")) {
          initialFormula = initialFormula.substring(1);
        }

        setFormulaModal({
          open: true,
          index,
          formula: initialFormula,
        });
        setFormulaValidation({ isValid: true, errors: [] });
      },
      [formulaAggregateList]
    );

    /**
     * Handles formula value change from FormulaEditor
     */
    const handleFormulaChange = useCallback((value) => {
      setFormulaModal((prev) => ({ ...prev, formula: value }));
    }, []);

    /**
     * Handles validation change from FormulaEditor
     */
    const handleFormulaValidationChange = useCallback((validation) => {
      setFormulaValidation(validation);
    }, []);

    /**
     * Saves formula from modal and closes it
     */
    const handleFormulaSave = useCallback(() => {
      const { index, formula } = formulaModal;
      if (!formula.trim()) {
        setFormulaModal({ open: false, index: null, formula: "" });
        return;
      }

      // Update the formula aggregate with = prefix
      setFormulaAggregateList((prev) => {
        const newList = [...prev];
        newList[index] = { ...newList[index], formula: `=${formula.trim()}` };
        return newList;
      });

      setIsModified(true);
      setFormulaModal({ open: false, index: null, formula: "" });
      setClearFormula(true);
      setTimeout(() => setClearFormula(false), 100);
    }, [formulaModal]);

    /**
     * Cancels formula modal
     */
    const handleFormulaCancel = useCallback(() => {
      setFormulaModal({ open: false, index: null, formula: "" });
      setFormulaValidation({ isValid: true, errors: [] });
      setClearFormula(true);
      setTimeout(() => setClearFormula(false), 100);
    }, []);

    /**
     * Handles saving the transformation
     */
    const handleSave = useCallback(async () => {
      try {
        if (!isModified) return;

        const validAggregateList = getValidAggregateList();
        const validFormulaAggregateList = getValidFormulaAggregateList();

        // Combine both simple and formula aggregates
        const combinedAggregateList = [
          ...validAggregateList,
          ...validFormulaAggregateList,
        ];

        // Add condition types to filter list
        let processedFilterList = filterList;
        if (filterList.length > 1) {
          processedFilterList = filterList.map((el, index) => ({
            ...el,
            logical_operator: conditionTypeIndex[index],
          }));
        }

        const groupsAndAggregationSpec = {
          ...getGroupAndAggregationSpec(
            spec?.transform,
            transformIds?.GROUPS_AND_AGGREGATION
          ),
          aggregate_columns: combinedAggregateList,
          group: groupByColumns,
          having: {
            criteria: removeIdFromObjects(havingList),
          },
          filter: {
            criteria: removeIdFromObjects(processedFilterList),
          },
        };

        let result = {};

        // Check if all fields are empty - delete transformation
        if (
          !combinedAggregateList?.length &&
          !groupByColumns?.length &&
          !groupsAndAggregationSpec?.having?.criteria?.length &&
          !groupsAndAggregationSpec?.filter?.criteria?.length
        ) {
          const body = { step_id: transformIds?.GROUPS_AND_AGGREGATION };
          result = await handleDeleteTransformation(body);
        } else {
          const body = {
            type: "groups_and_aggregation",
            groups_and_aggregation: groupsAndAggregationSpec,
          };
          result = await saveTransformation(
            body,
            transformIds?.GROUPS_AND_AGGREGATION
          );
        }

        if (result?.status === "success") {
          updateSpec(result?.spec);
          updateLabel(groupByColumns.length || combinedAggregateList.length);
          setOpen(false);
        }
      } catch (error) {
        console.error(error);
        notify({ error });
      }
    }, [
      isModified,
      getValidAggregateList,
      getValidFormulaAggregateList,
      filterList,
      conditionTypeIndex,
      spec?.transform,
      transformIds?.GROUPS_AND_AGGREGATION,
      groupByColumns,
      havingList,
      handleDeleteTransformation,
      saveTransformation,
      updateSpec,
      updateLabel,
    ]);

    /**
     * Handles deleting the transformation
     */
    const handleDelete = useCallback(async () => {
      try {
        const body = {
          step_id: transformIds?.GROUPS_AND_AGGREGATION,
        };
        const result = await handleDeleteTransformation(body);

        if (result?.status === "success") {
          updateSpec(result?.spec);
          updateLabel(0);
          setOpen(false);
        }
      } catch (error) {
        console.error(error);
        notify({ error });
      }
    }, [
      transformIds?.GROUPS_AND_AGGREGATION,
      handleDeleteTransformation,
      updateSpec,
      updateLabel,
    ]);

    /**
     * Handles group by column selection
     */
    const addGroupBy = useCallback(
      (value) => {
        if (value.length === 0) {
          setGroupByColumns([]);
          return;
        }

        // Update aggregate list if columns are removed
        if (value.length < aggregateList.length) {
          const removedColumn = difference(groupByColumns, value)[0];
          const filteredAggregateList = aggregateList.filter(
            (item) => item.column !== removedColumn
          );
          setAggregateList(filteredAggregateList);
        }

        setGroupByColumns(value);
      },
      [aggregateList, groupByColumns]
    );

    /**
     * Adds a new aggregation entry
     */
    const addAggregation = useCallback(() => {
      const isValid = validateList(aggregateList, "agg");
      if (isValid) {
        setAggregateList((prev) => [
          ...prev,
          { function: null, column: null, alias: "" },
        ]);
      }
    }, [aggregateList, validateList]);

    /**
     * Updates an aggregate item
     */
    const updateAggregateItem = useCallback(
      (value, key, index) => {
        setAggregateList((prev) => {
          const newList = [...prev];
          if (key === "alias") {
            value = value.replace(/\s+/g, "");
          }
          newList[index] = { ...newList[index], [key]: value };

          if (key === "function") {
            const newColumnOptions = [...columnOptions];
            filteringCols(value, index, newColumnOptions);
            setColumnOptions(newColumnOptions);

            // Clear column if switching away from COUNT when * is selected
            // If new function doesn't support * and current column is *, clear it
            if (
              !STAR_ALLOWED_FUNCTIONS.includes(value) &&
              newList[index].column === "*"
            ) {
              newList[index].column = null;
            }

            // Update filter list if needed
            if (filterList.length) {
              const aggItem = newList[index];
              setFilterList((prevFilter) =>
                prevFilter.map((item) => {
                  if (
                    item.condition?.lhs?.column?.column_name === aggItem.alias
                  ) {
                    let dataType = "Number";
                    if (DYNAMIC_DATATYPE_FUNCTIONS.includes(aggItem.function)) {
                      dataType =
                        dataType?.[aggItem.column]?.data_type || "Number";
                    }
                    return {
                      ...item,
                      condition: {
                        ...item.condition,
                        lhs: {
                          ...item.condition.lhs,
                          column: {
                            ...item.condition.lhs.column,
                            data_type: dataType,
                          },
                        },
                        operator: null,
                        rhs: { ...item.condition.rhs, value: [] },
                      },
                    };
                  }
                  return item;
                })
              );
            }
          }

          return newList;
        });

        // Revalidate if there were errors
        if (Object.keys(validationStates.agg).length) {
          validateList(aggregateList, "agg");
        }
      },
      [
        columnOptions,
        filteringCols,
        filterList,
        dataType,
        validationStates.agg,
        validateList,
        aggregateList,
      ]
    );

    /**
     * Removes an aggregation
     */
    const removeAggregation = useCallback((index) => {
      setAggregateList((prev) => prev.filter((_, i) => i !== index));
      setColumnOptions((prev) => prev.filter((_, i) => i !== index));
      setValidationStates((prev) => {
        const newState = { ...prev };
        delete newState.agg[index];
        return newState;
      });
    }, []);

    /**
     * Adds a new having entry
     */
    const addHavingEntry = useCallback(() => {
      if (groupByColumns.length === 0) return;

      const isValid = validateList(havingList, "having");
      if (isValid) {
        const newCondition = {
          id: generateKey(),
          condition: {
            lhs: {
              column: { column_name: null, data_type: null, function: null },
              type: "COLUMN",
            },
            operator: null,
            rhs: { type: "VALUE", value: [] },
          },
          logical_operator: undefined,
        };
        setHavingList((prev) => [...prev, newCondition]);
      }
    }, [groupByColumns.length, havingList, validateList]);

    /**
     * Removes a having entry
     */
    const removeHavingEntry = useCallback((id) => {
      setHavingList((prev) =>
        prev.filter((el, index) => {
          if (el.id !== id) return true;

          setValidationStates((prevState) => {
            const newState = { ...prevState };
            delete newState.having[index];
            return newState;
          });

          setHavingColumnOptions((prevOptions) =>
            prevOptions.filter((_, i) => i !== index)
          );

          return false;
        })
      );
    }, []);

    /**
     * Handles having condition changes
     */
    const handleHavingChange = useCallback(
      (value, name, id) => {
        setHavingList((prev) => {
          const updated = prev.map((item, index) => {
            if (item.id !== id) return item;

            const newItem = { ...item };

            switch (name) {
              case "logical_operator":
                newItem.logical_operator = value;
                break;

              case "function": {
                newItem.condition.lhs.column.function = value;
                const newHavingOptions = [...havingColumnOptions];
                filteringCols(value, index, newHavingOptions);
                setHavingColumnOptions(newHavingOptions);

                // Clear column if switching away from COUNT when * is selected
                // If new function doesn't support * and current column is *, clear it
                if (
                  !STAR_ALLOWED_FUNCTIONS.includes(value) &&
                  newItem.condition.lhs.column.column_name === "*"
                ) {
                  newItem.condition.lhs.column.column_name = null;
                  newItem.condition.operator = null;
                  newItem.condition.rhs.value = [];
                }
                break;
              }

              case "column_name":
                newItem.condition.lhs.column.column_name = value;
                newItem.condition.lhs.column.data_type =
                  dataType?.[value]?.data_type || "String";
                newItem.condition.operator = null;
                newItem.condition.rhs.value = [];
                break;

              case "operator":
                newItem.condition.operator = value;
                break;

              case "value":
                newItem.condition.rhs.value = [value.replace(/\s+/g, "")];
                break;
            }

            return newItem;
          });

          // Revalidate if needed
          if (Object.keys(validationStates.having).length) {
            validateList(updated, "having");
          }

          return updated;
        });
      },
      [
        havingColumnOptions,
        filteringCols,
        dataType,
        validationStates.having,
        validateList,
      ]
    );

    /**
     * Adds a new filter entry
     */
    const addFilterEntry = useCallback(() => {
      // Validate aggregations first
      const aggErrors = [];
      aggregateList.forEach((row) => {
        if (!row.function) aggErrors.push("function");
        if (!row.column) aggErrors.push("column");
        if (!row.alias) aggErrors.push("alias");
      });

      if (aggErrors.length > 0 || aggregateList.length === 0) {
        setValidationStates((prev) => ({ ...prev, filteronagg: true }));
        return;
      }

      const isValid = validateList(filterList, "filter");
      if (isValid) {
        setValidationStates((prev) => ({ ...prev, filteronagg: false }));

        const newCondition = {
          id: generateKey(),
          condition: {
            lhs: {
              column: {
                column_name: null,
                data_type: null,
                table_name: sourceTable,
                schema_name: schema,
              },
              type: "COLUMN",
            },
            operator: null,
            rhs: { type: "VALUE", value: [] },
          },
          logical_operator: undefined,
        };

        setFilterList((prev) => [...prev, newCondition]);
      }
    }, [aggregateList, filterList, sourceTable, schema, validateList]);

    /**
     * Removes a filter entry
     */
    const removeFilterEntry = useCallback((id) => {
      setFilterList((prev) =>
        prev.filter((el, index) => {
          if (el.id !== id) return true;

          setValidationStates((prevState) => {
            const newState = { ...prevState };
            delete newState.filter[index];
            return newState;
          });

          if (index > 0) {
            setConditionTypeIndex((prevIndex) => {
              const newIndex = [...prevIndex];
              newIndex.splice(index - 1, 1);
              return newIndex;
            });
          }

          return false;
        })
      );
    }, []);

    /**
     * Handles filter changes
     */
    const handleFilterChange = useCallback(
      (value, name, id) => {
        setFilterList((prev) => {
          const updated = prev.map((item, index) => {
            if (item.id !== id) return item;

            const newItem = { ...item };

            switch (name) {
              case "logical_operator":
                if (index > 0) {
                  setConditionTypeIndex((prevIndex) => {
                    const newIndex = [...prevIndex];
                    newIndex[index - 1] = value;
                    return newIndex;
                  });
                }
                break;

              case "column_name": {
                let dType = dataType?.[value]?.data_type;

                if (!dType) {
                  // Check if it's an aggregate alias
                  const aggItem = aggregateList.find(
                    (el) => el.alias === value
                  );
                  if (aggItem) {
                    dType = DYNAMIC_DATATYPE_FUNCTIONS.includes(
                      aggItem.function
                    )
                      ? dataType?.[aggItem.column]?.data_type
                      : "Number";
                  }
                }

                newItem.condition.lhs.column.column_name = value;
                newItem.condition.lhs.column.data_type = dType || "String";
                newItem.condition.operator = null;
                newItem.condition.rhs.value = [];
                break;
              }

              case "operator":
                newItem.condition.operator = value;
                break;

              case "value":
                newItem.condition.rhs.value = [value.replace(/\s+/g, "")];
                break;
            }

            return newItem;
          });

          // Revalidate if needed
          if (Object.keys(validationStates.filter).length) {
            validateList(updated, "filter");
          }

          return updated;
        });
      },
      [aggregateList, dataType, validationStates.filter, validateList]
    );

    /**
     * Clears and restores data from spec
     */
    const clearAndRestoreData = useCallback(() => {
      const newGroupedColumns = cloneDeep(spec?.transform);

      if (newGroupedColumns) {
        const groupsAndAggregationSpec = getGroupAndAggregationSpec(
          newGroupedColumns,
          transformIds?.GROUPS_AND_AGGREGATION
        );

        const cType = [];

        // Separate simple aggregates from formula aggregates
        // Formula aggregates have 'expression' field (no = prefix in backend)
        const allAggregates = groupsAndAggregationSpec.aggregate_columns || [];
        const simpleAggregates = allAggregates.filter(
          (item) => !item.expression
        );
        const formulaAggregates = allAggregates
          .filter((item) => item.expression)
          .map((item) => ({
            // Add = prefix back for display (backend stores without =)
            formula: `=${item.expression}`,
            alias: item.alias,
          }));

        setAggregateList(
          simpleAggregates.length > 0
            ? simpleAggregates
            : [{ function: null, column: null, alias: "" }]
        );
        setFormulaAggregateList(formulaAggregates);
        setGroupByColumns(groupsAndAggregationSpec.group || []);
        updateLabel(
          allAggregates?.length || groupsAndAggregationSpec?.group?.length
        );

        setHavingList(
          groupsAndAggregationSpec.having?.criteria?.map((el) => ({
            ...el,
            id: generateKey(),
          })) || []
        );

        setFilterList(
          groupsAndAggregationSpec.filter?.criteria?.map((el) => {
            if (el.logical_operator) {
              cType.push(el.logical_operator);
            }
            return { ...el, id: generateKey() };
          }) || []
        );

        setConditionTypeIndex(cType);
      } else {
        setGroupByColumns([]);
        setFormulaAggregateList([]);
        updateLabel(0);
      }
    }, [spec?.transform, transformIds?.GROUPS_AND_AGGREGATION, updateLabel]);

    /**
     * Handles open state change
     */
    const handleOpenChange = useCallback(
      (value) => {
        if (!value) {
          clearAndRestoreData();
        } else {
          handleGetColumns(
            transformIds?.GROUPS_AND_AGGREGATION,
            transformationTypes?.GROUPS_AND_AGGREGATION
          );
        }
        setOpen(value);
      },
      [
        clearAndRestoreData,
        handleGetColumns,
        transformIds?.GROUPS_AND_AGGREGATION,
      ]
    );

    /**
     * Handles cancel button
     */
    const handleCancelBtn = useCallback(() => {
      setOpen(false);
      clearAndRestoreData();
    }, [clearAndRestoreData]);

    /**
     * Checks if plus button should be disabled
     */
    const disabledPlus = useCallback((data, type) => {
      if (!data) return false;

      if (type === "agg") {
        return Object.values(data).some((value) => !value);
      } else {
        const { lhs, rhs, operator } = data.condition || {};
        const hasEmptyLhs = Object.values(lhs?.column || {}).some(
          (value) => !value
        );
        const needsValue = !NULL_OPERATORS.includes(operator);
        const hasEmptyRhs = needsValue && !rhs?.value?.[0];

        return hasEmptyLhs || hasEmptyRhs;
      }
    }, []);

    /**
     * Filter function for column name search
     */
    const handleFilterColumnName = useCallback((input, option) => {
      return (option?.label ?? "").toLowerCase().includes(input.toLowerCase());
    }, []);

    /**
     * Checks if configuration is valid
     */
    const checkDisable = useCallback(() => {
      if (!isModified) return false;

      // Consider both simple and formula aggregates
      // Only count aggregates that have actual values filled in
      const hasFilledAggregates = aggregateList.some(
        (el) => el.function && el.column && el.alias?.trim()
      );
      const hasAggregates =
        hasFilledAggregates || formulaAggregateList.length > 0;

      // All empty - valid (will delete)
      if (
        !groupByColumns.length &&
        !hasAggregates &&
        !filterList.length &&
        !havingList.length
      ) {
        return true;
      }

      // Only groupBy or only aggregates - valid
      if (
        (groupByColumns.length || hasAggregates) &&
        !filterList.length &&
        !havingList.length
      ) {
        return true;
      }

      // Filter + Aggregates (no groupBy) - valid
      if (
        !groupByColumns.length &&
        hasAggregates &&
        filterList.length &&
        !havingList.length
      ) {
        return true;
      }

      // GroupBy with aggregates and/or having (no filter) - valid
      if (
        groupByColumns.length &&
        (hasAggregates || havingList.length) &&
        !filterList.length
      ) {
        return true;
      }

      // GroupBy + Aggregates + Filter - valid
      if (
        groupByColumns.length &&
        hasAggregates &&
        filterList.length &&
        !havingList.length
      ) {
        return true;
      }

      // GroupBy + Aggregates + Having - valid
      if (
        groupByColumns.length &&
        hasAggregates &&
        !filterList.length &&
        havingList.length
      ) {
        return true;
      }

      // GroupBy + Aggregates + Having + Filter - valid
      if (
        groupByColumns.length &&
        hasAggregates &&
        filterList.length &&
        havingList.length
      ) {
        return true;
      }

      return false;
    }, [
      isModified,
      groupByColumns,
      aggregateList,
      formulaAggregateList,
      filterList,
      havingList,
    ]);

    /**
     * Memoized save button disabled state
     */
    const isSaveDisabled = useMemo(() => {
      // Check if there's a partially filled aggregate (started but not completed)
      const hasPartialAggregate = aggregateList.some(
        (el) =>
          (el.function || el.column || el.alias?.trim()) &&
          !(el.function && el.column && el.alias?.trim())
      );

      // Check for duplicate aliases across both aggregate lists
      const filledAggAliases = aggregateList
        .filter((el) => el.function && el.column && el.alias?.trim())
        .map((el) => el.alias);
      const allAliases = [
        ...filledAggAliases,
        ...formulaAggregateList.map((el) => el.alias),
      ];
      const isDuplicateAlias = new Set(allAliases).size !== allAliases.length;

      // Check if any formula aggregate has invalid format
      const hasInvalidFormulaFormat = formulaAggregateList.some(
        (el) => el.formula?.trim() && !el.formula.trim().startsWith("=")
      );

      // Check if last formula aggregate is incomplete
      const lastFormulaAgg = formulaAggregateList.at(-1);
      const isFormulaAggIncomplete =
        lastFormulaAgg &&
        (!lastFormulaAgg.formula?.trim() || !lastFormulaAgg.alias?.trim());

      return (
        isLoading ||
        !checkDisable() ||
        disabledPlus(filterList.at(-1)) ||
        disabledPlus(havingList.at(-1)) ||
        hasPartialAggregate ||
        isDuplicateAlias ||
        hasInvalidFormulaFormat ||
        isFormulaAggIncomplete
      );
    }, [
      isLoading,
      checkDisable,
      filterList,
      havingList,
      aggregateList,
      formulaAggregateList,
      disabledPlus,
    ]);

    // Effects
    useEffect(() => {
      if (open) {
        getAggregateOptions();
      }
    }, [open, getAggregateOptions]);

    useEffect(() => {
      clearAndRestoreData();
    }, [
      spec?.transform,
      transformIds?.GROUPS_AND_AGGREGATION,
      clearAndRestoreData,
    ]);

    useEffect(() => {
      const groupsAndAggregationSpec = getGroupAndAggregationSpec(
        spec?.transform,
        transformIds?.GROUPS_AND_AGGREGATION
      );

      // Combine current simple and formula aggregates for comparison
      const currentCombinedAggregates = [
        ...getValidAggregateList(aggregateList),
        ...getValidFormulaAggregateList(formulaAggregateList),
      ];

      // Count formula aggregates in spec to detect new/deleted entries
      // This is needed because getValidFormulaAggregateList filters incomplete entries,
      // but adding a new formula row (even empty) should enable save
      const specFormulaAggregateCount = (
        groupsAndAggregationSpec?.aggregate_columns || []
      ).filter((item) => item.expression).length;
      const hasFormulaCountChange =
        formulaAggregateList.length !== specFormulaAggregateCount;

      setIsModified(
        !isEqual(groupByColumns, groupsAndAggregationSpec?.group) ||
          !isEqual(
            currentCombinedAggregates,
            groupsAndAggregationSpec?.aggregate_columns || []
          ) ||
          hasFormulaCountChange ||
          !isEqual(havingList, groupsAndAggregationSpec?.having?.criteria) ||
          !isEqual(filterList, groupsAndAggregationSpec?.filter?.criteria)
      );
    }, [
      groupByColumns,
      aggregateList,
      formulaAggregateList,
      havingList,
      filterList,
      spec?.transform,
      transformIds?.GROUPS_AND_AGGREGATION,
      getValidAggregateList,
      getValidFormulaAggregateList,
    ]);

    useEffect(() => {
      const allCols = [];
      const havingCols = [];
      const aggregateSpec = getGroupAndAggregationSpec(
        spec?.transform,
        transformIds?.GROUPS_AND_AGGREGATION
      );

      if (aggregateSpec?.aggregate_columns?.length) {
        aggregateSpec.aggregate_columns.forEach((el, index) => {
          const res = filteringCols(el.function);
          allCols[index] = res;
        });
        setColumnOptions(allCols);
      }

      if (aggregateSpec?.having?.criteria?.length) {
        aggregateSpec.having.criteria.forEach((el, index) => {
          const funct = el.condition?.lhs?.column?.function;
          const res = filteringCols(funct);
          havingCols[index] = res;
        });
        setHavingColumnOptions(havingCols);
      }
    }, [spec?.transform, transformIds?.GROUPS_AND_AGGREGATION, filteringCols]);

    useEscapeKey(open, handleCancelBtn);

    return (
      <ToolbarItem
        icon={
          isDarkTheme ? (
            <AggregateDarkIcon className="toolbar-item-icon" />
          ) : (
            <AggregateLightIcon className="toolbar-item-icon" />
          )
        }
        label={label}
        open={open}
        className={
          groupedColumnCount ? "no-code-toolbar-grouped-cols-highlights" : ""
        }
        disabled={disabled}
        handleOpenChange={handleOpenChange}
        step={step}
      >
        <Space direction="vertical" className="aggregator_popover_wrapper">
          <Typography.Title level={5} className="m-0 draggable-title">
            Aggregator
          </Typography.Title>

          {/* Group By Section */}
          <Space direction="vertical" className="width-100">
            <Typography>Group By</Typography>
            <Select
              className="width-100"
              mode="multiple"
              placeholder="Select column(s)"
              value={groupByColumns}
              onChange={addGroupBy}
              options={allColumns.map((item) => ({
                value: item,
                label: item,
              }))}
              maxTagCount="responsive"
              allowClear
            />
          </Space>

          <Card size="small">
            {/* Summarised By Section */}
            <Space direction="vertical" className="width-100">
              <Card
                size="small"
                title={
                  <div className="aggregator-card-header">
                    <Typography.Text>Summarised By</Typography.Text>
                    <Button icon={<PlusOutlined />} onClick={addAggregation}>
                      Add Aggregator
                    </Button>
                  </div>
                }
              >
                <div>
                  {aggregateList?.map((aggregateItem, index) => (
                    <div
                      key={`aggregate-${index}`}
                      className="aggregator-select-items"
                    >
                      <div className="flex-1">
                        <Space direction="vertical" className="width-100">
                          {index === 0 && (
                            <Typography.Text>Function</Typography.Text>
                          )}
                          <Select
                            status={
                              validationStates?.agg?.[index]?.includes(
                                "function"
                              )
                                ? "error"
                                : ""
                            }
                            className="width-100"
                            placeholder="Select function"
                            value={aggregateItem.function}
                            onChange={(value) =>
                              updateAggregateItem(value, "function", index)
                            }
                            options={aggregateOptions}
                          />
                        </Space>
                      </div>

                      <div className="flex-1">
                        <Space direction="vertical" className="width-100">
                          {index === 0 && (
                            <Typography.Text>Column</Typography.Text>
                          )}
                          <Select
                            status={
                              validationStates?.agg?.[index]?.includes("column")
                                ? "error"
                                : ""
                            }
                            className="width-100"
                            placeholder="Select a column"
                            value={aggregateItem.column}
                            showSearch
                            filterOption={handleFilterColumnName}
                            onChange={(value) =>
                              updateAggregateItem(value, "column", index)
                            }
                            options={columnOptions[index] || []}
                          />
                        </Space>
                      </div>

                      <div className="mb-5">
                        <Typography.Text strong>AS</Typography.Text>
                      </div>

                      <div className="flex-1">
                        <Space direction="vertical" className="width-100">
                          {index === 0 && (
                            <Typography.Text>Alias</Typography.Text>
                          )}
                          <Input
                            status={
                              validationStates?.agg?.[index]?.includes(
                                "alias"
                              ) || isDuplicateColumnName(aggregateItem.alias)
                                ? "error"
                                : ""
                            }
                            className="width-100"
                            value={aggregateItem.alias}
                            onChange={(evt) =>
                              updateAggregateItem(
                                evt.target.value,
                                "alias",
                                index
                              )
                            }
                            onKeyDown={(e) => e.stopPropagation()}
                          />
                        </Space>
                      </div>

                      <div style={{ flex: "0 0 auto" }}>
                        <Button
                          icon={<DeleteOutlined />}
                          onClick={() => removeAggregation(index)}
                          danger
                        />
                      </div>
                    </div>
                  ))}
                </div>

                {Object.keys(validationStates.agg).length > 0 && (
                  <HelperText text="Almost there! Please complete the current aggregation before adding a new one" />
                )}
              </Card>

              {/* Formula Aggregates Section */}
              <Card
                size="small"
                title={
                  <div className="aggregator-card-header">
                    <Typography.Text>Formula Aggregates</Typography.Text>
                    <Button
                      icon={<PlusOutlined />}
                      onClick={addFormulaAggregation}
                    >
                      Add Formula
                    </Button>
                  </div>
                }
              >
                <div>
                  {formulaAggregateList?.map((formulaItem, index) => (
                    <div
                      key={`formula-agg-${index}`}
                      className="aggregator-select-items"
                    >
                      <div style={{ flex: 2 }}>
                        <Space direction="vertical" className="width-100">
                          {index === 0 && (
                            <Typography.Text>Formula</Typography.Text>
                          )}
                          <Space.Compact className="width-100">
                            <Input
                              status={
                                validationStates?.formulaAgg?.[index]?.includes(
                                  "formula"
                                ) ||
                                validationStates?.formulaAgg?.[index]?.includes(
                                  "formula_format"
                                )
                                  ? "error"
                                  : ""
                              }
                              style={{ flex: 1 }}
                              placeholder="=SUM(revenue)/COUNT(*)"
                              value={formulaItem.formula}
                              onChange={(evt) =>
                                updateFormulaAggregateItem(
                                  evt.target.value,
                                  "formula",
                                  index
                                )
                              }
                              onKeyDown={(e) => e.stopPropagation()}
                            />
                            <Tooltip title="Open Formula Editor">
                              <Button
                                icon={<FunctionOutlined />}
                                onClick={() => openFormulaModal(index)}
                              />
                            </Tooltip>
                          </Space.Compact>
                        </Space>
                      </div>

                      <div className="mb-5">
                        <Typography.Text strong>AS</Typography.Text>
                      </div>

                      <div className="flex-1">
                        <Space direction="vertical" className="width-100">
                          {index === 0 && (
                            <Typography.Text>Alias</Typography.Text>
                          )}
                          <Input
                            status={
                              validationStates?.formulaAgg?.[index]?.includes(
                                "alias"
                              ) || isDuplicateColumnName(formulaItem.alias)
                                ? "error"
                                : ""
                            }
                            className="width-100"
                            placeholder="result_alias"
                            value={formulaItem.alias}
                            onChange={(evt) =>
                              updateFormulaAggregateItem(
                                evt.target.value,
                                "alias",
                                index
                              )
                            }
                            onKeyDown={(e) => e.stopPropagation()}
                          />
                        </Space>
                      </div>

                      <div style={{ flex: "0 0 auto" }}>
                        <Button
                          icon={<DeleteOutlined />}
                          onClick={() => removeFormulaAggregation(index)}
                          danger
                        />
                      </div>
                    </div>
                  ))}
                </div>

                {formulaAggregateList.length === 0 && (
                  <Typography.Text type="secondary">
                    Use formulas like =SUM(revenue)/COUNT(*) or
                    =ROUND(AVG(price),2)
                  </Typography.Text>
                )}

                {Object.keys(validationStates.formulaAgg).length > 0 && (
                  <HelperText text="Formula must start with = and alias is required" />
                )}
              </Card>

              {/* Action Buttons */}
              <Space className="aggregator-action-buttons-container">
                <Button icon={<PlusOutlined />} onClick={addHavingEntry}>
                  Add having
                </Button>
                <Button icon={<PlusOutlined />} onClick={addFilterEntry}>
                  Add Filter
                </Button>
              </Space>

              {/* Having Section */}
              {havingList.length > 0 && (
                <Card size="small" title="Having">
                  {havingList?.map((el, index) => {
                    const { lhs, rhs, operator } = el.condition || {};
                    return (
                      <div className="aggregator-select-items" key={el.id}>
                        {index > 0 && (
                          <div className="flex-1">
                            <Select
                              status={
                                validationStates?.having?.[index]?.includes(
                                  "logical_operator"
                                )
                                  ? "error"
                                  : ""
                              }
                              className="width-100"
                              placeholder="Select"
                              value={el.logical_operator}
                              onChange={(value) =>
                                handleHavingChange(
                                  value,
                                  "logical_operator",
                                  el.id
                                )
                              }
                              options={[
                                { value: "AND", label: "AND" },
                                { value: "OR", label: "OR" },
                              ]}
                            />
                          </div>
                        )}

                        <div className="flex-1">
                          <Space direction="vertical" className="width-100">
                            {index === 0 && (
                              <Typography.Text>Function</Typography.Text>
                            )}
                            <Select
                              status={
                                validationStates?.having?.[index]?.includes(
                                  "function"
                                )
                                  ? "error"
                                  : ""
                              }
                              className="width-100"
                              placeholder="Select"
                              value={lhs?.column?.function}
                              onChange={(value) =>
                                handleHavingChange(value, "function", el.id)
                              }
                              options={HAVING_FUNCTIONS}
                            />
                          </Space>
                        </div>

                        <div className="flex-1">
                          <Space direction="vertical" className="width-100">
                            {index === 0 && (
                              <Typography.Text>Column</Typography.Text>
                            )}
                            <Select
                              status={
                                validationStates?.having?.[index]?.includes(
                                  "column_name"
                                )
                                  ? "error"
                                  : ""
                              }
                              className="width-100"
                              placeholder="Select"
                              value={lhs?.column?.column_name}
                              onChange={(value) =>
                                handleHavingChange(value, "column_name", el.id)
                              }
                              options={havingColumnOptions[index] || []}
                              filterOption={handleFilterColumnName}
                              showSearch
                            />
                          </Space>
                        </div>

                        <div className="flex-1">
                          <Space direction="vertical" className="width-100">
                            {index === 0 && (
                              <Typography.Text>Conditions</Typography.Text>
                            )}
                            <Select
                              status={
                                validationStates?.having?.[index]?.includes(
                                  "operator"
                                )
                                  ? "error"
                                  : ""
                              }
                              className="width-100"
                              placeholder="Select"
                              value={operator}
                              onChange={(value) =>
                                handleHavingChange(value, "operator", el.id)
                              }
                              options={getOperators(
                                null,
                                lhs?.column?.data_type
                              )}
                            />
                          </Space>
                        </div>

                        <div className="flex-1">
                          <Space direction="vertical" className="width-100">
                            {index === 0 && (
                              <Typography.Text>Value</Typography.Text>
                            )}
                            <Input
                              status={
                                validationStates?.having?.[index]?.includes(
                                  "value"
                                )
                                  ? "error"
                                  : ""
                              }
                              value={rhs?.value?.[0] || ""}
                              className="width-100"
                              onChange={(evt) =>
                                handleHavingChange(
                                  evt.target.value,
                                  "value",
                                  el.id
                                )
                              }
                              disabled={NULL_OPERATORS.includes(operator)}
                              onKeyDown={(e) => e.stopPropagation()}
                              type={
                                lhs?.column?.data_type?.toLowerCase() ===
                                "number"
                                  ? "number"
                                  : "text"
                              }
                            />
                          </Space>
                        </div>

                        <Button
                          icon={<DeleteOutlined />}
                          onClick={() => removeHavingEntry(el.id)}
                          danger
                        />
                      </div>
                    );
                  })}

                  {Object.keys(validationStates.having).length > 0 && (
                    <HelperText text="Oops! Looks like the current HAVING condition isn't complete." />
                  )}
                </Card>
              )}

              {/* Filter Section */}
              {filterList.length > 0 && (
                <Card size="small" title="Filter">
                  {filterList?.map((el, index) => {
                    const { lhs, rhs, operator } = el.condition || {};
                    return (
                      <div className="aggregator-select-items" key={el.id}>
                        {index > 0 ? (
                          <div className="flex-1">
                            <Space direction="vertical" className="width-100">
                              <Select
                                status={
                                  validationStates?.filter?.[index]?.includes(
                                    "logical_operator"
                                  )
                                    ? "error"
                                    : ""
                                }
                                className="width-100"
                                placeholder="Select"
                                value={conditionTypeIndex[index - 1]}
                                onChange={(value) =>
                                  handleFilterChange(
                                    value,
                                    "logical_operator",
                                    el.id
                                  )
                                }
                                options={[
                                  { value: "AND", label: "AND" },
                                  { value: "OR", label: "OR" },
                                ]}
                              />
                            </Space>
                          </div>
                        ) : (
                          <Space direction="vertical">
                            <Typography>Where</Typography>
                          </Space>
                        )}

                        <div className="flex-1">
                          <Space direction="vertical" className="width-100">
                            <Select
                              status={
                                validationStates?.filter?.[index]?.includes(
                                  "column_name"
                                )
                                  ? "error"
                                  : ""
                              }
                              className="width-100"
                              placeholder="Select a column"
                              value={lhs?.column?.column_name}
                              onChange={(value) =>
                                handleFilterChange(value, "column_name", el.id)
                              }
                              options={[
                                ...aggregateList
                                  .filter((el) => el.alias)
                                  .map((el) => ({
                                    value: el.alias,
                                    label: el.alias,
                                  })),
                                ...groupByColumns.map((el) => ({
                                  value: el,
                                  label: el,
                                })),
                              ]}
                              filterOption={handleFilterColumnName}
                              showSearch
                            />
                          </Space>
                        </div>

                        <div className="flex-1">
                          <Space direction="vertical" className="width-100">
                            <Select
                              status={
                                validationStates?.filter?.[index]?.includes(
                                  "operator"
                                )
                                  ? "error"
                                  : ""
                              }
                              className="width-100"
                              placeholder="Select"
                              value={operator}
                              onChange={(value) =>
                                handleFilterChange(value, "operator", el.id)
                              }
                              options={getOperators(
                                null,
                                lhs?.column?.data_type
                              )}
                            />
                          </Space>
                        </div>

                        <div className="flex-1">
                          <Space direction="vertical" className="width-100">
                            <Input
                              status={
                                validationStates?.filter?.[index]?.includes(
                                  "value"
                                )
                                  ? "error"
                                  : ""
                              }
                              value={rhs?.value?.[0] || ""}
                              className="width-100"
                              onChange={(evt) =>
                                handleFilterChange(
                                  evt.target.value,
                                  "value",
                                  el.id
                                )
                              }
                              disabled={NULL_OPERATORS.includes(operator)}
                              onKeyDown={(e) => e.stopPropagation()}
                              type={
                                lhs?.column?.data_type?.toLowerCase() ===
                                "number"
                                  ? "number"
                                  : "text"
                              }
                            />
                          </Space>
                        </div>

                        <Button
                          icon={<DeleteOutlined />}
                          onClick={() => removeFilterEntry(el.id)}
                          danger
                        />
                      </div>
                    );
                  })}

                  {Object.keys(validationStates.filter).length > 0 && (
                    <HelperText text="Hold on! You missed some fields in the current filter." />
                  )}
                </Card>
              )}
              {validationStates.filteronagg && (
                <HelperText text="Almost there! Wrap up the aggregation before moving to filter." />
              )}
            </Space>
          </Card>

          {/* Footer Buttons */}
          <div className="aggregator-footer-buttons">
            <Space>
              <Button onClick={handleCancelBtn} disabled={isLoading}>
                Cancel
              </Button>
              <Popconfirm
                title="Delete Aggregation"
                description="Are you sure you want to delete this aggregation? This action cannot be undone."
                onConfirm={handleDelete}
                okText="Delete"
                cancelText="Cancel"
                okButtonProps={{ danger: true }}
              >
                <Button danger disabled={isLoading}>
                  Delete
                </Button>
              </Popconfirm>
              <Button
                onClick={handleSave}
                disabled={isSaveDisabled}
                type="primary"
                loading={isLoading}
              >
                Save
              </Button>
            </Space>
          </div>

          {/* Formula Editor Modal */}
          <Modal
            title={
              <Space>
                <FunctionOutlined />
                <span>Formula Editor</span>
              </Space>
            }
            open={formulaModal.open}
            onCancel={handleFormulaCancel}
            width={600}
            footer={
              <Space>
                <Button onClick={handleFormulaCancel}>Cancel</Button>
                <Tooltip
                  title={
                    !formulaValidation.isValid
                      ? "Fix formula errors before saving"
                      : ""
                  }
                >
                  <Button
                    type="primary"
                    onClick={handleFormulaSave}
                    disabled={!formulaModal.formula.trim()}
                  >
                    Apply Formula
                  </Button>
                </Tooltip>
              </Space>
            }
            destroyOnClose
          >
            <div style={{ marginBottom: 16 }}>
              <Typography.Text type="secondary">
                Enter an aggregate formula expression using columns and
                aggregate functions.
              </Typography.Text>
            </div>

            <FormulaEditor
              allColumns={allColumns}
              value={formulaModal.formula}
              setValue={handleFormulaChange}
              clear={clearFormula}
              populate={null}
              onValidationChange={handleFormulaValidationChange}
            />

            {/* Formula validation errors */}
            {!formulaValidation.isValid &&
              formulaValidation.errors.length > 0 && (
                <Alert
                  type="error"
                  showIcon
                  icon={<WarningOutlined />}
                  message={
                    <span style={{ fontSize: "12px" }}>
                      {formulaValidation.errors[0]?.message}
                    </span>
                  }
                  style={{ marginTop: 12 }}
                />
              )}

            <div style={{ marginTop: 12 }}>
              <Typography.Text type="secondary" style={{ fontSize: "12px" }}>
                Examples: SUM(revenue)/COUNT(*), ROUND(AVG(price), 2),
                MAX(sales) - MIN(sales)
              </Typography.Text>
            </div>
          </Modal>
        </Space>
      </ToolbarItem>
    );
  }
);

Group.displayName = "Group";

Group.propTypes = {
  allColumns: PropTypes.arrayOf(PropTypes.string).isRequired,
  spec: PropTypes.object.isRequired,
  updateSpec: PropTypes.func.isRequired,
  isLoading: PropTypes.bool.isRequired,
  disabled: PropTypes.bool.isRequired,
  step: PropTypes.array,
  dataType: PropTypes.object,
  isDarkTheme: PropTypes.bool.isRequired,
  saveTransformation: PropTypes.func.isRequired,
  handleDeleteTransformation: PropTypes.func.isRequired,
  handleGetColumns: PropTypes.func.isRequired,
};

export { Group };
