import { useState, useEffect, useCallback } from "react";
import {
  Typography,
  Button,
  Space,
  Select,
  AutoComplete,
  Modal,
  Tooltip,
  Alert,
} from "antd";
import {
  PlusOutlined,
  DeleteOutlined,
  WarningOutlined,
  FunctionOutlined,
} from "@ant-design/icons";
import PropTypes from "prop-types";
import { isEqual } from "lodash";
import { current, produce } from "immer";

import {
  addIdToObjects,
  generateKey,
  getFilterCondition,
  removeIdFromObjects,
  stopPropagation,
  getOperators,
  renameMap,
  useEscapeKey,
  isFormulaExpression,
  extractFormulaExpression,
  validateFormulaExpression,
} from "../../../../common/helpers.js";
import { useProjectStore } from "../../../../store/project-store.js";
import { useTransformIdStore } from "../../../../store/transform-id-store.js";
import { getFilterSpec } from "../../no-code-model/helper.js";
import { FormulaEditor } from "../../no-code-toolbar/formula-editor.jsx";
// CSS for this component is added in the parent component's CSS file (no-code-model)
function Filter({
  columnDetails,
  filterConditions,
  setFilterConditions,
  type,
  // The below prop is required if type === "join"
  join,
  setJoins,
  joinIndex,
  joinColumnDetails,
  joinSourceColumns,
  getCriteria,
  // Below props are required if type === "model"
  isLoading,
  setOpen = () => {},
  updateSpec = () => {},
  // Below props are required if type === "source" or "model"
  spec,
  setConditionType,
  conditionType,
  saveTransformation,
  handleDeleteTransformation,
  joinType,
}) {
  const { dbConfigDetails } = useProjectStore();
  const isSchemaExists = dbConfigDetails?.is_schema_exists ?? false;
  const [isModified, setIsModified] = useState(false);
  const [showHint, setShowHint] = useState(false);
  const { transformIds } = useTransformIdStore();

  // Formula editor modal state
  const [formulaModal, setFormulaModal] = useState({
    open: false,
    index: null,
    side: null, // 'lhs' or 'rhs'
    formula: "",
  });
  const [formulaValidation, setFormulaValidation] = useState({
    isValid: true,
    errors: [],
  });
  const [clearFormula, setClearFormula] = useState(false);

  const isJoinType = type === "join";

  // Returns filter based on type
  const getFilter = () => {
    if (isJoinType) {
      return join[joinIndex].criteria;
    }

    const filterSpec = getFilterSpec(spec?.transform, transformIds?.FILTER);
    return filterSpec?.criteria || [];
  };

  // Get schema based on type
  const getSchema = () => {
    return spec[type].schema_name;
  };
  // Get table based on type
  const getTable = () => {
    return spec[type].table_name;
  };

  const getFilterColsDetails = (columnName) => {
    // Create rename mapping for lookup
    const renameChecks = renameMap(spec, transformIds?.RENAME_COLUMN);

    if (columnDetails?.columns?.filter) {
      return columnDetails.columns.filter
        .filter((column) => column !== columnName)
        .map((column) => {
          const newName = renameChecks[column];
          const dataType =
            columnDetails?.allColumnsDetails?.[column]?.["data_type"];

          return {
            label: newName ? `${newName} | ${column}` : column,
            value: column,
            dataType: dataType,
          };
        });
    }
  };

  // Helper to check if two data types are compatible for join
  const areTypesCompatible = (type1, type2) => {
    if (!type1 || !type2) return true; // If unknown, allow it
    const t1 = (type1 || "").toLowerCase();
    const t2 = (type2 || "").toLowerCase();
    if (t1 === t2) return true;

    // Define compatible type groups
    const numericTypes = [
      "int",
      "integer",
      "bigint",
      "smallint",
      "number",
      "numeric",
      "decimal",
      "float",
      "double",
      "real",
    ];
    const stringTypes = ["string", "varchar", "text", "char", "character"];
    const dateTypes = ["date", "datetime", "timestamp", "time"];

    const isNumeric = (t) => numericTypes.some((nt) => t.includes(nt));
    const isString = (t) => stringTypes.some((st) => t.includes(st));
    const isDate = (t) => dateTypes.some((dt) => t.includes(dt));

    // Same category = compatible
    if (isNumeric(t1) && isNumeric(t2)) return true;
    if (isString(t1) && isString(t2)) return true;
    if (isDate(t1) && isDate(t2)) return true;

    return false;
  };

  const getColumnOptions = (
    columnName = null,
    conditionSide = null,
    lhsDataType = null
  ) => {
    if (isJoinType) {
      if (conditionSide === "lhs") {
        const sourceCols = joinSourceColumns?.[joinIndex]?.columns || [];
        const sourceTypes = joinSourceColumns?.[joinIndex]?.dataTypes || [];

        return sourceCols.map((column, index) => ({
          label: column,
          value: column,
          dataType: sourceTypes[index] ?? null,
        }));
      } else {
        const joinCols = joinColumnDetails?.[joinIndex]?.columns || [];
        const joinTypes = joinColumnDetails?.[joinIndex]?.dataTypes || [];

        return joinCols.map((column, index) => {
          const rhsType = joinTypes[index] ?? null;
          const isCompatible = areTypesCompatible(lhsDataType, rhsType);

          return {
            label: isCompatible ? (
              column
            ) : (
              <span>
                {column}{" "}
                <WarningOutlined
                  style={{ color: "#faad14" }}
                  title="Type mismatch"
                />
              </span>
            ),
            value: column,
            dataType: rhsType,
            isCompatible,
          };
        });
      }
    } else {
      return getFilterColsDetails(columnName);
    }
  };

  const handleAdd = (event) => {
    event.stopPropagation();
    if (filterConditions.length >= 5) {
      return;
    }
    if (!isJoinType && filterConditions.length !== 0) {
      setConditionType((newConditionType) => {
        newConditionType.push("AND");
      });
    }
    setFilterConditions((newFilterConditions) => {
      let newFilterCondition;
      if (isJoinType) {
        const source = join?.[joinIndex]?.criteria?.[0]?.condition?.lhs?.column;
        newFilterCondition = getCriteria(
          joinColumnDetails[joinIndex]?.columns[0],
          joinColumnDetails[joinIndex]?.dataTypes[0],
          source
        );
        setJoins((newJoins) => {
          newJoins[joinIndex].criteria.push(newFilterCondition);
        });
        newFilterConditions[joinIndex].push({
          id: generateKey(),
          ...newFilterCondition,
        });
      } else {
        newFilterCondition = {
          condition: {
            lhs: getFilterCondition(
              [getSchema(), getTable(), ""],
              "COLUMN",
              null
            ),
            operator: "",
            rhs: getFilterCondition([""], "VALUE"),
          },
        };
        newFilterConditions.push({ id: generateKey(), ...newFilterCondition });
      }
    });
  };
  const handleRemove = (index, event) => {
    event.stopPropagation();
    if (isJoinType && filterConditions.length === 1) {
      return;
    }
    setFilterConditions((newFilterConditions) => {
      if (isJoinType) {
        newFilterConditions[joinIndex].splice(index, 1);
        const newjoin = produce(join, (draft) => {
          draft[joinIndex].criteria = removeIdFromObjects(
            current(newFilterConditions[joinIndex])
          );
        });
        setJoins(newjoin);
      } else {
        newFilterConditions.splice(index, 1);
      }
    });
    if (!isJoinType) {
      setConditionType((newConditionType) => {
        if (index !== 0) {
          newConditionType.splice(index - 1, 1);
        } else if (newConditionType.length !== 0) {
          newConditionType.splice(newConditionType.length - 1, 1);
        }
      });
    }
  };

  const settingFilterConditions = (
    isCondition,
    newFilterCondition,
    key,
    conditionKey,
    dataType,
    isJoinType,
    value,
    typed
  ) => {
    if (isCondition) {
      if (conditionKey === "operator") {
        const prevOp = newFilterCondition[key][conditionKey];
        newFilterCondition[key][conditionKey] = value;
        // Reset RHS when switching to/from BETWEEN
        if (value === "BETWEEN" && prevOp !== "BETWEEN") {
          newFilterCondition[key].rhs = { type: "VALUE", value: ["", ""] };
        } else if (prevOp === "BETWEEN" && value !== "BETWEEN") {
          newFilterCondition[key].rhs = { type: "VALUE", value: [""] };
        }
      } else if (conditionKey === "lhs") {
        if (isJoinType) {
          // Join type: always use COLUMN
          if (key === "column_name") {
            const operators = getOperators(dataType);
            newFilterCondition.condition.operator = operators[0].value;
            newFilterCondition.condition.rhs.value = [""];
          }
          newFilterCondition["condition"][conditionKey] = getFilterCondition(
            value?.split("."),
            "COLUMN",
            dataType
          );
        } else if (isFormulaExpression(value)) {
          // Non-join type with FORMULA
          const formulaExpr = extractFormulaExpression(value);
          newFilterCondition["condition"][conditionKey] = {
            type: "FORMULA",
            expression: formulaExpr,
            _displayValue: value,
          };
        } else {
          // Non-join type with COLUMN
          if (key === "column_name") {
            const operators = getOperators(dataType);
            newFilterCondition.condition.operator = operators[0].value;
            newFilterCondition.condition.rhs.value = [""];
          }
          // Ensure lhs has proper structure for COLUMN type
          if (!newFilterCondition["condition"][conditionKey]["column"]) {
            newFilterCondition["condition"][conditionKey] = {
              type: "COLUMN",
              column: {},
            };
          }
          newFilterCondition["condition"][conditionKey]["type"] = "COLUMN";
          newFilterCondition["condition"][conditionKey]["column"][key] = value;
          newFilterCondition["condition"][conditionKey]["column"]["data_type"] =
            dataType;
        }
      } else if (isJoinType) {
        newFilterCondition["condition"][conditionKey] = getFilterCondition(
          value?.split("."),
          "COLUMN",
          dataType
        );
      } else {
        // Check if this is a formula expression (starts with =)
        if (isFormulaExpression(value)) {
          const formulaExpr = extractFormulaExpression(value);
          newFilterCondition["condition"][conditionKey] = {
            type: "FORMULA",
            expression: formulaExpr,
          };
          // Store the display value with = prefix for UI
          newFilterCondition["condition"][conditionKey]["_displayValue"] =
            value;
        } else {
          const dataType =
            newFilterCondition[
              "condition"
            ].lhs.column?.data_type?.toLowerCase() || "";
          const operator = newFilterCondition["condition"]?.operator;
          const inAndNotIn = ["NOTIN", "IN"].includes(operator);
          // Remove leading spaces first
          value = value.trimStart();
          const setAndSanitizeValue = (regex, replaceRegex) => {
            setShowHint(regex.test(value));
            value = value.replace(replaceRegex, "");
          };

          if (dataType === "number") {
            inAndNotIn
              ? setAndSanitizeValue(/[^\d,]/, /[^\d,]/g)
              : setAndSanitizeValue(/[^\d]/, /\D/g);
          }
          newFilterCondition["condition"][conditionKey][key] = [value];
          newFilterCondition["condition"][conditionKey]["type"] = typed;
        }
      }
    } else {
      newFilterCondition[key] = value;
    }
  };

  const handleOptionChange = (
    index,
    value,
    key,
    isCondition = false,
    conditionKey = null,
    dataType = null,
    typed = "COLUMN"
  ) => {
    if (key === "logical_operator" && index > 0) {
      setConditionType((newConditionType) => {
        newConditionType[index - 1] = value;
      });
    } else {
      setFilterConditions((newFilterConditions) => {
        const newFilterCondition = isJoinType
          ? newFilterConditions[joinIndex][index]
          : newFilterConditions[index];
        settingFilterConditions(
          isCondition,
          newFilterCondition,
          key,
          conditionKey,
          dataType,
          isJoinType,
          value,
          typed
        );
        if (isJoinType) {
          const newjoin = produce(join, (draft) => {
            draft[joinIndex].criteria = removeIdFromObjects(
              current(newFilterConditions[joinIndex])
            );
          });
          setJoins(newjoin);
        }
      });
    }
  };

  const handleBetweenChange = (index, value, valueIndex) => {
    setFilterConditions((newFilterConditions) => {
      const newFilterCondition = isJoinType
        ? newFilterConditions[joinIndex][index]
        : newFilterConditions[index];
      const dataType =
        newFilterCondition.condition.lhs.column?.data_type?.toLowerCase() || "";
      let sanitized = value.trimStart();
      if (dataType === "number") {
        setShowHint(/[^\d.-]/.test(sanitized));
        sanitized = sanitized.replace(/[^\d.-]/g, "");
      }
      if (!Array.isArray(newFilterCondition.condition.rhs.value)) {
        newFilterCondition.condition.rhs.value = ["", ""];
      }
      newFilterCondition.condition.rhs.value[valueIndex] = sanitized;
    });
  };

  const handleSave = async () => {
    if (!isModified) {
      return;
    }

    const filter = getNewFilterConditions();

    let result = {};
    if (filter?.length === 0) {
      const body = {
        step_id: transformIds?.FILTER,
      };

      result = await handleDeleteTransformation(body);
    } else {
      const filterSpec = {
        ...getFilterSpec(spec?.transform, transformIds?.FILTER),
      };

      if (type === "model") {
        filterSpec["criteria"] = filter;
      }
      const body = {
        type: "filter",
        filter: filterSpec,
      };
      result = await saveTransformation(body, transformIds?.FILTER);
    }

    if (result?.status === "success") {
      setOpen(false);
      updateSpec(result?.spec);
    } else {
      setOpen(true);
    }
  };

  const setRhsValue = (values) => {
    return values.map((value) => {
      if (value === "" || value === undefined) return null;
      return isNaN(value) ? value : Number(value);
    });
  };

  // This function returns filterConditions without id and convert type of params.
  const getNewFilterConditions = () => {
    return produce(filterConditions, (draft) => {
      draft.forEach((item, index) => {
        delete item.id;

        // Handle LHS - remove _displayValue for FORMULA type
        if (item.condition.lhs.type === "FORMULA") {
          delete item.condition.lhs._displayValue;
        }

        // Handle RHS - remove _displayValue for FORMULA type, otherwise process VALUE
        if (item.condition.rhs.type === "FORMULA") {
          delete item.condition.rhs._displayValue;
        } else if (item.condition.rhs.value) {
          item.condition.rhs.value = setRhsValue(item.condition.rhs.value);
        }

        item.logical_operator = conditionType[index];
        if (!item.logical_operator) {
          delete item.logical_operator;
        }
      });
    });
  };

  // Loading initial data
  useEffect(() => {
    const filter = addIdToObjects(getFilter() || []);
    if (!isJoinType) {
      setConditionType([]);
      filter.forEach((filterCondition) => {
        const { logical_operator: conditionType } = filterCondition;
        if (conditionType) {
          setConditionType((newConditionType) => {
            newConditionType.push(conditionType);
          });
        }
      });
      setFilterConditions(filter);
    } else {
      setFilterConditions((newFilterConditions) => {
        newFilterConditions[joinIndex] = filter;
      });
    }
  }, [!isJoinType && getFilter()]);

  // Update isModified on model filterConditions change
  useEffect(() => {
    if (type === "model") {
      setIsModified(!isEqual(getNewFilterConditions(), getFilter()));
    }
  }, [filterConditions]);

  // Update filter_condition change in filterConditions
  useEffect(() => {
    if (!isJoinType) {
      setFilterConditions((newFilterConditions) => {
        newFilterConditions.forEach((filterCondition, index) => {
          filterCondition.logical_operator = conditionType[index];
        });
      });
    }
  }, [conditionType]);

  const handleCancelBtn = () => {
    setOpen(false);
    const filterSpec = getFilterSpec(spec?.transform, transformIds?.FILTER);
    const specData = filterSpec?.criteria;
    setFilterConditions(specData);
    setConditionType(
      specData.map((el) => {
        return el?.condition?.logical_operator && el.condition.logical_operator;
      })
    );
  };

  useEscapeKey(setOpen, handleCancelBtn);

  const validateSaveBtn = () => {
    const disabledOperators = ["TRUE", "FALSE", "NULL", "NOTNULL"];

    for (const item of filterConditions) {
      const operator = item.condition.operator;
      const isRhsFormula = item.condition.rhs.type === "FORMULA";
      const isLhsFormula = item.condition.lhs.type === "FORMULA";

      // For FORMULA type, check if expression is valid
      if (isRhsFormula) {
        const expr = item.condition.rhs.expression;
        const validation = validateFormulaExpression(expr);
        if (!validation.valid) return true;
      }

      if (isLhsFormula) {
        const expr = item.condition.lhs.expression;
        const validation = validateFormulaExpression(expr);
        if (!validation.valid) return true;
      }

      const rhsValue = isRhsFormula
        ? item.condition.rhs.expression
        : item.condition.rhs.value?.[0];
      const lhsType = isLhsFormula
        ? null
        : item.condition.lhs.column?.data_type?.toLowerCase();

      // Skip validation for special operators
      if (disabledOperators.includes(operator)) continue;

      // BETWEEN: both low and high values must be non-empty
      if (operator === "BETWEEN") {
        const low = item.condition.rhs.value?.[0];
        const high = item.condition.rhs.value?.[1];
        if (
          low === undefined ||
          low === "" ||
          low === null ||
          high === undefined ||
          high === "" ||
          high === null
        ) {
          return true;
        }
        continue;
      }

      // Allow empty string only for EQ / NOTEQ with string type
      const isStringEmptyAllowed =
        ["EQ", "NOTEQ"].includes(operator) && lhsType === "string";

      if (!operator) return true;

      // If RHS empty but not in allowed scenario → disable
      if (
        (rhsValue === undefined || rhsValue === "" || rhsValue === null) &&
        !isStringEmptyAllowed
      ) {
        return true;
      }
    }

    return false;
  };

  // Get all column names for formula editor autocomplete
  const getAllColumns = () => {
    return columnDetails?.columns?.filter || [];
  };

  // Open formula modal for a specific filter condition
  const openFormulaModal = (index, side) => {
    const condition = filterConditions[index]?.condition;
    const sideData = condition?.[side];
    let initialFormula = "";

    if (sideData?.type === "FORMULA") {
      initialFormula = sideData.expression || "";
    } else if (side === "lhs" && sideData?.column?.column_name) {
      // Optionally pre-populate with column name
      initialFormula = sideData.column.column_name;
    } else if (side === "rhs" && sideData?.value?.[0]) {
      initialFormula = sideData.value[0];
    }

    setFormulaModal({
      open: true,
      index,
      side,
      formula: initialFormula,
    });
    setFormulaValidation({ isValid: true, errors: [] });
  };

  // Handle formula value change
  const handleFormulaChange = (value) => {
    setFormulaModal((prev) => ({ ...prev, formula: value }));
  };

  // Handle validation change from FormulaEditor
  const handleValidationChange = useCallback((validation) => {
    setFormulaValidation(validation);
  }, []);

  // Save formula and close modal
  const handleFormulaSave = () => {
    const { index, side, formula } = formulaModal;
    if (!formula.trim()) {
      setFormulaModal({ open: false, index: null, side: null, formula: "" });
      return;
    }

    // Update the filter condition with the formula
    setFilterConditions((newFilterConditions) => {
      const condition = newFilterConditions[index].condition;
      if (side === "lhs") {
        condition.lhs = {
          type: "FORMULA",
          expression: formula.trim(),
          _displayValue: `=${formula.trim()}`,
        };
        // Reset operator when LHS changes
        condition.operator = "";
      } else if (side === "rhs") {
        condition.rhs = {
          type: "FORMULA",
          expression: formula.trim(),
          _displayValue: `=${formula.trim()}`,
        };
      }
    });

    setIsModified(true);
    setFormulaModal({ open: false, index: null, side: null, formula: "" });
    setClearFormula(true);
    setTimeout(() => setClearFormula(false), 100);
  };

  // Cancel formula modal
  const handleFormulaCancel = () => {
    setFormulaModal({ open: false, index: null, side: null, formula: "" });
    setFormulaValidation({ isValid: true, errors: [] });
    setClearFormula(true);
    setTimeout(() => setClearFormula(false), 100);
  };

  return (
    <Space
      direction="vertical"
      className={isJoinType ? "" : "ml-10 mr-10"}
      size={10}
    >
      {type !== "join" && (
        <Typography.Title
          level={5}
          className="m-0 filter-title draggable-title"
        >
          Filter By
        </Typography.Title>
      )}
      {filterConditions.map((filterCondition, index) => {
        const {
          id,
          condition: { lhs, rhs, operator },
        } = filterCondition;
        let lhsValue = null;
        let rhsValue = null;
        const lhsColumnName = lhs.column?.column_name;
        const isLhsFormula = lhs.type === "FORMULA";
        const isRhsFormula = rhs.type === "FORMULA";

        if (isJoinType) {
          // For LHS, fall back to model source table when null (visitran-ai sets null for computed columns)
          const lhsTableName =
            lhs.column.table_name || spec?.source?.table_name || "";
          const lhsSchemaName =
            lhs.column.schema_name || spec?.source?.schema_name || "";
          lhsValue = `${lhsTableName}.${lhsColumnName}`;
          rhsValue = `${rhs.column.table_name}.${rhs.column.column_name}`;
          if (isSchemaExists) {
            lhsValue = `${lhsSchemaName}.` + lhsValue;
            rhsValue = `${rhs.column.schema_name}.` + rhsValue;
          }
        } else {
          // Handle LHS - can be COLUMN or FORMULA
          if (isLhsFormula) {
            lhsValue = lhs._displayValue || `=${lhs.expression}`;
          } else {
            lhsValue = lhsColumnName;
          }
          // Handle RHS - can be VALUE, COLUMN, or FORMULA
          if (isRhsFormula) {
            rhsValue = rhs._displayValue || `=${rhs.expression}`;
          } else {
            rhsValue = `${rhs.value?.[0] ?? ""}`;
          }
        }
        const isBetweenOp = operator === "BETWEEN";
        const rhsValueLow = isBetweenOp ? `${rhs.value?.[0] ?? ""}` : "";
        const rhsValueHigh = isBetweenOp ? `${rhs.value?.[1] ?? ""}` : "";
        return (
          <Space direction="hoizontal" key={id} align="center">
            {!isJoinType &&
              (index === 0 ? (
                <div className="width-100px">Where</div>
              ) : (
                <Select
                  className="width-100px"
                  showSearch
                  value={conditionType[index - 1]}
                  onChange={(value) =>
                    handleOptionChange(index, value, "logical_operator")
                  }
                  filterOption={(input, option) =>
                    (option?.label ?? "")
                      .toLowerCase()
                      .includes(input.toLowerCase())
                  }
                  options={[
                    { value: "AND", label: "AND" },
                    { value: "OR", label: "OR" },
                  ]}
                />
              ))}

            <Space
              className={isJoinType ? "join-filter-items" : "filter-items"}
            >
              {isJoinType ? (
                <Select
                  showSearch
                  value={lhsValue}
                  onChange={(value, options) =>
                    handleOptionChange(
                      index,
                      value,
                      "column_name",
                      true,
                      "lhs",
                      options.dataType
                    )
                  }
                  options={getColumnOptions(null, "lhs")}
                  onKeyDown={stopPropagation}
                  className="width-300"
                />
              ) : (
                <Space.Compact>
                  <AutoComplete
                    value={lhsValue}
                    onSelect={(input, options) =>
                      handleOptionChange(
                        index,
                        input,
                        "column_name",
                        true,
                        "lhs",
                        options?.dataType ?? null,
                        "COLUMN"
                      )
                    }
                    onSearch={(input) =>
                      handleOptionChange(
                        index,
                        input,
                        "column_name",
                        true,
                        "lhs",
                        null,
                        isFormulaExpression(input) ? "FORMULA" : "COLUMN"
                      )
                    }
                    options={getColumnOptions(null, "lhs")}
                    filterOption={(input, option) =>
                      (option?.value ?? "")
                        .toLowerCase()
                        .includes(input.toLowerCase())
                    }
                    onKeyDown={stopPropagation}
                    className="width-260"
                    placeholder="Column or =formula"
                  />
                  <Tooltip title="Open Formula Editor">
                    <Button
                      icon={<FunctionOutlined />}
                      onClick={() => openFormulaModal(index, "lhs")}
                    />
                  </Tooltip>
                </Space.Compact>
              )}
              <Select
                value={operator}
                onChange={(value) =>
                  handleOptionChange(
                    index,
                    value,
                    "condition",
                    true,
                    "operator"
                  )
                }
                filterOption={(input, option) =>
                  (option?.label ?? "")
                    .toLowerCase()
                    .includes(input.toLowerCase())
                }
                options={
                  type === "join"
                    ? getOperators(type, lhs.column?.data_type).filter(
                        (item) => {
                          if (joinType === "Cross" && item.value !== "EQ") {
                            return false;
                          }

                          return ["NULL", "NOTNULL"].includes(item.value)
                            ? false
                            : true;
                        }
                      )
                    : getOperators(
                        type,
                        isLhsFormula ? "Formula" : lhs.column?.data_type
                      )
                }
                onKeyDown={stopPropagation}
                className={isJoinType ? "width-100px" : "width-300"}
              />

              {isJoinType ? (
                <Select
                  showSearch
                  value={
                    // For JOINs, only show value if it exists in dropdown options
                    getColumnOptions(
                      lhsColumnName,
                      "rhs",
                      lhs.column.data_type
                    ).some((option) => option.value === rhsValue)
                      ? rhsValue
                      : undefined
                  }
                  onChange={(value) =>
                    handleOptionChange(index, value, "column_name", true, "rhs")
                  }
                  filterOption={(input, option) =>
                    (option?.value ?? "")
                      .toLowerCase()
                      .includes(input.toLowerCase())
                  }
                  options={getColumnOptions(
                    lhsColumnName,
                    "rhs",
                    lhs.column.data_type
                  )}
                  onKeyDown={stopPropagation}
                  className="width-300"
                />
              ) : isBetweenOp ? (
                <div
                  className="filter-between-inputs"
                  style={{
                    display: "flex",
                    alignItems: "center",
                    gap: 4,
                    width: 150,
                  }}
                >
                  <AutoComplete
                    value={rhsValueLow}
                    onSearch={(input) => handleBetweenChange(index, input, 0)}
                    onKeyDown={stopPropagation}
                    placeholder="Low"
                  />
                  <span>&</span>
                  <AutoComplete
                    value={rhsValueHigh}
                    onSearch={(input) => handleBetweenChange(index, input, 1)}
                    onKeyDown={stopPropagation}
                    placeholder="High"
                  />
                </div>
              ) : (
                <Space.Compact>
                  <AutoComplete
                    value={rhsValue}
                    onSelect={(input, options) =>
                      handleOptionChange(
                        index,
                        input,
                        isJoinType ? "column_name" : "value",
                        true,
                        "rhs",
                        options?.dataType ?? null,
                        "COLUMN"
                      )
                    }
                    onSearch={(input, options) =>
                      handleOptionChange(
                        index,
                        input,
                        isJoinType ? "column_name" : "value",
                        true,
                        "rhs",
                        options?.dataType ?? null,
                        "VALUE"
                      )
                    }
                    options={getColumnOptions(lhsColumnName, "rhs")}
                    filterOption={(input, option) =>
                      (option?.value ?? "")
                        .toLowerCase()
                        .includes(input.toLowerCase())
                    }
                    disabled={["TRUE", "FALSE", "NULL", "NOTNULL"].includes(
                      operator
                    )}
                    onKeyDown={stopPropagation}
                    className="width-260"
                  />
                  <Tooltip title="Open Formula Editor">
                    <Button
                      icon={<FunctionOutlined />}
                      onClick={() => openFormulaModal(index, "rhs")}
                      disabled={["TRUE", "FALSE", "NULL", "NOTNULL"].includes(
                        operator
                      )}
                    />
                  </Tooltip>
                </Space.Compact>
              )}

              <Button
                icon={<DeleteOutlined />}
                onClick={(e) => handleRemove(index, e)}
                disabled={isJoinType && filterConditions.length === 1}
                danger
              />
            </Space>
          </Space>
        );
      })}

      <Typography.Text
        type="danger"
        className="hint"
        style={{
          display: showHint ? "block" : "none",
        }}
      >
        *Only numbers are allowed.
      </Typography.Text>

      <Button
        onClick={handleAdd}
        className="p-0 bg-transparent"
        icon={<PlusOutlined />}
        type="text"
        disabled={
          // For joins, only check length limit; for model/source, also check if columns exist
          isJoinType
            ? filterConditions.length >= 5
            : !columnDetails?.columns?.filter?.length ||
              filterConditions.length >= 5
        }
      >
        {filterConditions.length ? "Add another filter" : "Add a filter"}
      </Button>
      {type === "model" && (
        <div className="flex-end-container">
          <Button onClick={handleCancelBtn}>Cancel</Button>
          <Button
            className="ml-10"
            onClick={handleSave}
            disabled={isLoading || !isModified || validateSaveBtn()}
            type="primary"
            loading={isLoading}
          >
            Save
          </Button>
        </div>
      )}

      {/* Formula Editor Modal */}
      <Modal
        title={
          <Space>
            <FunctionOutlined />
            <span>
              Formula Editor ({formulaModal.side === "lhs" ? "Left" : "Right"}{" "}
              Side)
            </span>
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
            Enter a formula expression using available columns and functions.
          </Typography.Text>
        </div>

        <FormulaEditor
          allColumns={getAllColumns()}
          value={formulaModal.formula}
          setValue={handleFormulaChange}
          clear={clearFormula}
          populate={null}
          onValidationChange={handleValidationChange}
        />

        {/* Formula validation errors */}
        {!formulaValidation.isValid && formulaValidation.errors.length > 0 && (
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
            Examples: UPPER(name), CONCAT(first_name, &quot; &quot;, last_name),
            YEAR(created_at)
          </Typography.Text>
        </div>
      </Modal>
    </Space>
  );
}

Filter.propTypes = {
  columnDetails: PropTypes.object.isRequired,
  filterConditions: PropTypes.arrayOf(PropTypes.object).isRequired,
  setFilterConditions: PropTypes.func.isRequired,
  type: PropTypes.string,
  join: PropTypes.array,
  setJoins: PropTypes.func,
  joinIndex: PropTypes.number,
  joinColumnDetails: PropTypes.array,
  joinSourceColumns: PropTypes.array,
  getCriteria: PropTypes.func,
  isLoading: PropTypes.bool,
  setOpen: PropTypes.func,
  updateSpec: PropTypes.func,
  spec: PropTypes.object,
  conditionType: PropTypes.array,
  setConditionType: PropTypes.func,
  saveTransformation: PropTypes.func.isRequired,
  handleDeleteTransformation: PropTypes.func.isRequired,
  joinType: PropTypes.string,
};

export { Filter };
