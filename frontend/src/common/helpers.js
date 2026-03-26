import { produce } from "immer";
import isUndefined from "lodash/isUndefined.js";
import { isArray, isEmpty, isNull, isObject, omitBy } from "lodash";
import { useEffect } from "react";
import cronstrue from "cronstrue";

import { useSessionStore } from "../store/session-store";
import { usePermissionStore } from "../store/permission-store";
import { useProjectStore } from "../store/project-store";
import { getRenameColumnSpec } from "../ide/editor/no-code-model/helper";

const timeArray = [
  { value: "GT", label: " > " },
  { value: "GTE", label: " >= " },
  { value: "LT", label: " < " },
  { value: "LTE", label: " <= " },
];
const operators = {
  default: [
    { value: "EQ", label: " == " },
    { value: "NEQ", label: " != " },
    { value: "NULL", label: "Null" },
    { value: "NOTNULL", label: "Not Null" },
  ],
  Number: [
    { value: "GT", label: " > " },
    { value: "GTE", label: " >= " },
    { value: "LT", label: " < " },
    { value: "LTE", label: " <= " },
    { value: "BETWEEN", label: "Between" },
  ],
  String: [
    { value: "IN", label: "In" },
    { value: "NOTIN", label: "Not In" },
    { value: "CONTAINS", label: "Contains" },
    { value: "NOTCONTAINS", label: "Not Contains" },
    { value: "STARTSWITH", label: "Starts With" },
    { value: "ENDSWITH", label: "Ends With" },
  ],
  // Formula type: comparison and list operators (NO LIKE operators - blocked in backend)
  Formula: [
    { value: "GT", label: " > " },
    { value: "GTE", label: " >= " },
    { value: "LT", label: " < " },
    { value: "LTE", label: " <= " },
    { value: "IN", label: "In" },
    { value: "NOTIN", label: "Not In" },
    { value: "BETWEEN", label: "Between" },
  ],
  JoinString: [
    { value: "EQ", label: " == " },
    { value: "NEQ", label: " != " },
  ],
  Boolean: [
    { value: "TRUE", label: "True" },
    { value: "FALSE", label: "False" },
    { value: "NULL", label: "Null" },
    { value: "NOTNULL", label: "Not Null" },
  ],
  Time: [...timeArray, { value: "BETWEEN", label: "Between" }],
  Date: [...timeArray, { value: "BETWEEN", label: "Between" }],
};

const getOperators = (type, dataType) => {
  if (dataType === "Boolean") {
    return operators[dataType];
  }
  if (type === "join" && dataType === "String") {
    return operators["JoinString"];
  }
  return [...operators.default, ...(operators[dataType] ?? [])];
};
// Function to generate a unique key every time
const generateKey = () => {
  const timestamp = Date.now();
  const randomValue = crypto.randomUUID();
  return `${timestamp}-${randomValue}`;
};

// Function to stop propagation when arrow keys are pressed
const stopPropagation = (e) => {
  if ([13, 37, 38, 39, 40].includes(e.keyCode)) {
    e.stopPropagation();
  }
};

// Function to remove id from array of objects and omit undefined values
const removeIdFromObjects = (objects) => {
  return produce(objects, (draft) => {
    draft.forEach((item) => {
      delete item.id;
      Object.keys(item).forEach((key) => {
        if (isUndefined(item[key])) {
          delete item[key];
        }
      });
    });
  });
};

// Function to add id to array of objects
const addIdToObjects = (objects) => {
  return produce(objects, (draft) => {
    draft?.forEach((item) => {
      item.id = generateKey();
    });
  });
};

const checkPermission = (resource, action) => {
  if (!useSessionStore.getState().sessionDetails?.is_cloud) return true;
  const permissions = usePermissionStore.getState().permissionDetails;
  const sessionDetails = useSessionStore.getState().sessionDetails;
  // Handle case when session is expired/undefined/empty (e.g., after logout)
  if (!sessionDetails || Object.keys(sessionDetails).length === 0) return false;
  // Always use server-returned permissions — never trust client-side role
  return permissions[resource]?.[action] ?? false;
};

// Function to get filter conditions
const getFilterCondition = (data, type = "COLUMN", dataType = null) => {
  const filterCondition = { type: type };
  // Currently we have type COLUMN, VALUE, FORMULA
  switch (type) {
    case "COLUMN": {
      // to handle db which doesn't have schema
      const [columnName, tableName, schemaName] = data.reverse();
      filterCondition.column = {
        schema_name: schemaName ?? null,
        table_name: tableName,
        column_name: columnName,
        data_type: dataType,
      };
      break;
    }
    case "VALUE": {
      filterCondition.value = data;
      filterCondition.type = "COLUMN";
      break;
    }
    case "FORMULA": {
      // For formula expressions (e.g., "YEAR(order_date)", "col1 * col2")
      filterCondition.expression = data;
      break;
    }
  }
  return filterCondition;
};

/**
 * Check if a value is a formula expression (starts with =)
 * @param {string} value - The value to check
 * @return {boolean} - True if the value is a formula expression
 */
const isFormulaExpression = (value) => {
  return typeof value === "string" && value.trim().startsWith("=");
};

/**
 * Extract formula expression from value (removes = prefix)
 * @param {string} value - The value with = prefix
 * @return {string} - The formula expression without = prefix
 */
const extractFormulaExpression = (value) => {
  if (!isFormulaExpression(value)) return value;
  return value.trim().substring(1).trim();
};

/**
 * Validate a formula expression (basic frontend validation)
 * @param {string} expression - The formula expression to validate
 * @param {string[]} availableColumns - List of available column names
 * @return {{valid: boolean, errors: string[]}} - Validation result
 */
const validateFormulaExpression = (expression, availableColumns = []) => {
  const errors = [];

  if (!expression || expression.trim() === "") {
    errors.push("Formula expression cannot be empty");
    return { valid: false, errors };
  }

  // Check balanced parentheses
  let depth = 0;
  for (const char of expression) {
    if (char === "(") depth++;
    if (char === ")") depth--;
    if (depth < 0) {
      errors.push("Unbalanced parentheses: unexpected closing parenthesis");
      break;
    }
  }
  if (depth > 0) {
    errors.push("Unbalanced parentheses: missing closing parenthesis");
  }

  // Check for unclosed quotes
  const singleQuotes = (expression.match(/'/g) || []).length;
  const doubleQuotes = (expression.match(/"/g) || []).length;
  if (singleQuotes % 2 !== 0) {
    errors.push("Unclosed single quote");
  }
  if (doubleQuotes % 2 !== 0) {
    errors.push("Unclosed double quote");
  }

  return { valid: errors.length === 0, errors };
};

const getTableFullname = (schemaName, tableName) => {
  let tableFullname = tableName;
  if (schemaName) {
    tableFullname = schemaName + "." + tableName;
  }
  return tableFullname;
};

const getBaseUrl = () => {
  const location = window.location.href;
  const url = new URL(location).origin;
  return url;
};

const handleUserLogout = () => {
  localStorage.removeItem("orgid");
  localStorage.removeItem("session-storage");
  // Navigate directly to logout endpoint to allow SSO redirect
  // (fetch() doesn't follow cross-origin redirects for SSO logout)
  window.location.href = "/api/v1/logout";
};

const capitaliseString = (str) => {
  return str[0].toUpperCase() + str.slice(1, str.length);
};

const renameMap = (spec, transformationId) => {
  const renameColumnSpec = getRenameColumnSpec(
    spec?.transform,
    transformationId
  );
  return (
    renameColumnSpec?.mappings?.reduce((acc, item) => {
      acc[item.old_name] = item.new_name;
      return acc;
    }, {}) || {}
  );
};

export const useEscapeKey = (isOpen, onEscape) => {
  useEffect(() => {
    const handleEscapeKey = (event) => {
      if (event.key === "Escape" && isOpen) {
        onEscape();
      }
    };

    window.addEventListener("keydown", handleEscapeKey);
    return () => {
      window.removeEventListener("keydown", handleEscapeKey);
    };
  }, [isOpen, onEscape]);
};

const openNewlyGeneratedModel = (modelName) => {
  const projectId = useProjectStore.getState().projectId;
  const { projectDetails, setOpenedTabs } = useProjectStore.getState();
  const openedTabs = projectDetails?.[projectId]?.openedTabs || [];

  const key = `${useProjectStore.getState().projectName}/models/${modelName}`;

  const isAlreadyOpen = openedTabs.some((tab) => tab.key === key);
  if (!isAlreadyOpen) {
    const newTab = {
      key,
      label: modelName,
      type: "NO_CODE_MODEL",
      extension: modelName,
    };
    setOpenedTabs([...openedTabs, newTab], projectId);
  }
};

const getActiveModelName = (projectId, projectDetails) => {
  const modelInfo = projectDetails?.[projectId]?.focussedTab;

  if (!projectId || !modelInfo || Object.keys(modelInfo).length === 0) {
    return null;
  }

  if (modelInfo.type === "NO_CODE_MODEL" && modelInfo.key) {
    return modelInfo.key.split("/").pop() ?? null;
  }

  return modelInfo.key ?? null;
};

const removeUnwantedKeys = (data) => {
  if (isArray(data)) {
    const filteredArray = data.map((item) => removeUnwantedKeys(item));
    return filteredArray.filter((item) => {
      if (isArray(item)) {
        return item.length > 0;
      }
      return item !== undefined;
    });
  }
  if (isObject(data)) {
    const filteredObj = omitBy(data, isNull);
    for (const key in filteredObj) {
      if (isObject(filteredObj[key])) {
        filteredObj[key] = removeUnwantedKeys(filteredObj[key]);
      }
    }
    const nonEmptyKeys = Object.keys(filteredObj).filter((key) => {
      const value = filteredObj[key];
      return (
        value !== undefined &&
        !(isArray(value) && value.length === 0) &&
        !(isObject(value) && Object.keys(value).length === 0)
      );
    });
    const finalObj = Object.fromEntries(
      nonEmptyKeys.map((key) => [key, filteredObj[key]])
    );
    if (isEmpty(finalObj)) {
      return {};
    }
    return finalObj;
  }
  return data;
};

const sanitizeLowercaseOnly = (value) => {
  return value.replace(/[^a-z]/g, "");
};

const truncateText = (text, maxLength = 50) => {
  if (!text) return "";
  return text.length > maxLength ? text.slice(0, maxLength) + "..." : text;
};

const getTooltipText = (data, type) => {
  if (type === "interval") {
    return `every ${data.every} ${data.period}`;
  }
  return `${cronstrue.toString(data.cron_expression)}`;
};

const getRelativeTime = (dateString) => {
  if (!dateString) return "";
  const now = new Date();
  const then = new Date(dateString);
  const diffMs = now - then;
  const diffMins = Math.floor(diffMs / 60000);
  if (diffMins < 1) return "just now";
  if (diffMins < 60) return `${diffMins}m ago`;
  const diffHrs = Math.floor(diffMins / 60);
  if (diffHrs < 24) return `${diffHrs}h ago`;
  const diffDays = Math.floor(diffHrs / 24);
  if (diffDays < 30) return `${diffDays}d ago`;
  const diffMonths = Math.floor(diffDays / 30);
  return `${diffMonths}mo ago`;
};

export {
  generateKey,
  stopPropagation,
  removeIdFromObjects,
  addIdToObjects,
  getFilterCondition,
  getTableFullname,
  getOperators,
  getBaseUrl,
  handleUserLogout,
  renameMap,
  capitaliseString,
  checkPermission,
  openNewlyGeneratedModel,
  getActiveModelName,
  removeUnwantedKeys,
  sanitizeLowercaseOnly,
  truncateText,
  getTooltipText,
  isFormulaExpression,
  extractFormulaExpression,
  validateFormulaExpression,
  getRelativeTime,
};
