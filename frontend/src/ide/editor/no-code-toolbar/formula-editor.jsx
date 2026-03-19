import * as monaco from "monaco-editor";
import { useEffect, useRef, useState, useCallback, useMemo } from "react";
import PropTypes from "prop-types";
import debounce from "lodash/debounce";

import { THEME } from "../../../common/constants.js";
import { useUserStore } from "../../../store/user-store";
import { orgStore } from "../../../store/org-store.js";
import { useAxiosPrivate } from "../../../service/axios-service.js";

/**
 * Function signatures for argument validation and signature help
 * { min: minimum args, max: maximum args (Infinity for variadic), params: parameter descriptions }
 */
const FUNCTION_SIGNATURES = {
  // Math functions
  SUM: {
    min: 1,
    max: Infinity,
    params: ["value1", "[value2]", "..."],
    description: "Returns the sum of values",
  },
  AVERAGE: {
    min: 1,
    max: Infinity,
    params: ["value1", "[value2]", "..."],
    description: "Returns the average of values",
  },
  COUNT: {
    min: 1,
    max: Infinity,
    params: ["value1", "[value2]", "..."],
    description: "Counts the number of values",
  },
  MAX: {
    min: 1,
    max: Infinity,
    params: ["value1", "[value2]", "..."],
    description: "Returns the maximum value",
  },
  MIN: {
    min: 1,
    max: Infinity,
    params: ["value1", "[value2]", "..."],
    description: "Returns the minimum value",
  },
  ABS: {
    min: 1,
    max: 1,
    params: ["number"],
    description: "Returns the absolute value",
  },
  ROUND: {
    min: 1,
    max: 2,
    params: ["number", "[decimals]"],
    description: "Rounds to specified decimals",
  },
  FLOOR: {
    min: 1,
    max: 1,
    params: ["number"],
    description: "Rounds down to nearest integer",
  },
  CEIL: {
    min: 1,
    max: 1,
    params: ["number"],
    description: "Rounds up to nearest integer",
  },
  POWER: {
    min: 2,
    max: 2,
    params: ["base", "exponent"],
    description: "Returns base raised to exponent",
  },
  SQRT: {
    min: 1,
    max: 1,
    params: ["number"],
    description: "Returns the square root",
  },
  LOG: {
    min: 1,
    max: 2,
    params: ["number", "[base]"],
    description: "Returns the logarithm",
  },
  EXP: {
    min: 1,
    max: 1,
    params: ["number"],
    description: "Returns e raised to power",
  },
  MOD: {
    min: 2,
    max: 2,
    params: ["number", "divisor"],
    description: "Returns the remainder",
  },

  // Text functions
  CONCAT: {
    min: 1,
    max: Infinity,
    params: ["text1", "[text2]", "..."],
    description: "Concatenates text strings",
  },
  LEFT: {
    min: 2,
    max: 2,
    params: ["text", "num_chars"],
    description: "Returns leftmost characters",
  },
  RIGHT: {
    min: 2,
    max: 2,
    params: ["text", "num_chars"],
    description: "Returns rightmost characters",
  },
  LPAD: {
    min: 3,
    max: 3,
    params: ["text", "length", "pad_char"],
    description: "Left-pads text to target length",
  },
  RPAD: {
    min: 3,
    max: 3,
    params: ["text", "length", "pad_char"],
    description: "Right-pads text to target length",
  },
  MID: {
    min: 3,
    max: 3,
    params: ["text", "start", "num_chars"],
    description: "Returns characters from middle",
  },
  LEN: {
    min: 1,
    max: 1,
    params: ["text"],
    description: "Returns the length of text",
  },
  TRIM: {
    min: 1,
    max: 1,
    params: ["text"],
    description: "Removes extra spaces",
  },
  UPPER: {
    min: 1,
    max: 1,
    params: ["text"],
    description: "Converts to uppercase",
  },
  LOWER: {
    min: 1,
    max: 1,
    params: ["text"],
    description: "Converts to lowercase",
  },
  REPLACE: {
    min: 3,
    max: 3,
    params: ["text", "old_text", "new_text"],
    description: "Replaces text",
  },
  SUBSTITUTE: {
    min: 3,
    max: 4,
    params: ["text", "old_text", "new_text", "[instance]"],
    description: "Substitutes text",
  },
  FIND: {
    min: 2,
    max: 3,
    params: ["find_text", "within_text", "[start]"],
    description: "Finds text position",
  },
  SEARCH: {
    min: 2,
    max: 3,
    params: ["find_text", "within_text", "[start]"],
    description: "Searches text (case-insensitive)",
  },
  TEXT: {
    min: 2,
    max: 2,
    params: ["value", "format"],
    description: "Formats value as text",
  },
  VALUE: {
    min: 1,
    max: 1,
    params: ["text"],
    description: "Converts text to number",
  },

  // Regex functions
  REGEX_EXTRACT: {
    min: 2,
    max: 3,
    params: ["text", "pattern", "[group_index]"],
    description: "Extracts text matching regex pattern",
  },
  REGEX_REPLACE: {
    min: 3,
    max: 3,
    params: ["text", "pattern", "replacement"],
    description: "Replaces text matching regex",
  },
  REGEX_SEARCH: {
    min: 2,
    max: 2,
    params: ["text", "pattern"],
    description: "Searches for regex pattern",
  },

  // Logical functions
  IF: {
    min: 3,
    max: 3,
    params: ["condition", "value_if_true", "value_if_false"],
    description: "Returns value based on condition",
  },
  AND: {
    min: 2,
    max: Infinity,
    params: ["logical1", "logical2", "..."],
    description: "Returns TRUE if all are true",
  },
  OR: {
    min: 2,
    max: Infinity,
    params: ["logical1", "logical2", "..."],
    description: "Returns TRUE if any is true",
  },
  NOT: {
    min: 1,
    max: 1,
    params: ["logical"],
    description: "Reverses logical value",
  },
  ISNULL: {
    min: 1,
    max: 1,
    params: ["value"],
    description: "Checks if value is null",
  },
  IFNULL: {
    min: 2,
    max: 2,
    params: ["value", "default"],
    description: "Returns default if null",
  },
  COALESCE: {
    min: 1,
    max: Infinity,
    params: ["value1", "[value2]", "..."],
    description: "Returns first non-null value",
  },

  // Type conversion
  CAST: {
    min: 2,
    max: 2,
    params: ["value", '"TYPE"'],
    description: 'Converts to type (e.g., CAST(col, "INT"))',
  },

  // Date functions
  DATE: {
    min: 3,
    max: 3,
    params: ["year", "month", "day"],
    description: "Creates a date",
  },
  YEAR: { min: 1, max: 1, params: ["date"], description: "Returns the year" },
  MONTH: { min: 1, max: 1, params: ["date"], description: "Returns the month" },
  DAY: { min: 1, max: 1, params: ["date"], description: "Returns the day" },
  HOUR: {
    min: 1,
    max: 1,
    params: ["datetime"],
    description: "Returns the hour",
  },
  MINUTE: {
    min: 1,
    max: 1,
    params: ["datetime"],
    description: "Returns the minute",
  },
  SECOND: {
    min: 1,
    max: 1,
    params: ["datetime"],
    description: "Returns the second",
  },
  NOW: { min: 0, max: 0, params: [], description: "Returns current datetime" },
  TODAY: { min: 0, max: 0, params: [], description: "Returns current date" },
  DATEDIFF: {
    min: 3,
    max: 3,
    params: ["unit", "start_date", "end_date"],
    description: "Returns difference between dates",
  },
  DATEADD: {
    min: 3,
    max: 3,
    params: ["unit", "amount", "date"],
    description: "Adds interval to date",
  },

  // No-arg functions
  RANDOM: {
    min: 0,
    max: 0,
    params: [],
    description: "Returns random number 0-1",
  },
  PI: { min: 0, max: 0, params: [], description: "Returns PI constant" },
  E: { min: 0, max: 0, params: [], description: "Returns Euler's number" },
};

/**
 * Parses function calls and counts arguments (handling nested functions and strings)
 * @param {string} argsString - The string inside function parentheses
 * @return {number} - Number of arguments
 */
const countFunctionArgs = (argsString) => {
  if (!argsString || argsString.trim() === "") return 0;

  let depth = 0;
  let inString = false;
  let argCount = 1;

  for (let i = 0; i < argsString.length; i++) {
    const char = argsString[i];

    // Handle string boundaries
    if (char === '"' && (i === 0 || argsString[i - 1] !== "\\")) {
      inString = !inString;
      continue;
    }

    if (!inString) {
      if (char === "(") depth++;
      else if (char === ")") depth--;
      else if (char === "," && depth === 0) argCount++;
    }
  }

  return argCount;
};

/**
 * Extracts function call info at a position for argument validation
 * @param {string} formula - The formula to extract function calls from
 * @return {Array<{funcName: string, argsString: string, startIndex: number, endIndex: number}>} Array of function call info
 */
const extractFunctionCalls = (formula) => {
  const calls = [];
  const funcPattern = /([A-Z_][A-Z0-9_]*)\s*\(/gi;
  let match;

  while ((match = funcPattern.exec(formula)) !== null) {
    const funcName = match[1].toUpperCase();
    const openParenIndex = match.index + match[0].length - 1;

    // Find matching close paren
    let depth = 1;
    let closeParenIndex = -1;
    let inString = false;

    for (let i = openParenIndex + 1; i < formula.length && depth > 0; i++) {
      const char = formula[i];

      if (char === '"' && (i === 0 || formula[i - 1] !== "\\")) {
        inString = !inString;
        continue;
      }

      if (!inString) {
        if (char === "(") depth++;
        else if (char === ")") {
          depth--;
          if (depth === 0) closeParenIndex = i;
        }
      }
    }

    if (closeParenIndex !== -1) {
      const argsString = formula.substring(openParenIndex + 1, closeParenIndex);
      calls.push({
        funcName,
        argsString,
        startIndex: match.index,
        endIndex: closeParenIndex + 1,
      });
    }
  }

  return calls;
};

/**
 * Validates formula syntax and returns array of errors
 * @param {string} formula - The formula to validate
 * @param {string[]} formulaList - List of valid formula names
 * @param {string[]} columns - List of valid column names
 * @return {Array} Array of validation errors with message, startCol, endCol, and optional severity
 */
const validateFormula = (formula, formulaList, columns) => {
  const errors = [];

  if (!formula || formula.trim() === "") {
    return errors; // Empty formula is valid (will be caught by required field)
  }

  // 1. Check balanced parentheses
  let parenDepth = 0;
  let lastOpenParen = -1;
  let inString = false;

  for (let i = 0; i < formula.length; i++) {
    const char = formula[i];

    // Track string state (ignore parens inside strings)
    if (char === '"' && (i === 0 || formula[i - 1] !== "\\")) {
      inString = !inString;
      continue;
    }

    if (!inString) {
      if (char === "(") {
        parenDepth++;
        lastOpenParen = i;
      }
      if (char === ")") {
        parenDepth--;
        if (parenDepth < 0) {
          errors.push({
            message: "Unexpected closing parenthesis",
            startCol: i + 1,
            endCol: i + 2,
          });
          parenDepth = 0; // Reset to continue checking
        }
      }
    }
  }

  if (parenDepth > 0) {
    errors.push({
      message: "Missing closing parenthesis",
      startCol: lastOpenParen + 1,
      endCol: lastOpenParen + 2,
    });
  }

  // 2. Check balanced quotes
  let quoteCount = 0;
  let quoteStart = -1;
  for (let i = 0; i < formula.length; i++) {
    if (formula[i] === '"' && (i === 0 || formula[i - 1] !== "\\")) {
      if (quoteCount % 2 === 0) {
        quoteStart = i;
      }
      quoteCount++;
    }
  }

  if (quoteCount % 2 !== 0) {
    errors.push({
      message: "Unclosed string - missing closing quote",
      startCol: quoteStart + 1,
      endCol: formula.length + 1,
    });
  }

  // 3. Check for unknown functions
  const funcPattern = /([A-Z_][A-Z0-9_]*)\s*\(/gi;
  let match;
  while ((match = funcPattern.exec(formula)) !== null) {
    const funcName = match[1].toUpperCase();
    // Skip if it's a column name being used as function (edge case)
    if (
      formulaList &&
      formulaList.length > 0 &&
      !formulaList.includes(funcName)
    ) {
      errors.push({
        message: `Unknown function: ${funcName}`,
        startCol: match.index + 1,
        endCol: match.index + funcName.length + 1,
      });
    }
  }

  // 4. Check for empty function calls like FUNC()
  const emptyFuncPattern = /([A-Z_][A-Z0-9_]*)\s*\(\s*\)/gi;
  while ((match = emptyFuncPattern.exec(formula)) !== null) {
    const funcName = match[1].toUpperCase();
    // Some functions like RANDOM(), E(), PI() are valid without args
    const noArgFunctions = ["RANDOM", "E", "PI", "NOW", "TODAY"];
    if (!noArgFunctions.includes(funcName)) {
      errors.push({
        message: `Function ${funcName} requires arguments`,
        startCol: match.index + 1,
        endCol: match.index + match[0].length + 1,
      });
    }
  }

  // 4.5. Check for period used as separator instead of comma (common mistake)
  // Pattern: "string"."string" or value.value inside function arguments
  const periodSeparatorPattern = /"\s*\.\s*"/g;
  while ((match = periodSeparatorPattern.exec(formula)) !== null) {
    errors.push({
      message: 'Invalid separator "." - use comma "," to separate arguments',
      startCol: match.index + 1,
      endCol: match.index + match[0].length + 1,
    });
  }

  // 5. Validate argument counts for known functions
  const functionCalls = extractFunctionCalls(formula);
  for (const call of functionCalls) {
    const sig = FUNCTION_SIGNATURES[call.funcName];
    if (sig) {
      const argCount = countFunctionArgs(call.argsString);

      if (argCount < sig.min) {
        errors.push({
          message: `${call.funcName} requires at least ${sig.min} argument${
            sig.min > 1 ? "s" : ""
          }, got ${argCount}`,
          startCol: call.startIndex + 1,
          endCol: call.endIndex + 1,
        });
      } else if (argCount > sig.max && sig.max !== Infinity) {
        errors.push({
          message: `${call.funcName} accepts at most ${sig.max} argument${
            sig.max > 1 ? "s" : ""
          }, got ${argCount}`,
          startCol: call.startIndex + 1,
          endCol: call.endIndex + 1,
        });
      }
    }
  }

  // 6. Validate column references (as warnings)
  if (columns && columns.length > 0) {
    // Extract identifiers that are not inside strings and not function names
    const identifierPattern = /\b([a-zA-Z_][a-zA-Z0-9_]*)\b/g;
    const funcNames = new Set(formulaList || []);

    // Track positions inside strings
    const stringRanges = [];
    let inStr = false;
    let strStart = 0;
    for (let i = 0; i < formula.length; i++) {
      if (formula[i] === '"' && (i === 0 || formula[i - 1] !== "\\")) {
        if (!inStr) {
          inStr = true;
          strStart = i;
        } else {
          inStr = false;
          stringRanges.push({ start: strStart, end: i });
        }
      }
    }

    const isInString = (pos) =>
      stringRanges.some((r) => pos >= r.start && pos <= r.end);
    const isFollowedByParen = (pos, len) => {
      const afterIdent = formula.substring(pos + len).match(/^\s*\(/);
      return afterIdent !== null;
    };

    while ((match = identifierPattern.exec(formula)) !== null) {
      const ident = match[1];
      const identUpper = ident.toUpperCase();

      // Skip if inside string
      if (isInString(match.index)) continue;

      // Skip if it's a function name (followed by parenthesis)
      if (isFollowedByParen(match.index, ident.length)) continue;

      // Skip if it's a known function name
      if (funcNames.has(identUpper)) continue;

      // Skip common keywords/literals and type names
      const keywords = [
        // Logical
        "TRUE",
        "FALSE",
        "NULL",
        "AS",
        "AND",
        "OR",
        "NOT",
        // Type names (for CAST)
        "INT",
        "INT8",
        "INT16",
        "INT32",
        "INT64",
        "INTEGER",
        "FLOAT",
        "FLOAT32",
        "FLOAT64",
        "DOUBLE",
        "DECIMAL",
        "NUMERIC",
        "STRING",
        "TEXT",
        "VARCHAR",
        "CHAR",
        "BOOL",
        "BOOLEAN",
        "DATE",
        "DATETIME",
        "TIMESTAMP",
        "TIME",
        "BINARY",
        "BYTES",
        "ARRAY",
        "MAP",
        "STRUCT",
        "JSON",
      ];
      if (keywords.includes(identUpper)) continue;

      // Check if it's a valid column (case-insensitive)
      const columnsUpper = columns.map((c) => c.toUpperCase());
      if (!columnsUpper.includes(identUpper)) {
        errors.push({
          message: `Unknown column: ${ident}`,
          startCol: match.index + 1,
          endCol: match.index + ident.length + 1,
          severity: "warning",
        });
      }
    }
  }

  return errors;
};

/**
 * Sets Monaco editor markers for validation errors and warnings
 * @param {object} editorInstance - The Monaco editor instance
 * @param {object} monacoInstance - The Monaco module instance
 * @param {Array} errors - Array of validation errors
 * @return {void}
 */
const setEditorMarkers = (editorInstance, monacoInstance, errors) => {
  if (!editorInstance || !monacoInstance) return;

  const model = editorInstance.getModel();
  if (!model) return;

  const markers = errors.map((err) => ({
    severity:
      err.severity === "warning"
        ? monacoInstance.MarkerSeverity.Warning
        : monacoInstance.MarkerSeverity.Error,
    message: err.message,
    startLineNumber: 1,
    startColumn: err.startCol || 1,
    endLineNumber: 1,
    endColumn: err.endCol || model.getLineMaxColumn(1),
  }));

  monacoInstance.editor.setModelMarkers(model, "formula-validation", markers);
};

/**
 * Finds the current function context at cursor position for signature help
 * @param {string} text - The formula text
 * @param {number} position - The cursor position
 * @return {object|null} Function context with funcName and activeParameter, or null
 */
const findFunctionContext = (text, position) => {
  // Work backwards from position to find function name
  let depth = 0;
  let funcStart = -1;
  let argIndex = 0;

  for (let i = position - 1; i >= 0; i--) {
    const char = text[i];

    if (char === ")") depth++;
    else if (char === "(") {
      if (depth === 0) {
        funcStart = i;
        break;
      }
      depth--;
    } else if (char === "," && depth === 0) {
      argIndex++;
    }
  }

  if (funcStart === -1) return null;

  // Find function name before the opening paren
  const beforeParen = text.substring(0, funcStart);
  const funcMatch = beforeParen.match(/([A-Z_][A-Z0-9_]*)\s*$/i);

  if (!funcMatch) return null;

  return {
    funcName: funcMatch[1].toUpperCase(),
    activeParameter: argIndex,
  };
};

// Store providers globally to ensure proper cleanup
const globalProviders = {
  completion: null,
  hover: null,
  signatureHelp: null,
  codeAction: null,
};

function FormulaEditor({
  allColumns,
  value,
  setValue,
  clear,
  populate,
  formulaList,
  formulaDetails,
  onValidationChange,
}) {
  const axios = useAxiosPrivate();
  const { userDetails } = useUserStore();
  const { selectedOrgId } = orgStore();
  const editorRef = useRef(null);
  const monacoRef = useRef(null);
  const [editor, setEditor] = useState(null);
  const [localFormulaDetails, setLocalFormulaDetails] = useState([]);
  const [localFormulaList, setLocalFormulaList] = useState([]);
  const [isInitialized, setIsInitialized] = useState(false);

  // Track state changes to prevent conflicts
  const isUpdatingRef = useRef(false);
  const lastClearRef = useRef(clear);
  const lastPopulateRef = useRef(populate);

  // Debounced validation function
  const debouncedValidate = useMemo(
    () =>
      debounce((formula, formulas, cols, editorInstance, monacoInstance) => {
        const errors = validateFormula(formula, formulas, cols);
        setEditorMarkers(editorInstance, monacoInstance, errors);

        // Notify parent component of validation state
        if (onValidationChange) {
          onValidationChange({
            isValid: errors.length === 0,
            errors: errors,
          });
        }
      }, 300),
    [onValidationChange]
  );

  // Run validation when value changes
  useEffect(() => {
    if (editor && monacoRef.current && isInitialized) {
      debouncedValidate(
        value,
        localFormulaList,
        allColumns,
        editor,
        monacoRef.current
      );
    }

    // Cleanup debounce on unmount
    return () => {
      debouncedValidate.cancel();
    };
  }, [
    value,
    localFormulaList,
    allColumns,
    editor,
    isInitialized,
    debouncedValidate,
  ]);

  // Use props data or fallback to API call
  const getFormulas = async () => {
    // If props are provided, use them
    if (formulaList && formulaDetails) {
      return {
        formula_list: formulaList,
        formulas: formulaDetails,
      };
    }

    // Fallback to API call if props not provided
    let data = {
      formulas: [],
      formula_list: [],
    };
    const requestOptions = {
      method: "GET",
      url: `/api/v1/visitran/${selectedOrgId || "default_org"}/formulas`,
    };
    try {
      const response = await axios(requestOptions);
      data = response?.data || data;
    } catch (error) {
      console.error("Error fetching keywords:", error);
    }
    return data;
  };

  const getCurrentTheme = () => {
    return userDetails.currentTheme === THEME.DARK ? "dark" : "light";
  };

  // Clean up all providers
  const cleanupProviders = () => {
    if (globalProviders.completion) {
      globalProviders.completion.dispose();
      globalProviders.completion = null;
    }
    if (globalProviders.hover) {
      globalProviders.hover.dispose();
      globalProviders.hover = null;
    }
    if (globalProviders.signatureHelp) {
      globalProviders.signatureHelp.dispose();
      globalProviders.signatureHelp = null;
    }
    if (globalProviders.codeAction) {
      globalProviders.codeAction.dispose();
      globalProviders.codeAction = null;
    }
  };

  // Initialize Monaco and language settings (only once)
  const initializeMonaco = useCallback(() => {
    if (!monaco) return;

    // Clean up any existing providers first
    cleanupProviders();

    // Check if language already exists
    const languages = monaco.languages.getLanguages();
    const excelLangExists = languages.some((lang) => lang.id === "excel");

    if (!excelLangExists) {
      monaco.languages.register({ id: "excel" });
    }

    // Define dark theme if not exists
    try {
      monaco.editor.defineTheme("dark", {
        base: "vs-dark",
        inherit: true,
        rules: [],
        colors: {
          "editor.background": "#141414",
        },
      });
    } catch (e) {
      // Theme might already exist
    }

    monaco.languages.setLanguageConfiguration("excel", {
      brackets: [
        ["{", "}"],
        ["[", "]"],
        ["(", ")"],
        ['"', '"'],
        ["'", "'"],
      ],
      autoClosingPairs: [
        { open: "{", close: "}" },
        { open: "[", close: "]" },
        { open: "(", close: ")" },
        { open: '"', close: '"', notIn: ["string"] },
        { open: "'", close: "'", notIn: ["string"] },
      ],
      surroundingPairs: [
        { open: "{", close: "}" },
        { open: "[", close: "]" },
        { open: "(", close: ")" },
        { open: '"', close: '"' },
        { open: "'", close: "'" },
      ],
    });
  }, []);

  // Set up language tokens
  const setupLanguageTokens = useCallback((formulaKeywords) => {
    if (!monaco) return;

    monaco.languages.setMonarchTokensProvider("excel", {
      keywords: formulaKeywords,
      tokenizer: {
        root: [
          [/"([^"]*)"/, "string"],
          [/(\$?[A-Za-z]+\$?\d+)/, "variable"],
          [/(\d+\.\d+|\.\d+|\d+\.|\d+)/, "number.float"],
          [/0[xX][0-9a-fA-F]+/, "number.hex"],
          [/[-+]?(\d+)/, "number"],
          [/[-+*/%^&|=]/, "operators"],
          [/(\()(?=[^"])/, { token: "paren.lparen", next: "@push" }],
          [/(\))(?=[^"])/, { token: "paren.rparen", next: "@pop" }],
          [/[{}[\]()]/, "@brackets"],
          [/[<>]=?|==|!=/, "operators"],
          [/[;,.]/, "delimiter"],
          [
            /[A-Z_$][\w$]*/,
            {
              cases: {
                "@keywords": "keyword",
              },
            },
          ],
        ],
      },
    });
  }, []);

  // Set up providers with current data
  const setupProviders = useCallback(() => {
    if (!monaco || !isInitialized) return;

    // Clean up existing providers
    cleanupProviders();

    // Set up completion provider
    globalProviders.completion =
      monaco.languages.registerCompletionItemProvider("excel", {
        provideCompletionItems: (model, position) => {
          const wordUntilPosition = model.getWordUntilPosition(position);
          const word = wordUntilPosition?.word || "";

          const suggestions = [];

          // Add column suggestions
          allColumns.forEach((column) => {
            suggestions.push({
              label: column,
              kind: monaco.languages.CompletionItemKind.Variable,
              insertText: column,
              sortText: "0" + column,
              range: {
                startLineNumber: position.lineNumber,
                endLineNumber: position.lineNumber,
                startColumn: wordUntilPosition.startColumn,
                endColumn: wordUntilPosition.endColumn,
              },
            });
          });

          // Add formula suggestions
          localFormulaList.forEach((formula) => {
            const details = localFormulaDetails.find(
              (f) => f.value === formula
            );
            suggestions.push({
              label: formula,
              kind: monaco.languages.CompletionItemKind.Function,
              insertText: formula,
              documentation: details?.title || "",
              sortText: "1" + formula,
              range: {
                startLineNumber: position.lineNumber,
                endLineNumber: position.lineNumber,
                startColumn: wordUntilPosition.startColumn,
                endColumn: wordUntilPosition.endColumn,
              },
            });
          });

          // Filter suggestions if there's a word being typed
          if (word) {
            return {
              suggestions: suggestions.filter((item) =>
                item.label.toUpperCase().includes(word.toUpperCase())
              ),
            };
          }

          return { suggestions };
        },
        triggerCharacters: ["(", ",", " ", "="],
      });

    // Set up hover provider
    globalProviders.hover = monaco.languages.registerHoverProvider("excel", {
      provideHover: (model, position) => {
        const word = model.getWordAtPosition(position);
        if (!word) return null;

        const wordRange = new monaco.Range(
          position.lineNumber,
          word.startColumn,
          position.lineNumber,
          word.endColumn
        );

        const contents = [];

        if (localFormulaList.includes(word.word.toUpperCase())) {
          const details = localFormulaDetails.find(
            (f) => f.value === word.word.toUpperCase()
          );
          contents.push(
            { value: "**Function**: " + word.word },
            { value: details?.title || "Formula function" }
          );
        } else if (allColumns.includes(word.word.toUpperCase())) {
          contents.push(
            { value: "**Column**: " + word.word },
            { value: "This is a column in the table" }
          );
        }

        return contents.length > 0 ? { range: wordRange, contents } : null;
      },
    });

    // Set up signature help provider (shows parameter hints)
    globalProviders.signatureHelp =
      monaco.languages.registerSignatureHelpProvider("excel", {
        signatureHelpTriggerCharacters: ["(", ","],
        signatureHelpRetriggerCharacters: [","],
        provideSignatureHelp: (model, position) => {
          const text = model.getValue();
          const offset = model.getOffsetAt(position);

          const context = findFunctionContext(text, offset);
          if (!context) return null;

          const sig = FUNCTION_SIGNATURES[context.funcName];
          if (!sig) return null;

          // Build signature string
          const paramStrings = sig.params.map((p, i) => {
            const isOptional = p.startsWith("[");
            return isOptional ? p : p;
          });
          const signatureLabel = `${context.funcName}(${paramStrings.join(
            ", "
          )})`;

          // Build parameter info
          const parameters = sig.params.map((p) => ({
            label: p,
            documentation: "",
          }));

          return {
            value: {
              signatures: [
                {
                  label: signatureLabel,
                  documentation: sig.description,
                  parameters: parameters,
                },
              ],
              activeSignature: 0,
              activeParameter: Math.min(
                context.activeParameter,
                parameters.length - 1
              ),
            },
            dispose: () => {},
          };
        },
      });

    // Set up code action provider (quick fixes)
    globalProviders.codeAction = monaco.languages.registerCodeActionProvider(
      "excel",
      {
        provideCodeActions: (model, range, context) => {
          const formula = model.getValue();
          const markers = context.markers || [];

          if (markers.length === 0) return { actions: [], dispose: () => {} };

          const actions = [];

          for (const marker of markers) {
            // Quick fix for missing closing parenthesis
            if (marker.message.includes("Missing closing parenthesis")) {
              actions.push({
                title: '💡 Add missing ")"',
                kind: "quickfix",
                diagnostics: [marker],
                edit: {
                  edits: [
                    {
                      resource: model.uri,
                      textEdit: {
                        range: new monaco.Range(
                          1,
                          formula.length + 1,
                          1,
                          formula.length + 1
                        ),
                        text: ")",
                      },
                      versionId: model.getVersionId(),
                    },
                  ],
                },
                isPreferred: true,
              });
            }

            // Quick fix for unclosed string
            if (marker.message.includes("Unclosed string")) {
              actions.push({
                title: "💡 Add missing '\"'",
                kind: "quickfix",
                diagnostics: [marker],
                edit: {
                  edits: [
                    {
                      resource: model.uri,
                      textEdit: {
                        range: new monaco.Range(
                          1,
                          formula.length + 1,
                          1,
                          formula.length + 1
                        ),
                        text: '"',
                      },
                      versionId: model.getVersionId(),
                    },
                  ],
                },
                isPreferred: true,
              });
            }
          }

          return {
            actions,
            dispose: () => {},
          };
        },
      }
    );
  }, [allColumns, localFormulaList, localFormulaDetails, isInitialized]);

  // Initialize editor
  const initializeEditor = useCallback(async () => {
    if (!editorRef.current || editor) return;

    // Initialize Monaco settings
    initializeMonaco();

    // Get formulas (from props or API)
    const data = await getFormulas();
    setLocalFormulaList(data.formula_list || []);
    setLocalFormulaDetails(data.formulas || []);

    // Set up language tokens
    setupLanguageTokens(data.formula_list || []);

    // Create editor instance
    const newEditor = monaco.editor.create(editorRef.current, {
      value: value || "",
      language: "excel",
      theme: getCurrentTheme(),
      automaticLayout: true,
      glyphMargin: false,
      folding: false,
      renderLineHighlight: "none",
      lineNumbers: "off",
      minimap: { enabled: false },
      wordWrap: "on",
      scrollBeyondLastLine: false,
    });

    // Store reference to monaco instance
    monacoRef.current = monaco;

    // Set up event listeners
    const contentChangeDisposable = newEditor.onDidChangeModelContent(() => {
      if (!isUpdatingRef.current) {
        const currentValue = newEditor.getValue();
        setValue(currentValue);
      }
    });

    const keyDownDisposable = newEditor.onKeyDown((event) => {
      if (event.keyCode === monaco.KeyCode.Enter) {
        const suggestController = newEditor.getContribution(
          "editor.contrib.suggestController"
        );
        const suggestModel = suggestController?.model;
        if (suggestModel && suggestModel.state === 0) {
          event.stopPropagation();
        }
      }
    });

    // Store disposables on editor instance for cleanup
    newEditor._disposables = [contentChangeDisposable, keyDownDisposable];

    setEditor(newEditor);
    setIsInitialized(true);
  }, [
    value,
    getCurrentTheme,
    setValue,
    initializeMonaco,
    setupLanguageTokens,
    editor,
  ]);

  // Initialize editor on mount
  useEffect(() => {
    initializeEditor();

    return () => {
      cleanupProviders();
      if (editor) {
        editor._disposables?.forEach((d) => d.dispose());
        editor.dispose();
      }
      setEditor(null);
      setIsInitialized(false);
    };
  }, []); // Empty deps - only run on mount/unmount

  // Update providers when data changes
  useEffect(() => {
    if (isInitialized) {
      setupProviders();
    }
  }, [setupProviders, isInitialized]);

  // Update local data when props change
  useEffect(() => {
    if (formulaList && formulaDetails && isInitialized) {
      setLocalFormulaList(formulaList);
      setLocalFormulaDetails(formulaDetails);
      // Re-setup providers with new data
      setupProviders();
    }
  }, [formulaList, formulaDetails, isInitialized, setupProviders]);

  // Handle clear and populate
  useEffect(() => {
    if (!editor) return;

    // Handle clear
    if (clear && !lastClearRef.current) {
      isUpdatingRef.current = true;
      editor.setValue("");
      setTimeout(() => {
        isUpdatingRef.current = false;
      }, 0);
    }

    // Handle populate (for edit functionality)
    if (populate && !lastPopulateRef.current && value) {
      isUpdatingRef.current = true;
      editor.setValue(value);

      // Focus and move cursor to end
      editor.focus();
      const model = editor.getModel();
      const lastLine = model.getLineCount();
      const lastColumn = model.getLineLength(lastLine) + 1;
      editor.setPosition({ lineNumber: lastLine, column: lastColumn });

      setTimeout(() => {
        isUpdatingRef.current = false;
      }, 100); // Give it a bit more time to settle
    }

    // Update refs
    lastClearRef.current = clear;
    lastPopulateRef.current = populate;
  }, [clear, populate, value, editor]);

  // Handle external value changes (but not during clear/populate operations)
  useEffect(() => {
    if (editor && !isUpdatingRef.current && !clear && !populate) {
      const currentValue = editor.getValue();
      if (currentValue !== value) {
        isUpdatingRef.current = true;
        editor.setValue(value || "");
        setTimeout(() => {
          isUpdatingRef.current = false;
        }, 0);
      }
    }
  }, [value, editor, clear, populate]);

  // Handle theme changes
  useEffect(() => {
    if (editor && monacoRef.current) {
      monacoRef.current.editor.setTheme(getCurrentTheme());
    }
  }, [userDetails.currentTheme, editor, getCurrentTheme]);

  return (
    <div
      ref={editorRef}
      className="formula-editor"
      style={{ minHeight: "100px" }}
    />
  );
}

FormulaEditor.propTypes = {
  allColumns: PropTypes.arrayOf(PropTypes.string).isRequired,
  value: PropTypes.string.isRequired,
  setValue: PropTypes.func.isRequired,
  clear: PropTypes.bool.isRequired,
  populate: PropTypes.bool.isRequired,
  formulaList: PropTypes.arrayOf(PropTypes.string),
  formulaDetails: PropTypes.arrayOf(PropTypes.object),
  onValidationChange: PropTypes.func, // Callback: ({ isValid: boolean, errors: array }) => void
};

export { FormulaEditor };
