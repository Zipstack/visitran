import { useEffect, useState, useCallback } from "react";
import {
  Input,
  Button,
  Space,
  Typography,
  Table,
  Empty,
  Tooltip,
  Tabs,
  Popconfirm,
  Alert,
  Select,
  Tag,
} from "antd";
import {
  EditOutlined,
  SearchOutlined,
  DeleteOutlined,
  QuestionCircleOutlined,
  WarningOutlined,
  PlusOutlined,
  CloseOutlined,
} from "@ant-design/icons";
import PropTypes from "prop-types";
import isEqual from "lodash/isEqual.js";

import { ToolbarItem } from "../toolbar-item.jsx";
import {
  addIdToObjects,
  generateKey,
  removeIdFromObjects,
  stopPropagation,
  useEscapeKey,
} from "../../../../common/helpers.js";
import { FormulaEditor } from "../formula-editor.jsx";
import { orgStore } from "../../../../store/org-store.js";
import { useAxiosPrivate } from "../../../../service/axios-service.js";
import "./add-column.css";
import {
  AddColumnRightDarkIcon,
  AddColumnRightLightIcon,
} from "../../../../base/icons/index.js";
import { useTransformIdStore } from "../../../../store/transform-id-store.js";
import {
  getGroupAndAggregationSpec,
  getSynthesizeSpec,
  getWindowSpec,
  transformationTypes,
} from "../../no-code-model/helper.js";

// Internal reusable component for column form
const ColumnForm = ({
  columnName,
  setColumnName,
  formula,
  setFormula,
  dupeColCheck,
  warning,
  clearFormula,
  populateFormula,
  allColumns,
  isLoading,
  onCancel,
  onSave,
  submitButtonText = "Save",
  disabled = false,
  setTabKey,
  formulaList,
  formulaListData,
}) => {
  // Validation state
  const [formulaValidation, setFormulaValidation] = useState({
    isValid: true,
    errors: [],
  });

  // Handle column name input - remove spaces
  const handleColumnNameInput = (value) => {
    value = value.replace(/\s/g, "");
    setColumnName(value);
  };

  // Input status for visual feedback
  const inputStatus = (dupe) => {
    return dupe ? "error" : "";
  };

  // Handle validation changes from FormulaEditor
  const handleValidationChange = useCallback((validation) => {
    setFormulaValidation(validation);
  }, []);

  // Trim formula whenever it changes
  useEffect(() => {
    setFormula((prevFormula) => prevFormula?.trim() || "");
  }, [formula]);

  // Reset validation when formula is cleared
  useEffect(() => {
    if (!formula) {
      setFormulaValidation({ isValid: true, errors: [] });
    }
  }, [formula]);

  // Check if save should be disabled (validation errors don't block save - user can bypass)
  const isSaveDisabled = isLoading || !columnName || !formula || disabled;

  return (
    <div>
      <Typography className="margin-block-10">
        * {submitButtonText === "Save" ? "New" : "Update"} Column Name
      </Typography>
      <Input
        value={columnName}
        onChange={(e) => handleColumnNameInput(e.target.value)}
        onKeyDown={stopPropagation}
        status={inputStatus(dupeColCheck)}
      />
      {dupeColCheck && (
        <Typography.Text type="danger">
          Column name already exists
        </Typography.Text>
      )}

      <div className="margin-block-10">
        Formula
        <QuestionCircleOutlined
          onClick={() => setTabKey("help")}
          className="helpInfo"
        />
      </div>

      <FormulaEditor
        allColumns={allColumns}
        value={formula}
        setValue={setFormula}
        clear={clearFormula}
        populate={populateFormula}
        formulaList={formulaList}
        formulaDetails={formulaListData}
        onValidationChange={handleValidationChange}
      />

      <Typography className="helper-text">
        Use standard Excel-like formulas (e.g., SUM, AVERAGE, COUNT)
      </Typography>

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
          style={{ marginTop: "8px", padding: "4px 8px" }}
        />
      )}

      {warning && (
        <Typography.Text type="danger">
          You cannot use newly created columns from aggregate
        </Typography.Text>
      )}

      <Space className="flex-end-container mt-16 mb-16">
        <Button onClick={onCancel}>Cancel</Button>
        <Tooltip
          title={
            !formulaValidation.isValid ? "Fix formula errors before saving" : ""
          }
        >
          <Button
            className="ml-10"
            onClick={onSave}
            disabled={isSaveDisabled}
            type="primary"
            loading={isLoading}
          >
            {submitButtonText}
          </Button>
        </Tooltip>
      </Space>
    </div>
  );
};

ColumnForm.propTypes = {
  columnName: PropTypes.string.isRequired,
  setColumnName: PropTypes.func.isRequired,
  formula: PropTypes.string.isRequired,
  setFormula: PropTypes.func.isRequired,
  dupeColCheck: PropTypes.bool.isRequired,
  warning: PropTypes.bool.isRequired,
  clearFormula: PropTypes.bool.isRequired,
  populateFormula: PropTypes.bool.isRequired,
  allColumns: PropTypes.arrayOf(PropTypes.string).isRequired,
  isLoading: PropTypes.bool.isRequired,
  onCancel: PropTypes.func.isRequired,
  onSave: PropTypes.func.isRequired,
  submitButtonText: PropTypes.string,
  disabled: PropTypes.bool,
  setTabKey: PropTypes.func.isRequired,
  formulaList: PropTypes.arrayOf(PropTypes.string),
  formulaListData: PropTypes.arrayOf(PropTypes.object),
};

const FormulaHelp = ({
  handleSearchFormula,
  filteredformulaList,
  desc,
  handleClick,
}) => {
  return (
    <div className="width_100_percent">
      <Input
        placeholder="Search"
        onChange={handleSearchFormula}
        prefix={<SearchOutlined className="search-icon" />}
        className="m-0 mb-10"
        allowClear
        onKeyDown={stopPropagation}
      />
      <div className="formulaListWrapper">
        {filteredformulaList.map((item) => (
          <Button
            type="text"
            key={item}
            style={{
              backgroundColor:
                desc.value === item ? "var(--border-color-3)" : "",
            }}
            className="formulaListItem"
            onClick={(e) => handleClick(e, item)}
          >
            {item}
          </Button>
        ))}
      </div>
      <div className="mb-10">Description</div>
      <div className="descriptionBox">
        <Typography>{desc.value}</Typography>
        {desc.title.split("\n").map((item) => (
          <div key={item} className="p-2">
            {item}
          </div>
        ))}
      </div>
    </div>
  );
};
FormulaHelp.propTypes = {
  handleSearchFormula: PropTypes.func.isRequired,
  filteredformulaList: PropTypes.array.isRequired,
  desc: PropTypes.object.isRequired,
  handleClick: PropTypes.func.isRequired,
};

// Window function options grouped by category
const WINDOW_FUNCTIONS = [
  // Ranking functions (no agg column needed)
  {
    value: "ROW_NUMBER",
    label: "ROW_NUMBER",
    description: "Sequential row numbers (0, 1, 2, ...)",
    category: "Ranking",
  },
  {
    value: "RANK",
    label: "RANK",
    description: "Rank with gaps for ties (1, 2, 2, 4)",
    category: "Ranking",
  },
  {
    value: "DENSE_RANK",
    label: "DENSE_RANK",
    description: "Rank without gaps (1, 2, 2, 3)",
    category: "Ranking",
  },
  {
    value: "PERCENT_RANK",
    label: "PERCENT_RANK",
    description: "Relative rank as percentage (0 to 1)",
    category: "Ranking",
  },
  // Navigation functions (uses first order column)
  {
    value: "LAG",
    label: "LAG",
    description: "Previous row value",
    category: "Navigation",
  },
  {
    value: "LEAD",
    label: "LEAD",
    description: "Next row value",
    category: "Navigation",
  },
  // Aggregate functions (requires agg column)
  {
    value: "SUM",
    label: "SUM (Window)",
    description: "Running/cumulative sum",
    category: "Aggregate",
    needsAggColumn: true,
  },
  {
    value: "AVG",
    label: "AVG (Window)",
    description: "Moving average",
    category: "Aggregate",
    needsAggColumn: true,
  },
  {
    value: "COUNT",
    label: "COUNT (Window)",
    description: "Running count",
    category: "Aggregate",
    needsAggColumn: true,
  },
  {
    value: "MIN",
    label: "MIN (Window)",
    description: "Running minimum",
    category: "Aggregate",
    needsAggColumn: true,
  },
  {
    value: "MAX",
    label: "MAX (Window)",
    description: "Running maximum",
    category: "Aggregate",
    needsAggColumn: true,
  },
];

// Helper to check if function needs agg column
const needsAggColumn = (funcName) => {
  const func = WINDOW_FUNCTIONS.find((f) => f.value === funcName);
  return func?.needsAggColumn || false;
};

// Frame options for window functions
const FRAME_PRECEDING_OPTIONS = [
  { value: "unbounded", label: "UNBOUNDED" },
  { value: 0, label: "0 (Current Row)" },
  { value: 1, label: "1" },
  { value: 2, label: "2" },
  { value: 3, label: "3" },
  { value: 5, label: "5" },
  { value: 7, label: "7" },
  { value: 10, label: "10" },
];

const FRAME_FOLLOWING_OPTIONS = [
  { value: 0, label: "CURRENT ROW" },
  { value: 1, label: "1" },
  { value: 2, label: "2" },
  { value: 3, label: "3" },
  { value: "unbounded", label: "UNBOUNDED" },
];

// Window Function Form component
const WindowFunctionForm = ({
  columnName,
  setColumnName,
  windowFunction,
  setWindowFunction,
  aggColumn,
  setAggColumn,
  partitionBy,
  setPartitionBy,
  orderBy,
  setOrderBy,
  preceding,
  setPreceding,
  following,
  setFollowing,
  allColumns,
  isLoading,
  onCancel,
  onSave,
  dupeColCheck,
  submitButtonText = "Save",
  disabled = false,
}) => {
  // Handle column name input - remove spaces
  const handleColumnNameInput = (value) => {
    value = value.replace(/\s/g, "");
    setColumnName(value);
  };

  // Add a new order by entry
  const handleAddOrderBy = () => {
    setOrderBy([...orderBy, { column: "", direction: "ASC" }]);
  };

  // Remove an order by entry
  const handleRemoveOrderBy = (index) => {
    const newOrderBy = orderBy.filter((_, i) => i !== index);
    setOrderBy(newOrderBy);
  };

  // Update order by column
  const handleOrderByColumnChange = (index, column) => {
    const newOrderBy = [...orderBy];
    newOrderBy[index] = { ...newOrderBy[index], column };
    setOrderBy(newOrderBy);
  };

  // Update order by direction
  const handleOrderByDirectionChange = (index, direction) => {
    const newOrderBy = [...orderBy];
    newOrderBy[index] = { ...newOrderBy[index], direction };
    setOrderBy(newOrderBy);
  };

  // Check if this function needs an aggregation column
  const showAggColumn = needsAggColumn(windowFunction);

  // Check if save should be disabled
  const isSaveDisabled =
    isLoading ||
    !columnName ||
    !windowFunction ||
    orderBy.length === 0 ||
    orderBy.some((o) => !o.column) ||
    (showAggColumn && !aggColumn) ||
    disabled;

  return (
    <div className="window-function-form">
      <Typography className="margin-block-10">* Column Name</Typography>
      <Input
        value={columnName}
        onChange={(e) => handleColumnNameInput(e.target.value)}
        onKeyDown={stopPropagation}
        status={dupeColCheck ? "error" : ""}
        placeholder="Enter column name"
      />
      {dupeColCheck && (
        <Typography.Text type="danger">
          Column name already exists
        </Typography.Text>
      )}

      <Typography className="margin-block-10">* Function</Typography>
      <Select
        value={windowFunction}
        onChange={(val) => {
          setWindowFunction(val);
          // Clear agg column if switching to non-aggregate function
          if (!needsAggColumn(val)) {
            setAggColumn("");
          }
        }}
        placeholder="Select window function"
        style={{ width: "100%" }}
        options={WINDOW_FUNCTIONS.map((f) => ({
          value: f.value,
          label: (
            <div>
              <strong>{f.label}</strong>
              <span style={{ marginLeft: 8, color: "gray", fontSize: 12 }}>
                {f.description}
              </span>
            </div>
          ),
        }))}
      />

      {showAggColumn && (
        <>
          <Typography className="margin-block-10">
            * Aggregation Column
          </Typography>
          <Select
            value={aggColumn || undefined}
            onChange={setAggColumn}
            placeholder="Select column to aggregate"
            style={{ width: "100%" }}
            options={allColumns.map((col) => ({ value: col, label: col }))}
          />
          <Typography className="helper-text">
            The column to apply {windowFunction} on
          </Typography>
        </>
      )}

      <Typography className="margin-block-10">
        Partition By (optional)
      </Typography>
      <Select
        mode="multiple"
        value={partitionBy}
        onChange={setPartitionBy}
        placeholder="Select columns to partition by"
        style={{ width: "100%" }}
        options={allColumns.map((col) => ({ value: col, label: col }))}
        tagRender={({ label, onClose }) => (
          <Tag closable onClose={onClose} style={{ marginRight: 3 }}>
            {label}
          </Tag>
        )}
      />
      <Typography className="helper-text">
        Group rows before applying the window function
      </Typography>

      <Typography className="margin-block-10">* Order By</Typography>
      <div className="order-by-list">
        {orderBy.map((item, index) => (
          <div key={index} className="order-by-item">
            <Select
              value={item.column || undefined}
              onChange={(val) => handleOrderByColumnChange(index, val)}
              placeholder="Column"
              style={{ flex: 2 }}
              options={allColumns.map((col) => ({ value: col, label: col }))}
            />
            <Select
              value={item.direction}
              onChange={(val) => handleOrderByDirectionChange(index, val)}
              style={{ flex: 1 }}
              options={[
                { value: "ASC", label: "ASC" },
                { value: "DESC", label: "DESC" },
              ]}
            />
            {orderBy.length > 1 && (
              <Button
                type="text"
                icon={<CloseOutlined />}
                onClick={() => handleRemoveOrderBy(index)}
                danger
              />
            )}
          </div>
        ))}
        <Button
          type="dashed"
          onClick={handleAddOrderBy}
          icon={<PlusOutlined />}
          style={{ width: "100%", marginTop: 8 }}
        >
          Add Order Column
        </Button>
      </div>

      {/* Frame Specification - for aggregate window functions */}
      {showAggColumn && (
        <>
          <Typography className="margin-block-10">
            Frame (optional)
            <Tooltip title="Define the window frame for rolling/moving calculations. E.g., '2 PRECEDING to CURRENT ROW' for 3-row moving average.">
              <QuestionCircleOutlined
                style={{ marginLeft: 6, color: "gray" }}
              />
            </Tooltip>
          </Typography>
          <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
            <span style={{ whiteSpace: "nowrap" }}>ROWS BETWEEN</span>
            <Select
              value={preceding}
              onChange={setPreceding}
              placeholder="Preceding"
              style={{ flex: 1 }}
              allowClear
              options={FRAME_PRECEDING_OPTIONS}
            />
            <span>AND</span>
            <Select
              value={following}
              onChange={setFollowing}
              placeholder="Following"
              style={{ flex: 1 }}
              allowClear
              options={FRAME_FOLLOWING_OPTIONS}
            />
          </div>
          <Typography className="helper-text">
            Leave empty for unbounded frame (default cumulative behavior)
          </Typography>
        </>
      )}

      <Space className="flex-end-container mt-16 mb-16">
        <Button onClick={onCancel}>Cancel</Button>
        <Button
          className="ml-10"
          onClick={onSave}
          disabled={isSaveDisabled}
          type="primary"
          loading={isLoading}
        >
          {submitButtonText}
        </Button>
      </Space>
    </div>
  );
};

WindowFunctionForm.propTypes = {
  columnName: PropTypes.string.isRequired,
  setColumnName: PropTypes.func.isRequired,
  windowFunction: PropTypes.string,
  setWindowFunction: PropTypes.func.isRequired,
  aggColumn: PropTypes.string,
  setAggColumn: PropTypes.func.isRequired,
  partitionBy: PropTypes.arrayOf(PropTypes.string).isRequired,
  setPartitionBy: PropTypes.func.isRequired,
  orderBy: PropTypes.arrayOf(
    PropTypes.shape({
      column: PropTypes.string,
      direction: PropTypes.string,
    })
  ).isRequired,
  setOrderBy: PropTypes.func.isRequired,
  preceding: PropTypes.oneOfType([PropTypes.number, PropTypes.string]),
  setPreceding: PropTypes.func.isRequired,
  following: PropTypes.oneOfType([PropTypes.number, PropTypes.string]),
  setFollowing: PropTypes.func.isRequired,
  allColumns: PropTypes.arrayOf(PropTypes.string).isRequired,
  isLoading: PropTypes.bool.isRequired,
  onCancel: PropTypes.func.isRequired,
  onSave: PropTypes.func.isRequired,
  dupeColCheck: PropTypes.bool.isRequired,
  submitButtonText: PropTypes.string,
  disabled: PropTypes.bool,
};

// Main AddColumn component
function AddColumn({
  allColumns,
  spec,
  updateSpec,
  isLoading,
  disabled,
  step,
  openFormula,
  setOpenFormula,
  selectedFormulaCol,
  setSelectedFormulaCol,
  synthesizeValidationCols,
  isDarkTheme,
  saveTransformation,
  handleDeleteTransformation,
  handleGetColumns,
}) {
  const axiosPrivate = useAxiosPrivate();
  const { selectedOrgId } = orgStore();
  const [addedColumns, setAddedColumns] = useState([]);
  const [label, setLabel] = useState("Add Column");
  const [addedColumnCount, setAddedColumnCount] = useState(0);
  const [isModified, setIsModified] = useState(false);
  const [currentColumn, setCurrentColumn] = useState("");
  const [currentFormula, setCurrentFormula] = useState("");
  const [clearFormula, setClearFormula] = useState(false);
  const [populateFormula, setPopulateFormula] = useState(false);
  const [populateFormulaForEdit, setPopulateFormulaForEdit] = useState(
    !populateFormula
  );
  const [currentEditCol, setCurrentEditCol] = useState("");
  const [currentEditFormula, setCurrentEditFormula] = useState("");
  const [tabKey, setTabKey] = useState("add");
  const [formulaList, setFormulaList] = useState([]);
  const [filteredformulaList, setFilteredformulaList] = useState([]);
  const [warning, setWarning] = useState(false);
  const [formulaListData, setFormulaListData] = useState([]);
  const [notSelectedCols, setNotSelectedCols] = useState([]);
  const [dupeColCheck, setDupeColCheck] = useState(false);
  const [desc, setDesc] = useState({ title: "", value: "" });
  const [selectedCol, setSelectedCol] = useState("");
  const [aggCols, setAggCols] = useState([]);
  const { transformIds } = useTransformIdStore();

  // Window function state
  const [windowColumnName, setWindowColumnName] = useState("");
  const [windowFunction, setWindowFunction] = useState("");
  const [aggColumn, setAggColumn] = useState("");
  const [partitionBy, setPartitionBy] = useState([]);
  const [orderBy, setOrderBy] = useState([{ column: "", direction: "ASC" }]);
  const [preceding, setPreceding] = useState(undefined);
  const [following, setFollowing] = useState(undefined);
  const [windowDupeColCheck, setWindowDupeColCheck] = useState(false);
  const [editingWindowCol, setEditingWindowCol] = useState(null); // Track window column being edited

  const tableColumns = [
    {
      title: "Column",
      dataIndex: "column_name",
      key: "column_name",
      width: 130,
      ellipsis: true,
      render: (column_name, record, index) => (
        <span
          style={{ cursor: "pointer" }}
          onClick={() => {
            if (record.type === "WINDOW") {
              populateWindowOnEdit(index);
            } else {
              populateOnEdit(index, column_name);
              setPopulateFormulaForEdit(!populateFormulaForEdit);
            }
          }}
        >
          {column_name}
        </span>
      ),
    },
    {
      title: "Formula/Function",
      key: "formula",
      width: 200,
      render: ({ type, operation }) => {
        if (type === "WINDOW") {
          const aggCol = operation?.agg_column
            ? `(${operation.agg_column})`
            : "()";
          const partitions = operation?.partition_by?.length
            ? ` PARTITION BY ${operation.partition_by.join(", ")}`
            : "";
          const orders = operation?.order_by
            ?.map((o) => `${o.column} ${o.direction}`)
            .join(", ");
          return `${operation?.function || ""}${aggCol}${partitions} ORDER BY ${
            orders || ""
          }`;
        }
        return operation?.formula || "";
      },
      ellipsis: true,
    },
    {
      title: "Action",
      key: "action",
      width: 100,
      render: (record, { column_name }, index) => (
        <Space>
          <div className="action">
            <Tooltip title="Edit" key="edit">
              <EditOutlined
                onClick={() => {
                  if (record.type === "WINDOW") {
                    populateWindowOnEdit(index);
                  } else {
                    populateOnEdit(index, column_name);
                    setPopulateFormulaForEdit(!populateFormulaForEdit);
                  }
                }}
              />
            </Tooltip>
            <Popconfirm
              title="Delete Column"
              description="Are you sure you want to delete this column?"
              okText="Delete"
              okButtonProps={{ danger: true }}
              onConfirm={() => handleRemove(index, column_name)}
            >
              <Tooltip title="Delete" key="delete">
                <DeleteOutlined className="red" />
              </Tooltip>
            </Popconfirm>
          </div>
        </Space>
      ),
    },
  ];

  const updateLabel = (newAddedColumnCount) => {
    const plural = newAddedColumnCount > 1 ? "s" : "";
    const newLabel = newAddedColumnCount
      ? `Added ${newAddedColumnCount} column${plural}`
      : "Add Column";
    setLabel(newLabel);
    setAddedColumnCount(newAddedColumnCount);
  };

  const clearData = () => {
    setCurrentColumn("");
    setSelectedCol("");
    setCurrentFormula("");
    setClearFormula(!clearFormula);
    setDupeColCheck(false);
    setSelectedFormulaCol({ column: "", formula: "" });
  };

  const clearWindowData = () => {
    setWindowColumnName("");
    setWindowFunction("");
    setAggColumn("");
    setPartitionBy([]);
    setOrderBy([{ column: "", direction: "ASC" }]);
    setPreceding(undefined);
    setFollowing(undefined);
    setWindowDupeColCheck(false);
    setEditingWindowCol(null);
  };

  // Populate window form for editing
  const populateWindowOnEdit = (index) => {
    const col = addedColumns[index];
    if (col?.type !== "WINDOW") return;

    setWindowColumnName(col.column_name);
    setWindowFunction(col.operation.function || "");
    setAggColumn(col.operation.agg_column || "");
    setPartitionBy(col.operation.partition_by || []);
    setOrderBy(
      col.operation.order_by?.length > 0
        ? col.operation.order_by
        : [{ column: "", direction: "ASC" }]
    );
    setPreceding(col.operation.preceding);
    setFollowing(col.operation.following);
    setEditingWindowCol(col.column_name);
    setTabKey("window");
  };

  const checkWindowDupe = (colName, skipCol = null) => {
    try {
      for (const item of addedColumns || []) {
        // Skip the column being edited
        if (skipCol && item.column_name === skipCol) continue;
        if (item.column_name === colName) {
          return true;
        }
      }
      for (const col of synthesizeValidationCols || []) {
        if (col === colName) {
          return true;
        }
      }
      return false;
    } catch {
      return false;
    }
  };

  const check = (cols = addedColumns) => {
    try {
      for (const item of cols || []) {
        if (item.column_name === currentColumn) {
          return true;
        }
      }
      for (const col of synthesizeValidationCols || []) {
        if (col === currentColumn) {
          return true;
        }
      }
      return false;
    } catch {
      return false;
    }
  };

  const handleAdd = (event) => {
    event.stopPropagation();
    const checKDupe = check();
    setDupeColCheck(checKDupe);
    if (!checKDupe) {
      if (![currentColumn, currentFormula].includes("")) {
        handleSave([
          ...addedColumns,
          {
            id: generateKey(),
            column_name: currentColumn,
            type: "FORMULA",
            operation: { formula: currentFormula },
          },
        ]);
      }
      clearData();
    }
  };

  const handleAddWindowColumn = (event) => {
    event.stopPropagation();
    // Check for duplicate, skip the column being edited
    const isDupe = checkWindowDupe(windowColumnName, editingWindowCol);
    setWindowDupeColCheck(isDupe);
    if (!isDupe && windowColumnName && windowFunction && orderBy.length > 0) {
      const operation = {
        function: windowFunction,
        partition_by: partitionBy,
        order_by: orderBy.filter((o) => o.column), // Remove empty entries
      };
      // Add agg_column if this is an aggregate function
      if (needsAggColumn(windowFunction) && aggColumn) {
        operation.agg_column = aggColumn;
      }
      // Add frame specification if provided
      if (preceding !== undefined) {
        operation.preceding = preceding;
      }
      if (following !== undefined) {
        operation.following = following;
      }

      let newColumns;
      if (editingWindowCol) {
        // Update existing window column
        newColumns = addedColumns.map((col) =>
          col.column_name === editingWindowCol
            ? { ...col, column_name: windowColumnName, operation }
            : col
        );
      } else {
        // Add new window column
        newColumns = [
          ...addedColumns,
          {
            id: generateKey(),
            column_name: windowColumnName,
            type: "WINDOW",
            operation,
          },
        ];
      }
      handleSave(newColumns);
      clearWindowData();
    }
  };

  const populateOnEdit = (index, colName) => {
    setSelectedCol(colName);
    setCurrentEditCol(addedColumns[index].column_name);
    setCurrentEditFormula(addedColumns[index].operation.formula || "");
    setSelectedFormulaCol({
      column: addedColumns[index].column_name,
      formula: addedColumns[index].operation.formula || "",
    });
    setPopulateFormulaForEdit((prev) => !prev);
    const filtered = addedColumns.filter((el) => el.column_name !== colName);
    setNotSelectedCols(filtered);
    setTabKey("edit"); // Switch to Edit tab
  };

  const handleUpdate = () => {
    const checKDupe = check(notSelectedCols);
    setDupeColCheck(checKDupe);
    if (!checKDupe) {
      const newAddedColumns = [...addedColumns].map((item) => {
        if (item.column_name === selectedCol) {
          return {
            ...item,
            operation: { formula: currentEditFormula },
            column_name: currentEditCol,
          };
        }
        return item;
      });

      handleSave(newAddedColumns);
      setPopulateFormulaForEdit(!populateFormulaForEdit);
    }
  };

  const handleRemove = (index, colname) => {
    const removedCol = addedColumns[index];
    if (removedCol.column_name === currentColumn) {
      clearData();
    }
    const newAddedColumns = [...addedColumns].filter((el) => {
      return el.column_name !== colname;
    });
    handleSave(newAddedColumns);
    setCurrentColumn("");
    setSelectedCol("");
    setClearFormula(!clearFormula);
    setDupeColCheck(false);
  };

  const handleSave = async (cols) => {
    // Separate formula columns and window columns
    const formulaCols = cols.filter((col) => col.type !== "WINDOW");
    const windowCols = cols.filter((col) => col.type === "WINDOW");

    // Save formula columns to synthesize transform
    const synthesizeSpec = {
      ...getSynthesizeSpec(spec?.transform, transformIds?.SYNTHESIZE),
    };
    synthesizeSpec["columns"] = removeIdFromObjects(formulaCols);

    // Save window columns to window transform
    const windowSpec = {
      ...getWindowSpec(spec?.transform, transformIds?.WINDOW),
    };
    windowSpec["columns"] = removeIdFromObjects(windowCols);

    let result = {};

    // Handle synthesize transform
    if (synthesizeSpec?.columns?.length === 0 && transformIds?.SYNTHESIZE) {
      const body = {
        step_id: transformIds?.SYNTHESIZE,
      };
      result = await handleDeleteTransformation(body);
    } else if (synthesizeSpec?.columns?.length > 0) {
      const body = {
        type: "synthesize",
        synthesize: synthesizeSpec,
      };
      result = await saveTransformation(body, transformIds?.SYNTHESIZE);
    }

    // Handle window transform
    if (windowSpec?.columns?.length === 0 && transformIds?.WINDOW) {
      const body = {
        step_id: transformIds?.WINDOW,
      };
      const windowResult = await handleDeleteTransformation(body);
      // Use window result if no synthesize operation was done
      if (!result?.status) {
        result = windowResult;
      }
    } else if (windowSpec?.columns?.length > 0) {
      const body = {
        type: "window",
        window: windowSpec,
      };
      const windowResult = await saveTransformation(body, transformIds?.WINDOW);
      // Use window result if no synthesize operation was done
      if (!result?.status) {
        result = windowResult;
      } else if (windowResult?.status === "success") {
        // Update spec from window result if both succeeded
        result.spec = windowResult.spec;
      }
    }

    if (result?.status === "success") {
      updateSpec(result?.spec);
      updateLabel(cols?.length);
      setOpenFormula(false);
      clearData();
      setTabKey("add");
    } else {
      setOpenFormula(true);
    }
  };

  const handleOpenChange = (value) => {
    if (value) {
      handleGetColumns(
        transformIds?.SYNTHESIZE,
        transformationTypes?.SYNTHESIZE
      );
    }
    setOpenFormula(value);
  };

  const handleCancelBtn = () => {
    clearData();
    clearWindowData();
    setPopulateFormulaForEdit(!populateFormulaForEdit);
    setTabKey("add");
    // Get formula columns from synthesize transform
    const synthesizeSpec = getSynthesizeSpec(
      spec?.transform,
      transformIds?.SYNTHESIZE
    );
    // Get window columns from window transform
    const windowSpec = getWindowSpec(spec?.transform, transformIds?.WINDOW);
    // Convert window columns to have type: "WINDOW"
    const windowColumns = (windowSpec?.columns || []).map((col) => ({
      ...col,
      type: "WINDOW",
    }));
    // Combine formula and window columns
    const allColumns = [...(synthesizeSpec?.columns || []), ...windowColumns];
    updateLabel(allColumns.length);
    setAddedColumns(allColumns);
    setOpenFormula(false);
  };

  const getFormulas = () => {
    const requestOptions = {
      method: "GET",
      url: `/api/v1/visitran/${selectedOrgId || "default_org"}/formulas`,
    };
    axiosPrivate(requestOptions)
      .then((res) => {
        setFormulaList(res?.data?.formula_list);
        setFilteredformulaList(res?.data?.formula_list);
        setFormulaListData(res?.data?.formulas);
      })
      .catch((error) => {
        console.error("Error fetching keywords:", error);
      });
  };

  const getDescription = (formula) => {
    for (const i of formulaListData) {
      if (i.value === formula) {
        const { title, value } = i;
        setDesc({ title, value });
        return;
      }
    }
  };

  const handleSearchFormula = (e) => {
    const text = e.target.value;
    if (text.length === 0) {
      setFilteredformulaList(formulaList);
    } else {
      const filtered = formulaList.filter((el) => {
        return el.includes(text.toUpperCase());
      });
      setFilteredformulaList(filtered);
    }
  };

  const handleClick = (e, value) => {
    switch (e.detail) {
      case 1:
        getDescription(value);
        break;
      case 2:
        setCurrentFormula(value);
        setPopulateFormula(!populateFormula);
        break;
    }
  };

  function checkArrayValuesInString(array, string) {
    return array.some((value) => string.includes(value));
  }

  const countCol = (count) => {
    return count ? "no-code-toolbar-added-cols-highlights" : "";
  };

  const onChange = (key) => {
    setTabKey(key);
  };

  // Effects
  useEscapeKey(openFormula, () => setOpenFormula(false));

  useEffect(() => {
    // Get formula columns from synthesize transform
    const synthesizeSpec = getSynthesizeSpec(
      spec?.transform,
      transformIds?.SYNTHESIZE
    );
    // Get window columns from window transform
    const windowSpec = getWindowSpec(spec?.transform, transformIds?.WINDOW);
    // Convert window columns to have type: "WINDOW" for consistent handling
    const windowColumns = (windowSpec?.columns || []).map((col) => ({
      ...col,
      type: "WINDOW",
    }));
    // Combine formula and window columns
    const newAddedColumns = addIdToObjects([
      ...(synthesizeSpec?.columns || []),
      ...windowColumns,
    ]);
    setAddedColumns(newAddedColumns);
    updateLabel(newAddedColumns?.length);
  }, [spec?.transform, transformIds?.SYNTHESIZE, transformIds?.WINDOW]);

  useEffect(() => {
    if (selectedFormulaCol?.formula && selectedFormulaCol.column) {
      setIsModified(
        !isEqual(currentEditFormula, selectedFormulaCol?.formula) ||
          !isEqual(currentEditCol, selectedFormulaCol?.column)
      );
    }
  }, [selectedFormulaCol, currentEditCol, currentEditFormula]);
  useEffect(() => {
    if (!openFormula) {
      clearData();
      clearWindowData();
      // Get formula columns from synthesize transform
      const synthesizeSpec = getSynthesizeSpec(
        spec?.transform,
        transformIds?.SYNTHESIZE
      );
      // Get window columns from window transform
      const windowSpec = getWindowSpec(spec?.transform, transformIds?.WINDOW);
      // Convert window columns to have type: "WINDOW" for consistent handling
      const windowColumns = (windowSpec?.columns || []).map((col) => ({
        ...col,
        type: "WINDOW",
      }));
      // Combine formula and window columns
      const allColumns = [...(synthesizeSpec?.columns || []), ...windowColumns];
      updateLabel(allColumns.length);
      setAddedColumns(allColumns);
      setSelectedFormulaCol({ column: "", formula: "" });
    }
    setTabKey("add");
  }, [openFormula]);

  useEffect(() => {
    // Handle WINDOW type columns
    if (
      selectedFormulaCol?.type === "WINDOW" &&
      selectedFormulaCol?.windowData
    ) {
      const windowData = selectedFormulaCol.windowData;
      setWindowColumnName(windowData.column_name || selectedFormulaCol.column);
      setWindowFunction(windowData.operation?.function || "");
      setAggColumn(windowData.operation?.agg_column || "");
      setPartitionBy(windowData.operation?.partition_by || []);
      setOrderBy(
        windowData.operation?.order_by?.length > 0
          ? windowData.operation.order_by
          : [{ column: "", direction: "ASC" }]
      );
      setEditingWindowCol(windowData.column_name || selectedFormulaCol.column);
      setTabKey("window");
      return;
    }
    // Handle FORMULA type columns
    if (selectedFormulaCol.formula) {
      setTabKey("edit");
      setCurrentEditFormula(selectedFormulaCol?.formula);
      setCurrentEditCol(selectedFormulaCol?.column);
      setPopulateFormulaForEdit(!populateFormulaForEdit);
      setSelectedCol(selectedFormulaCol?.column);
      const filtered = addedColumns.filter(
        (el) => el.column_name !== selectedFormulaCol?.column
      );
      setNotSelectedCols(filtered);
    }
  }, [selectedFormulaCol]);

  useEffect(() => {
    const res = checkArrayValuesInString(aggCols, currentFormula);
    setWarning(res);
  }, [currentFormula, aggCols]);

  useEffect(() => {
    if (openFormula) {
      getFormulas();
    }
  }, [openFormula]);

  useEffect(() => {
    const aggregateSpec = getGroupAndAggregationSpec(
      spec?.transform,
      transformIds?.GROUPS_AND_AGGREGATION
    );
    const cols = aggregateSpec.aggregate_columns.map((el) => el.alias);
    setAggCols(cols);
  }, [spec?.transform, transformIds?.GROUPS_AND_AGGREGATION]);

  useEffect(() => {
    if (openFormula) {
      setCurrentColumn("");
      setCurrentFormula("");
    }
  }, [openFormula]);

  // Tab items definition using the reusable component
  const items = [
    {
      key: "add",
      label: "Add Column",
      children: (
        <ColumnForm
          columnName={currentColumn}
          setColumnName={setCurrentColumn}
          formula={currentFormula}
          setFormula={setCurrentFormula}
          dupeColCheck={dupeColCheck}
          warning={warning}
          clearFormula={clearFormula}
          populateFormula={populateFormula}
          allColumns={allColumns}
          isLoading={isLoading}
          onCancel={handleCancelBtn}
          onSave={(e) => handleAdd(e)}
          setTabKey={setTabKey}
          formulaList={formulaList}
          formulaListData={formulaListData}
        />
      ),
    },
    {
      key: "edit",
      label: `Edit Column${
        addedColumns.length ? `: ${addedColumns.length}` : ""
      }`,
      children: (
        <div>
          {selectedCol && (
            <ColumnForm
              columnName={currentEditCol}
              setColumnName={setCurrentEditCol}
              formula={currentEditFormula}
              setFormula={setCurrentEditFormula}
              dupeColCheck={dupeColCheck}
              warning={warning}
              clearFormula={clearFormula}
              populateFormula={populateFormulaForEdit}
              allColumns={allColumns}
              isLoading={isLoading}
              onCancel={handleCancelBtn}
              onSave={handleUpdate}
              submitButtonText="Update"
              disabled={!isModified}
              setTabKey={setTabKey}
              formulaList={formulaList}
              formulaListData={formulaListData}
            />
          )}
          <Table
            bordered
            className="formula-list"
            columns={tableColumns}
            dataSource={addedColumns}
            footer={false}
            pagination={false}
            loading={isLoading}
            rowKey="column_name"
            scroll={selectedCol ? { y: 300 } : undefined}
            rowClassName={(record) =>
              record.type === "WINDOW" ? "window-row" : "formula-row"
            }
            locale={{
              emptyText: (
                <Empty
                  image={Empty.PRESENTED_IMAGE_SIMPLE}
                  description="No Columns Added"
                />
              ),
            }}
          />
        </div>
      ),
    },
    {
      key: "window",
      label: editingWindowCol ? `Window: ${editingWindowCol}` : "Window",
      children: (
        <WindowFunctionForm
          columnName={windowColumnName}
          setColumnName={setWindowColumnName}
          windowFunction={windowFunction}
          setWindowFunction={setWindowFunction}
          aggColumn={aggColumn}
          setAggColumn={setAggColumn}
          partitionBy={partitionBy}
          setPartitionBy={setPartitionBy}
          orderBy={orderBy}
          setOrderBy={setOrderBy}
          preceding={preceding}
          setPreceding={setPreceding}
          following={following}
          setFollowing={setFollowing}
          allColumns={allColumns}
          isLoading={isLoading}
          onCancel={handleCancelBtn}
          onSave={handleAddWindowColumn}
          dupeColCheck={windowDupeColCheck}
          submitButtonText={editingWindowCol ? "Update" : "Save"}
        />
      ),
    },
    {
      key: "help",
      label: "Help",
      children: (
        <FormulaHelp
          handleSearchFormula={handleSearchFormula}
          filteredformulaList={filteredformulaList}
          desc={desc}
          handleClick={handleClick}
        />
      ),
    },
  ];

  return (
    <>
      <ToolbarItem
        icon={
          isDarkTheme ? (
            <AddColumnRightDarkIcon className="toolbar-item-icon" />
          ) : (
            <AddColumnRightLightIcon className="toolbar-item-icon" />
          )
        }
        label={label}
        open={openFormula}
        className={countCol(addedColumnCount)}
        disabled={disabled}
        handleOpenChange={handleOpenChange}
        step={step}
      >
        <div className="ml-10 tabs-wrap">
          <Typography.Title level={5} className="m-0 mb-10 draggable-title">
            Add Column
          </Typography.Title>

          <Tabs
            items={items}
            onChange={onChange}
            className="tab-style"
            activeKey={tabKey}
          />
        </div>
      </ToolbarItem>
    </>
  );
}

AddColumn.propTypes = {
  allColumns: PropTypes.arrayOf(PropTypes.string).isRequired,
  spec: PropTypes.object.isRequired,
  updateSpec: PropTypes.func.isRequired,
  isLoading: PropTypes.bool.isRequired,
  disabled: PropTypes.bool.isRequired,
  step: PropTypes.array,
  openFormula: PropTypes.bool.isRequired,
  setOpenFormula: PropTypes.func,
  selectedFormulaCol: PropTypes.object,
  setSelectedFormulaCol: PropTypes.func,
  synthesizeValidationCols: PropTypes.array,
  isDarkTheme: PropTypes.bool.isRequired,
  saveTransformation: PropTypes.func.isRequired,
  handleDeleteTransformation: PropTypes.func.isRequired,
  handleGetColumns: PropTypes.func.isRequired,
};

export { AddColumn };
