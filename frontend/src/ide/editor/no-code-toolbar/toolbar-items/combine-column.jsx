import { useState, useMemo, useCallback, useEffect } from "react";
import {
  Form,
  Input,
  Select,
  Button,
  Typography,
  Table,
  message,
  Divider,
  Space,
  Alert,
} from "antd";
import { PlusOutlined, DeleteOutlined, EditOutlined } from "@ant-design/icons";
import PropTypes from "prop-types";

import { ToolbarItem } from "../toolbar-item";
import {
  CombineColumnsDarkIcon,
  CombineColumnsLightIcon,
} from "../../../../base/icons";
import {
  generateKey,
  renameMap,
  useEscapeKey,
} from "../../../../common/helpers";

import "./combine-column.css";
import { useTransformIdStore } from "../../../../store/transform-id-store";
import {
  getCombineColumnsSpec,
  transformationTypes,
} from "../../no-code-model/helper";

const MIN_COL_COUNT = 2;
const MIN_TOTAL = 2;
const MAX_TOTAL = 10;

const initialRow = () => ({
  id: generateKey(),
  columnName: "",
  values: [
    { id: generateKey(), type: "", value: "" },
    { id: generateKey(), type: "", value: "" },
  ],
});

const CombineColumn = ({
  allColumns,
  updateSpec,
  spec,
  disabled,
  step,
  isDarkTheme,
  saveTransformation,
  handleDeleteTransformation,
  isLoading,
  handleGetColumns,
}) => {
  /* --------------------------- state -------------------------------- */
  const [rows, setRows] = useState([initialRow()]);
  const [combineList, setCombineList] = useState([]);
  const [open, setOpen] = useState(false);
  const [editingId, setEditingId] = useState(null);
  const [label, setLabel] = useState("Combine Column");
  const [isModified, setIsModified] = useState(false);
  const [validationError, setValidationError] = useState("");
  const { transformIds } = useTransformIdStore();
  const [combineCount, setcombineCount] = useState(0);
  const [editColumnName, setEditColumnName] = useState("");
  /* ------------------ escape key to close form ---------------------- */
  useEscapeKey(open, () => setOpen(false));

  /* ------------------ derive label from combineList ----------------- */
  const updateLabel = useCallback((count) => {
    setLabel(
      count
        ? `CombineColumn ${count} column${count > 1 ? "s" : ""}`
        : "Combine Column"
    );
    setcombineCount(count);
  }, []);

  /* ------------- initialise from existing spec (edit mode) ---------- */
  useEffect(() => {
    const list = getCombineColumnsSpec(
      spec?.transform,
      transformIds?.COMBINE_COLUMNS
    )?.columns;
    const formatted = list.map((i) => ({ ...i, id: generateKey() }));
    setCombineList(formatted);
    updateLabel(formatted.length);
  }, [spec?.transform, transformIds?.COMBINE_COLUMNS, updateLabel]);

  /* ------------- Reset states when modal closes ---------- */
  useEffect(() => {
    if (!open) {
      setRows([initialRow()]);
      setEditingId(null);
      setEditColumnName("");
      setValidationError("");

      // Restore combineList from spec if modal was closed without saving
      const list = getCombineColumnsSpec(
        spec?.transform,
        transformIds?.COMBINE_COLUMNS
      )?.columns;
      const formatted = list.map((i) => ({ ...i, id: generateKey() }));
      setCombineList(formatted);
      updateLabel(formatted.length);
      setIsModified(false); // reset modified flag
    } else {
      handleGetColumns(
        transformIds?.COMBINE_COLUMNS,
        transformationTypes?.COMBINE_COLUMNS
      );
    }
  }, [open]);

  /* ------------------------ helpers --------------------------------- */
  /** Build <Select options> for column dropdown, obeying max-5 rule */
  const buildColumnOptions = useCallback(
    (row) => {
      const renameChecks = renameMap(spec, transformIds?.RENAME_COLUMN);
      /** all columns already chosen anywhere in *this* row */
      const selected = row.values
        .filter((v) => v.type === "column")
        .map((v) => v.value);

      const seen = new Set();
      return allColumns
        .filter((c) => !selected.includes(c))
        .map((c) => {
          const original = renameChecks?.[c] || c;
          const labelTxt = renameChecks?.[c] ? `${c} (${original})` : c;
          if (seen.has(c)) return null;
          seen.add(c);
          return { label: labelTxt, value: original };
        })
        .filter(Boolean);
    },
    [allColumns, spec, transformIds?.RENAME_COLUMN]
  );

  /** Validate current `rows` state before Add / Update */
  const validateRows = useCallback(
    (checkRows) => {
      setValidationError("");
      for (const row of checkRows) {
        if (!row.columnName.trim()) {
          setValidationError("Column name cannot be empty.");
          return false;
        }
        if (
          allColumns
            .filter((c) => !editingId || c !== editColumnName)
            .includes(row.columnName)
        ) {
          setValidationError(
            "Column name conflicts with an existing table column."
          );
          return false;
        }
        if (
          combineList
            .filter((i) => i.id !== editingId)
            .concat(checkRows.filter((r) => r.id !== row.id))
            .some((i) => i.columnName === row.columnName)
        ) {
          setValidationError("Duplicate column name found.");
          return false;
        }

        if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(row.columnName)) {
          setValidationError(
            "Invalid column name. Use letters, numbers, and underscores, and do not start with a number."
          );
          return false;
        }

        const columnsCount = row.values.filter(
          (v) => v.type === "column"
        ).length;
        if (row.values.length < MIN_TOTAL) {
          setValidationError("A minimum of two values is required.");
          return false;
        }
        if (columnsCount < MIN_COL_COUNT) {
          setValidationError("At least two value must be of type Column.");
          return false;
        }

        if (row.values.length > MAX_TOTAL) {
          setValidationError(`Maximum ${MAX_TOTAL} values per row.`);
          return false;
        }

        const everyFilled = row.values.every((v) => v.type && v.value);
        if (!everyFilled) {
          setValidationError("Please fill all type/value fields.");
          return false;
        }
      }
      return true;
    },
    [combineList, allColumns, editingId, editColumnName]
  );

  /* --------------------- state changers ----------------------------- */
  const handleRowChange = useCallback((rowId, key, val) => {
    setRows((prev) =>
      prev.map((r) => (r.id === rowId ? { ...r, [key]: val } : r))
    );
  }, []);

  const handleValueChange = useCallback((rowId, valId, key, val) => {
    setRows((prev) =>
      prev.map((r) =>
        r.id === rowId
          ? {
              ...r,
              values: r.values.map((v) =>
                v.id === valId ? { ...v, [key]: val } : v
              ),
            }
          : r
      )
    );
  }, []);

  const addValue = useCallback((rowId, afterValId) => {
    setRows((prev) =>
      prev.map((r) => {
        if (r.id !== rowId) return r;

        const insertAt = r.values.findIndex((v) => v.id === afterValId) + 1;
        const nextValues = [...r.values];
        nextValues.splice(insertAt, 0, {
          id: generateKey(),
          type: "",
          value: "",
        });
        return { ...r, values: nextValues };
      })
    );
  }, []);

  const removeValue = useCallback(
    (rowId, valId) =>
      setRows((prev) =>
        prev.map((r) =>
          r.id === rowId
            ? { ...r, values: r.values.filter((v) => v.id !== valId) }
            : r
        )
      ),
    []
  );

  /* ----------------------- Add / Update ----------------------------- */
  const addCombineColumn = useCallback(() => {
    if (!validateRows(rows)) return;

    setCombineList((prev) => {
      if (editingId) {
        return prev.map((i) =>
          i.id === editingId ? { ...rows[0], id: editingId } : i
        );
      }
      return [...prev, ...rows];
    });
    setRows([initialRow()]);
    setEditingId(null);
    setEditColumnName("");
    setIsModified(true);
    setValidationError("");
  }, [rows, validateRows, editingId]);

  const editCombineColumn = useCallback(
    (id) => {
      const data = combineList.find((i) => i.id === id);
      if (data) {
        setRows([{ ...data }]);
        setEditingId(id);
        setEditColumnName(data?.columnName);
        setOpen(true);
      }
    },
    [combineList]
  );

  const removeCombineColumn = useCallback((id) => {
    setCombineList((prev) => prev.filter((i) => i.id !== id));
    setIsModified(true);
  }, []);

  /* ------------------------- Save ----------------------------------- */
  const handleSave = useCallback(async () => {
    if (!isModified) {
      message.warning("Nothing to save.");
      return;
    }

    const combineColumns = combineList.map(({ id, ...r }) => r);
    let result = {};
    if (combineColumns?.length === 0) {
      const body = {
        step_id: transformIds?.COMBINE_COLUMNS,
      };
      result = await handleDeleteTransformation(body);
    } else {
      const body = {
        type: "combine_columns",
        combine_columns: { columns: combineColumns },
      };
      result = await saveTransformation(body, transformIds?.COMBINE_COLUMNS);
    }

    if (result?.status === "success") {
      updateSpec(result?.spec);
      setIsModified(false);
      setOpen(false);
      updateLabel(combineList.length);
    } else {
      setOpen(true);
    }
  }, [
    isModified,
    spec,
    combineList,
    saveTransformation,
    updateSpec,
    updateLabel,
  ]);

  /* ------------------------ table cols memo -------------------------- */
  const tableColumns = useMemo(
    () => [
      {
        title: "Combined Column",
        dataIndex: "columnName",
        key: "columnName",
      },
      {
        title: "Values",
        dataIndex: "values",
        key: "values",
        render: (vals) =>
          vals
            .map(
              (v) => `${v.type === "column" ? "Column: " : "Value: "}${v.value}`
            )
            .join(", "),
      },
      {
        title: "Action",
        key: "action",
        render: (_, record) => (
          <div className="row-actions">
            <EditOutlined onClick={() => editCombineColumn(record.id)} />
            <DeleteOutlined
              onClick={() => removeCombineColumn(record.id)}
              className="ml-10 red"
            />
          </div>
        ),
      },
    ],
    [editCombineColumn, removeCombineColumn]
  );

  /* ---------------------------- UI ---------------------------------- */
  return (
    <ToolbarItem
      icon={
        isDarkTheme ? (
          <CombineColumnsDarkIcon className="toolbar-item-icon" />
        ) : (
          <CombineColumnsLightIcon className="toolbar-item-icon" />
        )
      }
      label={label}
      open={open}
      setOpen={setOpen}
      disabled={disabled}
      step={step}
      handleOpenChange={setOpen}
      className={
        combineCount !== 0 ? "no-code-toolbar-filter-conditions-highlight" : ""
      }
    >
      <div className="combine-col-container">
        <Typography.Title
          level={5}
          className="combine-col-title draggable-title"
        >
          Combine Column
        </Typography.Title>

        {rows.map((row) => {
          return (
            <div key={row?.id} className="combine-col-form-container">
              {validationError && (
                <Alert
                  type="error"
                  message={validationError}
                  showIcon
                  className="mb-10"
                />
              )}
              <Form
                key={row.id}
                layout="vertical"
                className="combine-col-row-form"
                autoComplete="off"
                onFinish={addCombineColumn}
              >
                <div>
                  <Typography.Text>Construct Column</Typography.Text>
                </div>
                {/* ----- Values list ----- */}
                {row.values.map((val, idx) => {
                  const prevIsText =
                    idx > 0 && row.values[idx - 1].type === "value";

                  return (
                    <div key={idx} className="field-group">
                      <Form.Item required className="field">
                        <Select
                          value={val.type}
                          onChange={(v) =>
                            handleValueChange(row.id, val.id, "type", v)
                          }
                          placeholder="Select type"
                          options={[
                            {
                              label: "Column",
                              value: "column",
                            },
                            {
                              label: "Text",
                              value: "value",
                              disabled: prevIsText,
                            },
                          ]}
                        />
                      </Form.Item>

                      <Form.Item required className="field">
                        {val.type === "value" ? (
                          <Input
                            value={val.value}
                            className="field"
                            placeholder="Enter text"
                            disabled={!val.type}
                            onChange={(e) =>
                              handleValueChange(
                                row.id,
                                val.id,
                                "value",
                                e.target.value
                              )
                            }
                          />
                        ) : (
                          <Select
                            value={val.value}
                            options={buildColumnOptions(row)}
                            className="field"
                            placeholder="Select column"
                            disabled={!val.type}
                            onChange={(v) =>
                              handleValueChange(row.id, val.id, "value", v)
                            }
                          />
                        )}
                      </Form.Item>
                      <Button
                        icon={<PlusOutlined />}
                        onClick={() => addValue(row.id, val.id)}
                        disabled={row.values.length >= MAX_TOTAL}
                      />
                      <Button
                        icon={<DeleteOutlined />}
                        onClick={() => removeValue(row.id, val.id)}
                        disabled={row.values.length <= MIN_TOTAL}
                        danger
                      />
                    </div>
                  );
                })}

                <Divider />

                {/* ----- new column name ----- */}
                <Form.Item
                  label="New Column Name"
                  help={
                    /* compute error inline for live feedback */
                    allColumns
                      .filter((c) => !editingId || c !== editColumnName)
                      .includes(row.columnName)
                      ? "Conflicts with existing table column."
                      : combineList
                          .filter((c) => c.id !== editingId)
                          .concat(rows.filter((r) => r.id !== row.id))
                          .some((c) => c.columnName === row.columnName)
                      ? "Duplicate column name."
                      : row.columnName &&
                        !/^[A-Za-z_][A-Za-z0-9_]*$/.test(row.columnName)
                      ? "Use letters, numbers, and underscores; cannot start with a number."
                      : ""
                  }
                  validateStatus={
                    row.columnName &&
                    (allColumns
                      .filter((c) => !editingId || c !== editColumnName)
                      .includes(row.columnName) ||
                      combineList
                        .filter((c) => c.id !== editingId)
                        .concat(rows.filter((r) => r.id !== row.id))
                        .some((c) => c.columnName === row.columnName) ||
                      !/^[A-Za-z_][A-Za-z0-9_]*$/.test(row.columnName))
                      ? "error"
                      : ""
                  }
                >
                  <Input
                    value={row.columnName}
                    onChange={(e) =>
                      handleRowChange(row.id, "columnName", e.target.value)
                    }
                    placeholder="Enter new column name"
                  />
                </Form.Item>
                <div className="combine-col-footer-actions">
                  <Button
                    type="primary"
                    htmlType="submit"
                    disabled={!rows.length}
                  >
                    {editingId ? "Update" : "Add"}
                  </Button>
                </div>
              </Form>
            </div>
          );
        })}

        <Space direction="vertical" className="width-100">
          <Table
            size="small"
            rowKey="id"
            pagination={false}
            dataSource={combineList}
            columns={tableColumns}
            className="mt-20"
            bordered
          />
          <div className="combine-col-save-container">
            <Button
              type="primary"
              onClick={handleSave}
              disabled={!isModified}
              loading={isLoading}
            >
              Save
            </Button>
          </div>
        </Space>
      </div>
    </ToolbarItem>
  );
};

/* ------------------------ PropTypes --------------------------------- */
CombineColumn.propTypes = {
  allColumns: PropTypes.arrayOf(PropTypes.string).isRequired,
  updateSpec: PropTypes.func.isRequired,
  spec: PropTypes.object.isRequired,
  disabled: PropTypes.bool.isRequired,
  step: PropTypes.array.isRequired,
  isDarkTheme: PropTypes.bool.isRequired,
  saveTransformation: PropTypes.func.isRequired,
  handleDeleteTransformation: PropTypes.func.isRequired,
  isLoading: PropTypes.bool.isRequired,
  handleGetColumns: PropTypes.func.isRequired,
};

export { CombineColumn };
