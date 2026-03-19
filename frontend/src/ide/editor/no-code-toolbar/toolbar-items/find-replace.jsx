import { useState, useEffect } from "react";
import { Select, Typography, Button, Input, Table, Divider } from "antd";
import PropTypes from "prop-types";
import isEqual from "lodash/isEqual.js";
import { DeleteOutlined, EditOutlined } from "@ant-design/icons";

import FindReplaceInfoText from "./info-text";
import {
  generateKey,
  removeIdFromObjects,
  useEscapeKey,
  renameMap,
} from "../../../../common/helpers";
import { ToolbarItem } from "../toolbar-item";
import {
  FindReplaceDarkIcon,
  FindReplaceLightIcon,
} from "../../../../base/icons";
import { useTransformIdStore } from "../../../../store/transform-id-store";
import {
  getFindAndReplaceSpec,
  transformationTypes,
} from "../../no-code-model/helper";

const FindReplace = ({
  allColumns,
  updateSpec,
  spec,
  disabled,
  step,
  isDarkTheme,
  saveTransformation,
  isLoading,
  handleDeleteTransformation,
  handleGetColumns,
}) => {
  const optionEntry = {
    match_type: "TEXT",
    find: "",
    replace: "",
    id: generateKey(),
  };
  const [colCount, setColCount] = useState(0);
  const [label, setLabel] = useState("Find Replace");
  const [tableData, setTableData] = useState([]);
  const [edit, setEdit] = useState("");
  const [isModified, setIsModified] = useState(false);
  const [isUpdated, setIsUpdated] = useState(false);
  const [cols, setCols] = useState([]);
  const [initialData, setInitialData] = useState([]);
  const { transformIds } = useTransformIdStore();

  const [dataForEdit, setDataForEdit] = useState({
    column_list: [],
    operation: [],
  });
  const [operation, setOperation] = useState([optionEntry]);
  const [open, setOpen] = useState(false);
  const handleOpenChange = (value) => {
    if (value) {
      handleGetColumns(
        transformIds?.FIND_AND_REPLACE,
        transformationTypes?.FIND_AND_REPLACE
      );
    }
    setOpen(value);
  };

  useEscapeKey(open, () => setOpen(false));

  const handleEditAction = (data) => {
    const { operation, column_list } = data;
    setOperation(operation);
    setCols(column_list);
    setDataForEdit({ column_list, operation });
  };

  const handleDeleteColumn = (id) => {
    const filteredCols = tableData.filter((el) => el.id !== id);
    if (edit === id) {
      setOperation([optionEntry]);
      setCols([]);
    }
    setTableData(filteredCols);
  };

  const columns = [
    {
      title: "Column",
      dataIndex: "column",
      key: "column",
      render: (_, record) => (
        <Typography className="word-break" key={record.id}>
          {record.column_list.join(",")}
        </Typography>
      ),
    },
    {
      title: <Typography className="nowrap">Match Type</Typography>,
      dataIndex: "match type",
      key: "match type",
      render: (_, record) => (
        <Typography key={record.id} className="word-break">
          {record.operation.map((el) => el.match_type).join(",")}
        </Typography>
      ),
    },
    {
      title: "Find",
      dataIndex: "find",
      key: "find",
      render: (_, record) => (
        <Typography key={record.id} className="word-break">
          {record.operation.map((el) => el.find).join(",")}
        </Typography>
      ),
    },
    {
      title: "Replace",
      dataIndex: "replace",
      key: "replace",
      render: (_, record) => (
        <Typography key={record.id}>
          {record.operation.map((el) => el.replace).join(",")}
        </Typography>
      ),
    },
    {
      title: "Action",
      dataIndex: "action",
      key: "action",
      render: (_, record) => (
        <div>
          <EditOutlined
            onClick={() => {
              handleEditAction(record);
              setEdit(record.id);
            }}
          />
          <DeleteOutlined
            onClick={() => {
              handleDeleteColumn(record.id);
            }}
            className="ml-10 red"
          />
        </div>
      ),
    },
  ];

  const matchType = [
    { label: "Text", value: "TEXT" },
    { label: "Exact Text", value: "EXACT_TEXT" },
    { label: "Empty", value: "EMPTY" },
    { label: "Letters", value: "LETTERS" },
    { label: "Digits", value: "DIGITS" },
    { label: "Symbols", value: "SYMBOLS" },
    { label: "Whitespace", value: "WHITESPACE" },
    { label: "Currency", value: "CURRENCY" },
    { label: "Punctuation", value: "PUNCTUATION" },
    { label: "Regex", value: "REGEX" },
    { label: "Fill Null", value: "FILL_NULL" },
  ];

  const handleColChange = (value) => {
    setCols(value);
  };

  const updateLabel = (count) => {
    const plural = count > 1 ? "s" : "";
    const newLabel = count
      ? `Find & Replaced by ${count} column${plural}`
      : "Find & Replace";
    setLabel(newLabel);
  };

  const handleOperationChange = (value, name, id) => {
    const updated = operation.map((item) => {
      if (item.id === id) {
        if (name === "match_type") {
          return { ...item, [name]: value, find: "", replace: "" };
        }
        return { ...item, [name]: value };
      }
      return item;
    });
    setOperation(updated);
  };

  const handleSave = async () => {
    const removedID = removeIdFromObjects(tableData);
    const removedIdfromOpertaion = removedID.map((el) => {
      const updated = removeIdFromObjects(el.operation).map((el) => {
        const data = { ...el };
        if (!data.find) {
          delete data.find;
        }
        return data;
      });
      return { ...el, operation: updated };
    });

    let result = {};
    if (removedIdfromOpertaion?.length === 0) {
      const body = {
        step_id: transformIds?.FIND_AND_REPLACE,
      };
      result = await handleDeleteTransformation(body);
    } else {
      const body = {
        type: "find_and_replace",
        find_and_replace: { replacements: removedIdfromOpertaion },
      };
      result = await saveTransformation(body, transformIds?.FIND_AND_REPLACE);
    }

    if (result?.status === "success") {
      updateSpec(result?.spec);
      setOpen(false);
      setColCount(tableData.length);
      updateLabel(tableData.length);
      setOperation([optionEntry]);
      setCols([]);
      setEdit("");
      setTableData([]);
      setInitialData([]);
    } else {
      setOpen(true);
    }
  };

  useEffect(() => {
    const specEdit = getFindAndReplaceSpec(
      spec?.transform,
      transformIds?.FIND_AND_REPLACE
    )?.replacements;

    if (open) {
      if (specEdit.length) {
        const updatedWithKey = specEdit.map((el) => {
          const idInOperation = el.operation.map((item) => {
            return { ...item, id: generateKey() };
          });

          return { ...el, id: generateKey(), operation: idInOperation };
        });
        updateLabel(updatedWithKey.length);
        setTableData(updatedWithKey);
        setInitialData(updatedWithKey);
        setColCount(updatedWithKey.length);
      } else {
        updateLabel(0);
        setColCount(0);
      }
    } else {
      updateLabel(specEdit?.length);
      setColCount(specEdit?.length);
    }
  }, [spec?.transform, transformIds?.FIND_AND_REPLACE, open]);

  const addQueryToTable = () => {
    setTableData([
      ...tableData,
      { id: generateKey(), operation: operation, column_list: cols },
    ]);

    setOperation([optionEntry]);
    setCols([]);
  };

  const updateQueryFromTable = () => {
    const updatedQuery = tableData.map((el) => {
      if (el.id === edit) {
        return { ...el, operation: operation, column_list: cols };
      }
      return el;
    });
    setTableData(updatedQuery);
    setOperation([optionEntry]);
    setEdit("");
    setCols([]);
  };

  const plusBtnValidation = () => {
    for (const item of operation) {
      for (const i in item) {
        if (!item[i]) {
          if (!["REGEX", "TEXT"].includes(item.match_type) && item.replace) {
            return item?.find;
          }
          return true;
        }
      }
    }
    return false;
  };

  useEffect(() => {
    const res = isEqual(initialData, tableData);
    setIsModified(!res);
  }, [initialData, tableData]);

  useEffect(() => {
    if (edit) {
      const res = isEqual({ operation, column_list: cols }, dataForEdit);
      setIsUpdated(!res);
    }
  }, [dataForEdit, operation, cols]);

  const filterCols = () => {
    // Create rename mapping for lookup
    const renameChecks = renameMap(spec, transformIds?.RENAME_COLUMN);

    const seen = new Set();

    return Object.keys(allColumns)
      .filter((column) => {
        return !["Number", "Time", "Date"].includes(
          allColumns[column].data_type
        );
      })
      .map((column) => {
        const newName = renameChecks?.[column] || column;
        const label = renameChecks?.[column]
          ? `${newName} | ${column}`
          : column;

        if (seen.has(newName)) return null;
        seen.add(newName);

        return { label, value: column };
      })
      .filter(Boolean);
  };

  return (
    <ToolbarItem
      icon={
        isDarkTheme ? (
          <FindReplaceDarkIcon className="toolbar-item-icon" />
        ) : (
          <FindReplaceLightIcon className="toolbar-item-icon" />
        )
      }
      label={label}
      open={open}
      setOpen={setOpen}
      disabled={disabled}
      handleOpenChange={handleOpenChange}
      className={
        colCount > 0 ? "no-code-toolbar-filter-conditions-highlight" : ""
      }
      step={step}
    >
      <div className="ml-10 mr-10 width-600 ">
        <Typography.Title
          level={5}
          className="m-0 mb-16 group-title draggable-title"
        >
          Find & Replace
        </Typography.Title>
        <div className="m-10">
          <div className="column_field_wrap">
            <Typography>Columns</Typography>
            <Select
              mode="multiple"
              placeholder="Please select"
              onChange={handleColChange}
              className="width-100"
              options={filterCols()}
              value={cols}
            />
          </div>
        </div>

        {operation.length > 0 &&
          operation.map((item) => {
            return (
              <div className="m-5" key={item.id}>
                <div className="column_field_wrap width-100 flex-align-end">
                  <div className="flex-1">
                    <Typography>Match Type</Typography>
                    <Select
                      placeholder="Please select"
                      className="width-100"
                      value={item.match_type}
                      options={matchType}
                      onChange={(value) =>
                        handleOperationChange(value, "match_type", item.id)
                      }
                    />
                  </div>
                  <div className="flex-1  ml-10 mr-10">
                    <Typography>Find</Typography>
                    <Input
                      value={item.find}
                      disabled={
                        !["REGEX", "TEXT", "EXACT_TEXT"].includes(
                          item.match_type
                        )
                      }
                      onChange={(e) =>
                        handleOperationChange(e.target.value, "find", item.id)
                      }
                      onKeyDown={(e) => e.stopPropagation()}
                    />
                  </div>
                  <div className="flex-1">
                    <Typography>Replace</Typography>
                    <Input
                      value={item.replace}
                      onChange={(e) =>
                        handleOperationChange(
                          e.target.value,
                          "replace",
                          item.id
                        )
                      }
                      onKeyDown={(e) => e.stopPropagation()}
                    />
                  </div>
                  <Button
                    className="ml-10"
                    onClick={edit ? updateQueryFromTable : addQueryToTable}
                    disabled={
                      plusBtnValidation() ||
                      !cols.length ||
                      (edit && !isUpdated)
                    }
                    type="primary"
                  >
                    {edit ? "Update" : "+ Add"}
                  </Button>
                </div>
                <FindReplaceInfoText type={item.match_type} />
              </div>
            );
          })}

        <Divider className="m-25-tb" />
        <Typography.Text type="secondary" strong>
          Existing Rules
        </Typography.Text>
        <div className="m-10">
          <div className="column_field_wrap flex-end-container">
            <Table
              columns={columns}
              dataSource={tableData}
              pagination={false}
              scroll={{ y: 200 }}
            />
          </div>
        </div>

        <div className="flex-end-container">
          <Button onClick={() => setOpen(false)}>Cancel</Button>
          <Button
            className="ml-10"
            type="primary"
            onClick={handleSave}
            disabled={!isModified}
            loading={isLoading}
          >
            Save
          </Button>
        </div>
      </div>
    </ToolbarItem>
  );
};

FindReplace.propTypes = {
  disabled: PropTypes.bool.isRequired,
  spec: PropTypes.object.isRequired,
  updateSpec: PropTypes.func.isRequired,
  allColumns: PropTypes.object.isRequired,
  step: PropTypes.array.isRequired,
  isDarkTheme: PropTypes.bool.isRequired,
  saveTransformation: PropTypes.func.isRequired,
  handleDeleteTransformation: PropTypes.func.isRequired,
  isLoading: PropTypes.bool.isRequired,
  handleGetColumns: PropTypes.func.isRequired,
};
export { FindReplace };
