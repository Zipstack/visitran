import { useEffect, useState } from "react";
import { Select, Button, Space, Typography } from "antd";
import { DeleteOutlined, PlusOutlined } from "@ant-design/icons";
import PropTypes from "prop-types";
import isEqual from "lodash/isEqual.js";
import cloneDeep from "lodash/cloneDeep.js";

import { ToolbarItem } from "../toolbar-item.jsx";
import {
  stopPropagation,
  renameMap,
  useEscapeKey,
} from "../../../../common/helpers.js";
import { SortDarkIcon, SortLightIcon } from "../../../../base/icons/index.js";
import { useTransformIdStore } from "../../../../store/transform-id-store.js";
import { transformationTypes } from "../../no-code-model/helper.js";

// CSS for this component is added in the parent component's CSS file (no-code-model)
function Sort({
  allColumns,
  spec,
  updateSpec,
  isLoading,
  disabled,
  step,
  allColumnsDetails,
  isDarkTheme,
  handleSetPresentation,
  handleGetColumns,
}) {
  const [sortedColumns, setSortedColumns] = useState([]);
  const [open, setOpen] = useState(false);
  const [label, setLabel] = useState("Sort");
  const [sortedColumnCount, setSortedColumnCount] = useState(0);
  const [isModified, setIsModified] = useState(false);
  const allColumnsKey = Object.values(allColumns);
  const { transformIds } = useTransformIdStore();
  const columnDataTypes = Object.entries(allColumnsDetails).map(
    ([key, value]) => ({
      [key]: value.data_type,
    })
  );
  const updateLabel = (newSortedColumnCount) => {
    const plural = newSortedColumnCount > 1 ? "s" : "";
    const newLabel = newSortedColumnCount
      ? `Sorted by ${newSortedColumnCount} column${plural}`
      : "Sort";
    setLabel(newLabel);
    setSortedColumnCount(newSortedColumnCount);
  };

  const handleAdd = (event) => {
    event.stopPropagation();
    const options = [...sortedColumns];
    const columnOptions = getColumnOptions(null);
    const selectedColumn = columnOptions[0].value;
    const columnType = columnDataTypes.find((col) => col[selectedColumn]);

    if (columnOptions.length) {
      setSortedColumns([
        ...options,
        {
          column: columnOptions[0].value,
          order_by: "ASC",
          data_type: columnType?.[selectedColumn],
        },
      ]);
    }
  };

  const handleOptionChange = (index, value, key) => {
    const newSortedColumns = [...sortedColumns];
    newSortedColumns[index] = { ...newSortedColumns[index], [key]: value };

    if (key === "column") {
      const columnType = columnDataTypes.find((col) => col[value]);
      if (columnType) {
        newSortedColumns[index].data_type = columnType[value];
      } else {
        newSortedColumns[index].data_type = undefined;
      }
    }

    setSortedColumns(newSortedColumns);
  };

  const handleRemove = (index, event) => {
    event.stopPropagation();
    const newSortedColumns = [...sortedColumns];
    newSortedColumns.splice(index, 1);
    setSortedColumns(newSortedColumns);
  };

  const handleSave = async () => {
    if (!isModified) {
      return;
    }

    const body = {
      sort: sortedColumns,
    };

    const result = await handleSetPresentation(body);
    if (result?.status === "success") {
      updateSpec(result?.spec);
      updateLabel(sortedColumns.length);
      setOpen(false);
    } else {
      setOpen(true);
    }
  };

  const getColumnOptions = (currentColumn) => {
    const usedColumns = new Set(sortedColumns.map(({ column }) => column));
    usedColumns.delete(currentColumn);

    // Map of renamed columns from spec
    const renameChecks = renameMap(spec, transformIds?.RENAME_COLUMN);
    const seenValues = new Set();

    return allColumnsKey
      .filter((column) => !usedColumns.has(column))
      .map((column) => {
        const newName = renameChecks[column] || column;
        const label = renameChecks[column] ? `${newName} | ${column}` : column;

        let uniqueValue = newName;
        let counter = 1;
        while (seenValues.has(uniqueValue)) {
          uniqueValue = `${newName}_${counter++}`;
        }
        seenValues.add(uniqueValue);

        return { value: uniqueValue, label };
      });
  };

  useEffect(() => {
    const newSortedColumns = cloneDeep(spec.presentation.sort || []);
    setSortedColumns(newSortedColumns);
    updateLabel(newSortedColumns.length);
  }, [spec?.presentation?.sort, open]);

  useEffect(() => {
    setIsModified(!isEqual(sortedColumns, spec.presentation.sort));
  }, [sortedColumns]);
  const handleOpenChange = (value) => {
    if (!value) {
      setSortedColumns(spec.presentation.sort);
    } else {
      handleGetColumns(transformIds?.SORT, transformationTypes?.SORT);
    }
    setOpen(value);
  };

  const handleCancelBtn = () => {
    setOpen(false);
    setSortedColumns(spec.presentation.sort);
  };

  useEscapeKey(open, handleCancelBtn);

  return (
    <ToolbarItem
      icon={
        isDarkTheme ? (
          <SortDarkIcon className="toolbar-item-icon" />
        ) : (
          <SortLightIcon className="toolbar-item-icon" />
        )
      }
      label={label}
      open={open}
      className={
        sortedColumnCount ? "no-code-toolbar-sorted-cols-highlight" : ""
      }
      disabled={disabled}
      handleOpenChange={handleOpenChange}
      step={step}
    >
      <div className="ml-10 mr-10 width-420px">
        <Typography.Title
          level={5}
          className="m-0 mb-10 sort-title draggable-title"
        >
          Sort By
        </Typography.Title>
        <div className="sort-list-wrap">
          {sortedColumns.map(({ column, order_by }, index) => (
            <div className="mb-10" key={column}>
              <Space className="sort-items">
                <Select
                  showSearch
                  defaultValue={column}
                  onChange={(value) =>
                    handleOptionChange(index, value, "column")
                  }
                  filterOption={(input, option) =>
                    (option?.label ?? "")
                      .toLowerCase()
                      .includes(input.toLowerCase())
                  }
                  options={getColumnOptions(column)}
                  onKeyDown={stopPropagation}
                />
                <Select
                  value={order_by}
                  onChange={(value) =>
                    handleOptionChange(index, value, "order_by")
                  }
                  options={[
                    {
                      label: "Ascending",
                      value: "ASC",
                    },
                    {
                      label: "Descending",
                      value: "DESC",
                    },
                  ]}
                  onKeyDown={stopPropagation}
                />
                <Button
                  icon={<DeleteOutlined />}
                  onClick={(e) => handleRemove(index, e)}
                  danger
                />
              </Space>
            </div>
          ))}
        </div>

        <Button
          onClick={(e) => handleAdd(e)}
          className="p-0 mb-10 bg-transparent"
          disabled={allColumnsKey.length === sortedColumns.length}
          icon={<PlusOutlined />}
          type="text"
        >
          {sortedColumns.length ? "Add another sort" : "Add a sort"}
        </Button>
        <div className="flex-end-container">
          <Button onClick={handleCancelBtn}>Cancel</Button>
          <Button
            className="ml-10"
            onClick={handleSave}
            disabled={isLoading || !isModified}
            type="primary"
            loading={isLoading}
          >
            Sort
          </Button>
        </div>
      </div>
    </ToolbarItem>
  );
}

Sort.propTypes = {
  allColumns: PropTypes.arrayOf(PropTypes.string).isRequired,
  spec: PropTypes.object.isRequired,
  updateSpec: PropTypes.func.isRequired,
  isLoading: PropTypes.bool.isRequired,
  disabled: PropTypes.bool.isRequired,
  step: PropTypes.array,
  allColumnsDetails: PropTypes.object.isRequired,
  isDarkTheme: PropTypes.bool.isRequired,
  handleSetPresentation: PropTypes.func.isRequired,
  handleGetColumns: PropTypes.func.isRequired,
};

export { Sort };
