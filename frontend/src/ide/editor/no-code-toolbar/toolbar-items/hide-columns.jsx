import { useEffect, useState, useCallback, useMemo } from "react";
import { Typography, Input, List, Switch, Button } from "antd";
import { SearchOutlined, DragOutlined } from "@ant-design/icons";
import PropTypes from "prop-types";
import debounce from "lodash/debounce.js";
import isEqual from "lodash/isEqual.js";
import "./hide-columns.css";

import { ToolbarItem } from "../toolbar-item.jsx";
import { useEscapeKey, stopPropagation } from "../../../../common/helpers.js";
import {
  OrganiserLightIcon,
  OrganiserDarkIcon,
} from "../../../../base/icons/index.js";

function HideColumns({
  allColumns,
  spec,
  updateSpec,
  isLoading,
  disabled,
  step,
  isDarkTheme,
  handleSetPresentation,
  width = 520,
}) {
  const [open, setOpen] = useState(false);
  const [columns, setColumns] = useState([]);
  const [pendingChanges, setPendingChanges] = useState([]);
  const [searchText, setSearchText] = useState("");
  const [label, setLabel] = useState("Column Organizer");
  const [hiddenColumnCount, setHiddenColumnCount] = useState(0);
  const [dragstarted, setDragstarted] = useState(null);
  const [disable, setDisable] = useState(false);
  const [filteredColumns, setFilteredColumns] = useState([]);

  // Backup state
  const [originalColumnsState, setOriginalColumnsState] = useState([]);

  const filtered = useMemo(() => {
    return columns.filter(({ column }) =>
      column?.toLowerCase()?.includes(searchText?.toLowerCase() || "")
    );
  }, [columns, searchText]);

  useEffect(() => {
    setFilteredColumns(filtered);
  }, [filtered]);

  useEscapeKey(open, () => {
    setOpen(false);
    resetChanges();
  });

  const debounceSetSearchText = useCallback(
    debounce((text) => {
      setSearchText(text);
    }, 500),
    []
  );

  const handleSearch = (e) => {
    debounceSetSearchText(e.target.value.trim());
  };

  const updateLabel = (newHiddenColumnCount) => {
    const newLabel = newHiddenColumnCount
      ? `Column Organizer (${newHiddenColumnCount}) `
      : "Column Organizer";
    setLabel(newLabel);
    setHiddenColumnCount(newHiddenColumnCount);
  };

  const handleUpdateColumns = useCallback((newColumns) => {
    setColumns(newColumns);
    setPendingChanges(newColumns);
    setFilteredColumns(newColumns);
  }, []);

  const applyChanges = async () => {
    const hiddenColumns = pendingChanges
      .filter(({ show }) => !show)
      .map(({ column }) => column);

    updateLabel(hiddenColumns.length);

    const body = {
      hidden_columns: hiddenColumns.length === 0 ? [] : hiddenColumns,
      column_order: pendingChanges.map(({ column }) => column),
    };

    const result = await handleSetPresentation(body);

    if (result?.status === "success") {
      updateSpec(result?.spec);
      setOpen(false);
      setPendingChanges([]);
    } else {
      resetChanges();
    }
  };

  const resetChanges = () => {
    setColumns(originalColumnsState);
    setPendingChanges(originalColumnsState);
    setFilteredColumns(originalColumnsState);
  };

  const handleToggle = useCallback(
    (updateColumn) => {
      const newColumns = columns.map((item) => {
        if (item.column !== updateColumn) return item;
        if (item.disableToggle) return item; // prevent logic update if toggle is disabled
        return { ...item, show: !item.show };
      });

      setFilteredColumns(newColumns);
      handleUpdateColumns(newColumns);
    },
    [columns]
  );

  const handleToggleAll = useCallback(
    (type) => {
      const filteredColumnNames = filteredColumns.map(({ column }) => column);
      const newColumns = columns.map((item) => ({
        column: item?.column,
        show: filteredColumnNames.includes(item?.column)
          ? type === "show"
          : item?.show,
      }));
      handleUpdateColumns(newColumns);
    },
    [filteredColumns, columns]
  );

  useEffect(() => {
    const outputColumns = spec?.presentation?.hidden_columns || [];
    const savedOrder = spec?.presentation?.column_order || [];

    // Determine base column list respecting persisted order
    let orderedColumns;
    if (savedOrder.length > 0) {
      const existing = savedOrder.filter((col) => allColumns?.includes(col));
      const newCols = allColumns?.filter((col) => !savedOrder.includes(col));
      orderedColumns = [...existing, ...newCols];
    } else {
      orderedColumns = allColumns;
    }

    const newColumns = orderedColumns?.map((column) => {
      const isHiddenInSpec =
        outputColumns.length > 0 && outputColumns.includes(column);

      return {
        column,
        show: !isHiddenInSpec,
      };
    });

    setColumns(newColumns);
    setPendingChanges(newColumns);
    setFilteredColumns(newColumns);
    updateLabel(newColumns.filter(({ show }) => !show).length);

    if (open) {
      setOriginalColumnsState(newColumns);
    }
  }, [
    open,
    spec?.presentation?.hidden_columns,
    spec?.presentation?.column_order,
    allColumns,
  ]);

  const handleOpenChange = (value) => {
    setOpen(value);
  };

  const dragStart = useCallback((e, index) => {
    setDragstarted(index);
    e.dataTransfer.effectAllowed = "move";
  }, []);

  const dragOver = useCallback(
    (e, index) => {
      e.preventDefault();
      if (dragstarted === undefined || index === dragstarted) return;
      const updatedItems = [...filteredColumns];
      const dragged = updatedItems.splice(dragstarted, 1);
      updatedItems.splice(index, 0, dragged[0]);
      setFilteredColumns(updatedItems);
      setDragstarted(index);
    },
    [dragstarted, filteredColumns]
  );

  const detectColumnReorder = useCallback(() => {
    for (let i = 0; i < columns.length; i++) {
      const filtered = filteredColumns[i]?.column;
      const initial = columns[i]?.column;
      if (filtered !== initial) return true;
    }
    return false;
  }, [columns, filteredColumns]);

  const handleDrop = useCallback(
    (e) => {
      e.preventDefault();
      if (dragstarted === undefined) return;
      if (detectColumnReorder()) {
        setPendingChanges(filteredColumns);
      }
      setDragstarted(undefined);
    },
    [dragstarted, filteredColumns, detectColumnReorder]
  );

  const handleDragEnd = useCallback(() => {
    // This ensures drag state is always cleared, even if dropped outside
    setDragstarted(undefined);
  }, []);

  const handleDragLeave = useCallback(
    (e) => {
      // Only reset if we're leaving the list container itself
      if (e.currentTarget.contains(e.relatedTarget)) return;
      // Optional: You can add a timeout to prevent flickering
      setTimeout(() => {
        if (dragstarted !== undefined) {
          setDragstarted(undefined);
        }
      }, 100);
    },
    [dragstarted]
  );

  useEffect(() => {
    const specHidden = spec?.presentation?.hidden_columns || [];
    const currentHidden = filteredColumns
      .filter(({ show }) => !show)
      .map(({ column }) => column);

    // Check if hidden columns have changed
    const hiddenColumnsUnchanged = isEqual(
      [...currentHidden].sort(),
      [...specHidden].sort()
    );

    // Check if column order has changed by comparing with original state
    const columnOrderUnchanged = isEqual(
      filteredColumns.map(({ column }) => column),
      originalColumnsState.map(({ column }) => column)
    );

    // Disable Apply button only if both hidden columns AND column order are unchanged
    const res = hiddenColumnsUnchanged && columnOrderUnchanged;
    setDisable(res);
  }, [
    spec?.presentation?.hidden_columns,
    filteredColumns,
    originalColumnsState,
  ]);

  return (
    <ToolbarItem
      icon={
        isDarkTheme ? (
          <OrganiserDarkIcon className="toolbar-item-icon" />
        ) : (
          <OrganiserLightIcon className="toolbar-item-icon" />
        )
      }
      label={label}
      open={open}
      className={
        hiddenColumnCount ? "no-code-toolbar-hidden-cols-highlight" : ""
      }
      disabled={disabled}
      handleOpenChange={handleOpenChange}
      step={step}
      width={width}
    >
      <div className="hide-cols-container">
        <Typography.Title level={5} className="hide-cols-title">
          Column Organizer
        </Typography.Title>
        <Input
          placeholder="Find a Column"
          onChange={handleSearch}
          prefix={<SearchOutlined className="hide-cols-search-icon" />}
          bordered
          className="hide-cols-search-input"
          allowClear
          onKeyDown={stopPropagation}
        />
        <List
          className="hide-cols-list"
          itemLayout="vertical"
          dataSource={filteredColumns}
          onDragLeave={handleDragLeave}
          renderItem={({ column, show }, index) => (
            <List.Item
              className={`hide-cols-list-item ${
                index === dragstarted ? "dragging" : ""
              }`}
              key={column}
              draggable
              onDragStart={(e) => dragStart(e, index)}
              onDragOver={(e) => dragOver(e, index)}
              onDrop={handleDrop}
              onDragEnd={handleDragEnd}
            >
              <div className="hide-cols-item-container">
                <div className="hide-cols-item-left">
                  <Switch
                    size="small"
                    checked={show}
                    onChange={() => handleToggle(column)}
                    disabled={isLoading}
                    className="hide-cols-switch"
                  />
                  <Typography.Text className="hide-cols-column-name">
                    {column}
                  </Typography.Text>
                </div>
                <div className="hide-cols-item-right">
                  <DragOutlined
                    className={`hide-cols-drag-icon ${
                      index === dragstarted ? "grabbing" : ""
                    }`}
                  />
                </div>
              </div>
            </List.Item>
          )}
        />
        <div className="hide-cols-actions">
          <div className="hide-cols-actions-left">
            {searchText !== "" && (
              <Button
                onClick={() => handleToggleAll("hide")}
                disabled={
                  filteredColumns.every(({ show }) => !show) || isLoading
                }
                size="middle"
              >
                Hide Searched
              </Button>
            )}
            <Button
              onClick={() => handleToggleAll("show")}
              disabled={filteredColumns.every(({ show }) => show) || isLoading}
              size="middle"
            >
              Show All
            </Button>
          </div>
          <Button
            type="primary"
            onClick={applyChanges}
            disabled={disable || pendingChanges.length === 0 || isLoading}
            loading={isLoading}
            size="middle"
          >
            Apply
          </Button>
        </div>
      </div>
    </ToolbarItem>
  );
}

HideColumns.propTypes = {
  allColumns: PropTypes.arrayOf(PropTypes.string).isRequired,
  spec: PropTypes.object.isRequired,
  updateSpec: PropTypes.func.isRequired,
  isLoading: PropTypes.bool.isRequired,
  disabled: PropTypes.bool.isRequired,
  step: PropTypes.array,
  isDarkTheme: PropTypes.bool,
  handleSetPresentation: PropTypes.func.isRequired,
  width: PropTypes.number,
};

export { HideColumns };
