import { memo, useMemo, useRef, useState, useEffect, useCallback } from "react";
import PropTypes from "prop-types";
import { Space, Button, Dropdown } from "antd";
import { DownOutlined } from "@ant-design/icons";
import debounce from "lodash/debounce";

import "./no-code-toolbar.css";

import { HideColumns } from "./toolbar-items/hide-columns.jsx";
import { Sort } from "./toolbar-items/sort.jsx";
import { DestinationFilter } from "./toolbar-items/destination-filter.jsx";
import { Group } from "./toolbar-items/group.jsx";
import { AddColumn } from "./toolbar-items/add-column.jsx";
import { Merge } from "./toolbar-items/merge.jsx";
import { CombineColumn } from "./toolbar-items/combine-column.jsx";
import { Pivot } from "./toolbar-items/pivot.jsx";
import { useProjectStore } from "../../../store/project-store.js";
import { FindReplace } from "./toolbar-items/find-replace.jsx";
import { useUserStore } from "../../../store/user-store.js";
import { DropDuplicates } from "./toolbar-items/drop-duplicates";
import { Join } from "./toolbar-items/join.jsx";

const ELLIPSIS_BUTTON_WIDTH = 90;
const DEBOUNCE_DELAY = 300;

const NoCodeToolbar = memo(function NoCodeToolbar({
  columnDetails,
  formulaColumns,
  spec,
  updateSpec,
  isLoading,
  handleModalOpen,
  disabled,
  seqOrder,
  openFormula,
  setOpenFormula,
  selectedFormulaCol,
  setSelectedFormulaCol,
  disablePreview,
  modelName,
  allColumnsDetails,
  orginalColumnList,
  saveTransformation,
  handleSetPresentation,
  handleDeleteTransformation,
  handleGetColumns,
  numberOfJoins,
}) {
  const { previewTimeTravel } = useProjectStore();
  const currentTheme = useUserStore(
    (state) => state?.userDetails?.currentTheme
  );

  const containerRef = useRef(null);
  const [hasOverflow, setHasOverflow] = useState(false);
  const [hiddenItems, setHiddenItems] = useState([]);

  // Cache for item widths to prevent measurement issues with hidden items
  const itemWidthsRef = useRef(new Map());
  // Track last container width to avoid unnecessary recalculations
  const lastContainerWidthRef = useRef(null);
  // Track if initial measurement has been done
  const initialMeasurementDoneRef = useRef(false);

  // Memoize the toolbar items to avoid unnecessary re-renders
  const items = useMemo(
    () => [
      // 1. Filter
      {
        key: "filter",
        label: (
          <DestinationFilter
            columnDetails={columnDetails}
            spec={spec}
            updateSpec={updateSpec}
            isLoading={isLoading}
            disabled={disabled}
            step={[seqOrder?.filters]}
            isDarkTheme={currentTheme === "dark"}
            saveTransformation={saveTransformation}
            handleDeleteTransformation={handleDeleteTransformation}
            handleGetColumns={handleGetColumns}
          />
        ),
      },
      // 2. Sort
      {
        key: "sort",
        label: (
          <Sort
            allColumns={columnDetails.columns.sort || []}
            spec={spec}
            updateSpec={updateSpec}
            isLoading={isLoading}
            disabled={disabled}
            step={[seqOrder?.sort]}
            allColumnsDetails={allColumnsDetails}
            isDarkTheme={currentTheme === "dark"}
            saveTransformation={saveTransformation}
            handleSetPresentation={handleSetPresentation}
            handleGetColumns={handleGetColumns}
          />
        ),
      },
      // 3. Add Column
      {
        key: "add-column",
        label: (
          <AddColumn
            allColumns={columnDetails.columns.synthesize || []}
            synthesizeValidationCols={columnDetails.columns.synthesis_validator}
            spec={spec}
            updateSpec={updateSpec}
            isLoading={isLoading}
            disabled={disabled}
            step={[seqOrder?.synthesize]}
            openFormula={openFormula}
            setOpenFormula={setOpenFormula}
            selectedFormulaCol={selectedFormulaCol}
            setSelectedFormulaCol={setSelectedFormulaCol}
            isDarkTheme={currentTheme === "dark"}
            saveTransformation={saveTransformation}
            handleDeleteTransformation={handleDeleteTransformation}
            handleGetColumns={handleGetColumns}
          />
        ),
      },
      // 4. Column Organizer (HideColumns)
      {
        key: "column-organizer",
        label: (
          <HideColumns
            allColumns={columnDetails.columns.visible || []}
            spec={spec}
            updateSpec={updateSpec}
            isLoading={isLoading}
            disabled={disabled}
            step={[seqOrder?.hidden_columns]}
            isDarkTheme={currentTheme === "dark"}
            saveTransformation={saveTransformation}
            handleDeleteTransformation={handleDeleteTransformation}
            handleSetPresentation={handleSetPresentation}
            handleGetColumns={handleGetColumns}
          />
        ),
      },
      // 5. Find and Replace
      {
        key: "find-replace",
        label: (
          <FindReplace
            allColumns={columnDetails.allColumnsDetails || {}}
            spec={spec}
            isLoading={isLoading}
            updateSpec={updateSpec}
            disabled={disabled}
            step={[seqOrder?.find_and_replace]}
            isDarkTheme={currentTheme === "dark"}
            saveTransformation={saveTransformation}
            handleDeleteTransformation={handleDeleteTransformation}
            handleGetColumns={handleGetColumns}
          />
        ),
      },
      // 6. Drop Duplicate
      {
        key: "drop-duplicate",
        label: (
          <DropDuplicates
            tableCols={columnDetails.columns.distinct || []}
            spec={spec}
            updateSpec={updateSpec}
            disabled={disabled}
            step={[seqOrder?.distinct]}
            saveTransformation={saveTransformation}
            isLoading={isLoading}
            handleDeleteTransformation={handleDeleteTransformation}
            handleGetColumns={handleGetColumns}
          />
        ),
      },
      // 7. Combine Column
      {
        key: "combine-column",
        label: (
          <CombineColumn
            isLoading={isLoading}
            allColumns={columnDetails.columns.combine_columns || []}
            spec={spec}
            updateSpec={updateSpec}
            disabled={disabled}
            step={[seqOrder?.combine_columns]}
            isDarkTheme={currentTheme === "dark"}
            saveTransformation={saveTransformation}
            handleDeleteTransformation={handleDeleteTransformation}
            handleGetColumns={handleGetColumns}
          />
        ),
      },
      // 8. Aggregator (Group)
      {
        key: "aggregator",
        label: (
          <Group
            allColumns={columnDetails?.columns?.groups_and_aggregation || []}
            spec={spec}
            updateSpec={updateSpec}
            isLoading={isLoading}
            disabled={disabled}
            modelName={modelName}
            dataType={columnDetails.allColumnsDetails || {}}
            step={[
              seqOrder?.groups,
              seqOrder?.aggregate,
              seqOrder?.aggregate_filter,
              seqOrder?.havings,
            ]}
            isDarkTheme={currentTheme === "dark"}
            saveTransformation={saveTransformation}
            handleDeleteTransformation={handleDeleteTransformation}
            handleGetColumns={handleGetColumns}
          />
        ),
      },
      // 9. Join (NEW TOP-LEVEL MENU)
      {
        key: "join",
        label: (
          <Join
            disabled={disabled}
            handleModalOpen={handleModalOpen}
            isDarkTheme={currentTheme === "dark"}
            step={[seqOrder?.joins]}
            numberOfJoins={numberOfJoins}
          />
        ),
      },
      // 10. Merge
      {
        key: "merge",
        label: (
          <Merge
            disabled={disabled}
            spec={spec}
            isLoading={isLoading}
            updateSpec={updateSpec}
            allColumns={columnDetails || {}}
            step={[seqOrder?.unions]}
            modelName={modelName}
            isDarkTheme={currentTheme === "dark"}
            saveTransformation={saveTransformation}
            handleDeleteTransformation={handleDeleteTransformation}
            handleGetColumns={handleGetColumns}
          />
        ),
      },
      // 11. Pivot
      {
        key: "pivot",
        label: (
          <Pivot
            allColumns={columnDetails.allColumnsDetails || []}
            spec={spec}
            isLoading={isLoading}
            pivotCols={columnDetails.columns.pivot}
            updateSpec={updateSpec}
            disabled={disabled}
            step={[seqOrder?.pivot]}
            isDarkTheme={currentTheme === "dark"}
            saveTransformation={saveTransformation}
            handleDeleteTransformation={handleDeleteTransformation}
            handleGetColumns={handleGetColumns}
          />
        ),
      },
    ],
    [
      columnDetails,
      formulaColumns,
      spec,
      updateSpec,
      isLoading,
      handleModalOpen,
      disabled,
      seqOrder,
      openFormula,
      setOpenFormula,
      selectedFormulaCol,
      setSelectedFormulaCol,
      disablePreview,
      modelName,
      allColumnsDetails,
      orginalColumnList,
      previewTimeTravel,
      handleSetPresentation,
      handleDeleteTransformation,
      handleGetColumns,
      currentTheme,
      numberOfJoins,
    ]
  );

  const calculateVisibleItems = useCallback(
    (transformationItems, forceRecalculate = false) => {
      const container = containerRef.current;
      if (!container || !transformationItems.length) {
        setHiddenItems([]);
        setHasOverflow(false);
        return;
      }

      const containerWidth = container.clientWidth;

      // Skip if container width hasn't changed (prevents feedback loop)
      if (
        !forceRecalculate &&
        lastContainerWidthRef.current === containerWidth &&
        initialMeasurementDoneRef.current
      ) {
        return;
      }

      // Get all toolbar item elements
      const toolbarItemElements = container.querySelectorAll(".toolbar-item");

      // Measure and cache item widths (only for visible items with width > 0)
      transformationItems.forEach((item, index) => {
        const itemElement = toolbarItemElements[index];
        if (itemElement) {
          const rect = itemElement.getBoundingClientRect();
          // Only cache if width > 0 (item is currently visible)
          if (rect.width > 0) {
            itemWidthsRef.current.set(item.key, rect.width);
          }
        }
      });

      // Mark initial measurement as done
      initialMeasurementDoneRef.current = true;
      lastContainerWidthRef.current = containerWidth;

      // Calculate total width needed using cached widths
      const ITEM_SPACING = 5;
      let totalWidthNeeded = 0;
      transformationItems.forEach((item) => {
        const itemWidth = itemWidthsRef.current.get(item.key) || 0;
        totalWidthNeeded += itemWidth + ITEM_SPACING;
      });

      // Check if overflow exists based on cached widths
      const hasOverflowNow = totalWidthNeeded > containerWidth;

      if (!hasOverflowNow) {
        // Only update state if it changed
        if (hiddenItems.length > 0) {
          setHiddenItems([]);
          setHasOverflow(false);
        }
        return;
      }

      // Reserve space for the "More" button
      const availableWidth = containerWidth - ELLIPSIS_BUTTON_WIDTH;

      let accumulatedWidth = 0;
      const hidden = [];

      // Determine which items fit using cached widths
      transformationItems.forEach((item) => {
        const itemWidth = itemWidthsRef.current.get(item.key) || 0;

        if (accumulatedWidth + itemWidth <= availableWidth) {
          accumulatedWidth += itemWidth + ITEM_SPACING;
        } else {
          hidden.push(item);
        }
      });

      // Only update state if hidden items actually changed
      const newHiddenKeys = hidden.map((h) => h.key).join(",");
      const currentHiddenKeys = hiddenItems.map((h) => h.key).join(",");

      if (newHiddenKeys !== currentHiddenKeys) {
        setHiddenItems(hidden);
        setHasOverflow(hidden.length > 0);
      }
    },
    [hiddenItems]
  );

  // Create debounced calculate function for resize events
  const debouncedCalculate = useMemo(
    () =>
      debounce((transformationItems) => {
        calculateVisibleItems(transformationItems, false);
      }, DEBOUNCE_DELAY),
    [calculateVisibleItems]
  );

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    // Reset measurement state when items change
    initialMeasurementDoneRef.current = false;
    lastContainerWidthRef.current = null;

    // Use requestAnimationFrame to ensure DOM is ready for measurement
    const rafId = requestAnimationFrame(() => {
      // Initial calculation with force flag to ensure first measurement
      calculateVisibleItems(items, true);
    });

    // Set up ResizeObserver for container width changes
    const resizeObserver = new ResizeObserver((entries) => {
      // Only trigger if the container's width actually changed
      const entry = entries[0];
      if (entry) {
        const newWidth = entry.contentRect.width;
        if (
          lastContainerWidthRef.current !== null &&
          lastContainerWidthRef.current !== newWidth
        ) {
          debouncedCalculate(items);
        }
      }
    });
    resizeObserver.observe(container);

    // Cleanup
    return () => {
      cancelAnimationFrame(rafId);
      resizeObserver.disconnect();
      debouncedCalculate.cancel();
    };
  }, [items, calculateVisibleItems, debouncedCalculate]);

  // Create a set of hidden item keys for quick lookup
  const hiddenItemKeys = useMemo(
    () => new Set(hiddenItems.map((item) => item.key)),
    [hiddenItems]
  );

  // Render function for the "More" dropdown - memoized to avoid unnecessary re-renders
  const renderMoreDropdown = useCallback(
    () => (
      <div className="no-code-toolbar-more-menu">
        {hiddenItems.map((item) => {
          const freshItem = items.find((i) => i.key === item.key);
          return (
            <div key={item.key} className="no-code-toolbar-more-item">
              {freshItem?.label ?? item.label}
            </div>
          );
        })}
      </div>
    ),
    [hiddenItems, items]
  );

  return (
    <div className="no-code-toolbar-wrapper">
      <div className="no-code-toolbar-content" ref={containerRef}>
        <Space size={5} align="center">
          {items.map((item) => (
            <div
              key={item.key}
              className={`toolbar-item ${
                hiddenItemKeys.has(item.key) ? "toolbar-item-hidden" : ""
              }`}
            >
              {item.label}
            </div>
          ))}
          {hasOverflow && (
            <div className="toolbar-item toolbar-more-item">
              <Dropdown
                dropdownRender={renderMoreDropdown}
                trigger={["click"]}
                placement="bottomRight"
                overlayClassName="no-code-toolbar-dropdown"
              >
                <Button
                  type="text"
                  className="toolbar-ellipsis-button"
                  aria-label="More transformations"
                >
                  More{" "}
                  <span className="toolbar-more-badge">
                    {hiddenItems.length}
                  </span>{" "}
                  <DownOutlined />
                </Button>
              </Dropdown>
            </div>
          )}
        </Space>
      </div>
    </div>
  );
});

NoCodeToolbar.propTypes = {
  columnDetails: PropTypes.object.isRequired,
  formulaColumns: PropTypes.arrayOf(PropTypes.string).isRequired,
  spec: PropTypes.object.isRequired,
  updateSpec: PropTypes.func.isRequired,
  isLoading: PropTypes.bool.isRequired,
  handleModalOpen: PropTypes.func.isRequired,
  disabled: PropTypes.bool.isRequired,
  seqOrder: PropTypes.object.isRequired,
  openFormula: PropTypes.bool,
  setOpenFormula: PropTypes.func,
  selectedFormulaCol: PropTypes.object,
  setSelectedFormulaCol: PropTypes.func,
  disablePreview: PropTypes.func,
  modelName: PropTypes.string.isRequired,
  allColumnsDetails: PropTypes.object.isRequired,
  orginalColumnList: PropTypes.array.isRequired,
  saveTransformation: PropTypes.func.isRequired,
  handleSetPresentation: PropTypes.func.isRequired,
  handleDeleteTransformation: PropTypes.func.isRequired,
  handleGetColumns: PropTypes.func.isRequired,
  numberOfJoins: PropTypes.number.isRequired,
};

NoCodeToolbar.defaultProps = {
  openFormula: false,
  setOpenFormula: () => {},
  selectedFormulaCol: null,
  setSelectedFormulaCol: () => {},
  disablePreview: () => {},
};

export { NoCodeToolbar };
