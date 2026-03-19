import { useEffect, useState } from "react";
import PropTypes from "prop-types";
import { useImmer } from "use-immer";

import { ToolbarItem } from "../toolbar-item.jsx";
import { Filter } from "../../no-code-ui/filter/filter.jsx";
import {
  FilterDarkIcon,
  FilterLightIcon,
} from "../../../../base/icons/index.js";
import {
  getFilterSpec,
  transformationTypes,
} from "../../no-code-model/helper.js";
import { useTransformIdStore } from "../../../../store/transform-id-store.js";

// CSS for this component is added in the parent component's CSS file (no-code-model)
function DestinationFilter({
  columnDetails,
  spec,
  updateSpec,
  isLoading,
  disabled,
  step,
  isDarkTheme,
  saveTransformation,
  handleDeleteTransformation,
  handleGetColumns,
}) {
  const [filterConditions, setFilterConditions] = useImmer([]);
  const [open, setOpen] = useState(false);
  const [label, setLabel] = useState("Filter");
  const [filterConditionCount, setFilterConditionCount] = useState(0);
  const [conditionType, setConditionType] = useImmer([]);
  const { transformIds } = useTransformIdStore();

  const updateLabel = (newFilterConditionCount) => {
    const plural = newFilterConditionCount > 1 ? "s" : "";
    const newLabel = newFilterConditionCount
      ? `Filtered by ${newFilterConditionCount} condition${plural}`
      : "Filter";
    setLabel(newLabel);
    setFilterConditionCount(newFilterConditionCount);
  };

  useEffect(() => {
    const filterSpec = getFilterSpec(spec?.transform, transformIds?.FILTER);
    updateLabel(filterSpec?.criteria?.length);
    setFilterConditions(filterSpec?.criteria);
  }, [spec?.transform, transformIds?.FILTER]);

  const handleOpenChange = (value) => {
    if (!value) {
      const filterSpec = getFilterSpec(spec?.transform, transformIds?.FILTER);
      setFilterConditions(filterSpec?.criteria);
      setConditionType(
        filterSpec?.criteria?.map((el) => {
          return (
            el?.condition?.logical_operator && el.condition.logical_operator
          );
        })
      );
    }
    if (value) {
      handleGetColumns(transformIds?.FILTER, transformationTypes?.FILTER);
    }
    setOpen(value);
  };

  return (
    <ToolbarItem
      icon={
        isDarkTheme ? (
          <FilterDarkIcon className="toolbar-item-icon" />
        ) : (
          <FilterLightIcon className="toolbar-item-icon" />
        )
      }
      label={label}
      open={open}
      className={
        filterConditionCount !== 0
          ? "no-code-toolbar-filter-conditions-highlight"
          : ""
      }
      disabled={disabled}
      handleOpenChange={handleOpenChange}
      step={step}
    >
      <Filter
        columnDetails={columnDetails}
        spec={spec}
        updateSpec={updateSpec}
        isLoading={isLoading}
        type={"model"}
        filterConditions={filterConditions}
        setFilterConditions={setFilterConditions}
        setOpen={setOpen}
        filterConditionCount={filterConditionCount}
        setFilterConditionCount={setFilterConditionCount}
        conditionType={conditionType}
        setConditionType={setConditionType}
        saveTransformation={saveTransformation}
        handleDeleteTransformation={handleDeleteTransformation}
      />
    </ToolbarItem>
  );
}

DestinationFilter.propTypes = {
  columnDetails: PropTypes.object.isRequired,
  spec: PropTypes.object.isRequired,
  updateSpec: PropTypes.func.isRequired,
  isLoading: PropTypes.bool.isRequired,
  disabled: PropTypes.bool.isRequired,
  step: PropTypes.array,
  isDarkTheme: PropTypes.bool.isRequired,
  saveTransformation: PropTypes.func.isRequired,
  handleDeleteTransformation: PropTypes.func.isRequired,
  handleGetColumns: PropTypes.func.isRequired,
};

export { DestinationFilter };
