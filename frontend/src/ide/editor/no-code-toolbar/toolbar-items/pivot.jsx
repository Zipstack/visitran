import { useState, useEffect } from "react";
import { Select, Typography, Button, Input } from "antd";
import PropTypes from "prop-types";
import isEqual from "lodash/isEqual.js";

import { useAxiosPrivate } from "../../../../service/axios-service";
import { orgStore } from "../../../../store/org-store";
import { ToolbarItem } from "../toolbar-item";
import { PivotDarkIcon, PivotLightIcon } from "../../../../base/icons";
import { renameMap, useEscapeKey } from "../../../../common/helpers.js";
import { useTransformIdStore } from "../../../../store/transform-id-store.js";
import {
  getPivotSpec,
  transformationTypes,
} from "../../no-code-model/helper.js";

const Pivot = ({
  allColumns,
  updateSpec,
  spec,
  disabled,
  step,
  pivotCols,
  isDarkTheme,
  saveTransformation,
  handleDeleteTransformation,
  isLoading,
  handleGetColumns,
}) => {
  const axios = useAxiosPrivate();
  const { selectedOrgId } = orgStore();
  const label = "Pivot";
  const [colCount, setColCount] = useState(0);
  const [open, setOpen] = useState(false);
  const [summerizedType, setSummerizedType] = useState("");
  const [isModified, setIsModified] = useState(false);
  const [aggregateOptions, setAggregateOptions] = useState([]);
  const emptySkeleton = {
    fill_null: "",
    row: "",
    column: "",
    summerize_by: {
      aggregator: "",
      summerize_column: "",
    },
  };
  const [initialData, setInitialData] = useState(emptySkeleton);
  const [data, setData] = useState(emptySkeleton);
  const [pivotColsOptions, setPivotColsOptions] = useState([]);
  const { transformIds } = useTransformIdStore();

  const handleOpenChange = (value) => {
    if (!value) {
      setData(
        getPivotSpec(spec?.transform, transformIds?.PIVOT) || emptySkeleton
      );
    } else {
      handleGetColumns(transformIds?.PIVOT, transformationTypes?.PIVOT);
    }
    setOpen(value);
  };

  useEscapeKey(open, () => {
    setOpen(false);
    setData(
      getPivotSpec(spec?.transform, transformIds?.PIVOT) || emptySkeleton
    );
  });

  const handleOnChangeData = (value, name) => {
    if (name === "row" || name === "column" || name === "fill_null") {
      setData({ ...data, [name]: value });
    } else {
      if (name === "summerize_column") {
        const type = allColumns[value]?.data_type;
        setSummerizedType(type);
      }
      setData({
        ...data,
        ...(name === "summerize_column" ||
        (name === "aggregator" && typeof data.fill_null === "string")
          ? { fill_null: "" }
          : {}),
        summerize_by: { ...data.summerize_by, [name]: value },
      });
    }
  };

  const handleSave = async () => {
    const pivotData = { ...data };
    if (!pivotData.fill_null) {
      delete pivotData.fill_null;
    }

    let result = {};
    const { column, row, summerize_by } = pivotData;
    if (
      !column &&
      !row &&
      !summerize_by.aggregator &&
      !summerize_by.summerize_column
    ) {
      const body = {
        step_id: transformIds?.PIVOT,
      };
      result = await handleDeleteTransformation(body);
    } else {
      const body = {
        type: "pivot",
        pivot: pivotData,
      };
      result = await saveTransformation(body, transformIds?.PIVOT);
    }
    if (result?.status === "success") {
      setOpen(false);
      setData(emptySkeleton);
      setInitialData(emptySkeleton);
      setColCount(data.column ? 1 : 0);
      updateSpec(result?.spec);
    } else {
      setOpen(true);
    }
  };

  const getAggregateOptions = () => {
    const requestOptions = {
      method: "GET",
      url: `/api/v1/visitran/${selectedOrgId || "default_org"}/aggregations`,
    };
    axios(requestOptions)
      .then((res) => {
        setAggregateOptions(res.data?.aggregations);
      })
      .catch((error) => {
        console.error("Error fetching keywords:", error);
      });
  };

  const disabledClearBtn = (input = data) => {
    for (const key of Object.keys(input)) {
      const type = typeof input[key];
      if (type === "object") {
        if (input[key].aggregator || input[key].summerize_column) {
          return false;
        }
      } else if (input[key]) {
        return false;
      }
    }
    return true;
  };

  function filteringCols() {
    if (summerizedType !== "Number") {
      return aggregateOptions.filter((el) => {
        return ["COUNT", "MAX", "MIN"].includes(el.value);
      });
    } else {
      return aggregateOptions;
    }
  }

  useEffect(() => {
    const renameChecks = renameMap(spec, transformIds?.RENAME_COLUMN);

    const seen = new Set();

    const pivotColsFilter = pivotCols?.filter((el) => {
      return (
        allColumns[el]?.data_type === "String" &&
        ![data.summerize_by.summerize_column, data.row].includes(el)
      );
    });

    const pivotColsMap = pivotColsFilter?.map((column) => {
      const newName = renameChecks?.[column] || column;

      if (seen.has(newName)) return null;

      const label = renameChecks?.[column] ? `${newName} | ${column}` : column;

      seen.add(column);
      seen.add(newName);

      return { label, value: column };
    });

    const result = pivotColsMap?.filter(Boolean);

    setPivotColsOptions(result);
  }, [pivotCols, allColumns, data]);

  useEffect(() => {
    let specData = getPivotSpec(spec?.transform, transformIds?.PIVOT);
    if (open) {
      if (!specData.fill_null) {
        specData = {
          ...specData,
          ...(!specData?.fill_null && { fill_null: "" }),
        };
      }
      setData(specData);
      setInitialData(specData);
    }
    setColCount(specData.column ? 1 : 0);
  }, [spec?.transform, transformIds?.PIVOT, open]);

  useEffect(() => {
    const res = isEqual(initialData, data);
    const { row, column, summerize_by, fill_null } = data;
    if (!res) {
      if (
        !row &&
        !column &&
        !summerize_by.aggregator &&
        !summerize_by.summerize_column &&
        !fill_null
      ) {
        setIsModified(true);
      } else if (!row || !column || !summerize_by.summerize_column) {
        setIsModified(false);
      } else {
        setIsModified(true);
      }
    } else {
      setIsModified(!res);
    }
  }, [initialData, data]);

  useEffect(() => {
    if (open) {
      getAggregateOptions();
    }
  }, [open]);

  function validateByDtype(value, dtype) {
    switch (dtype) {
      case "Number":
        return /^\d*$/.test(value);
      case "String":
        return /[a-zA-Z]/.test(value) && /^[a-zA-Z0-9\s]*$/.test(value);
      case "Date":
        return (
          value === "" ||
          (/[a-zA-Z]/.test(value) && /^[a-zA-Z0-9\s]*$/.test(value))
        );
      default:
        return true;
    }
  }

  return (
    <ToolbarItem
      label={label}
      open={open}
      setOpen={setOpen}
      disabled={disabled}
      handleOpenChange={handleOpenChange}
      icon={
        isDarkTheme ? (
          <PivotDarkIcon className="toolbar-item-icon" />
        ) : (
          <PivotLightIcon className="toolbar-item-icon" />
        )
      }
      className={
        colCount !== 0 ? "no-code-toolbar-filter-conditions-highlight" : ""
      }
      step={step}
    >
      <div className="ml-10 mr-10 " style={{ width: "400px" }}>
        <Typography.Title
          level={5}
          className="m-0 mb-16 group-title
            draggable-title"
        >
          Pivot
        </Typography.Title>
        <div className="m-10">
          <div className="column_field_wrap">
            <Typography>Columns</Typography>
            <Select
              placeholder="Please select"
              onChange={(value) => handleOnChangeData(value, "column")}
              className="width-100"
              value={data.column}
              options={pivotColsOptions}
            />
          </div>
        </div>

        <div className="m-10">
          <div className="column_field_wrap">
            <Typography>Rows</Typography>
            <Select
              placeholder="Please select"
              onChange={(value) => handleOnChangeData(value, "row")}
              className="width-100"
              value={data.row}
              options={pivotCols
                ?.filter(
                  (el) =>
                    el !== data.column &&
                    el !== data.summerize_by.summerize_column
                )
                .map((el) => {
                  return { value: el, label: el };
                })}
            />
          </div>
        </div>

        <div className="m-10">
          <div className="column_field_wrap">
            <Typography>Summarized column</Typography>
            <Select
              placeholder="Please select"
              onChange={(value) =>
                handleOnChangeData(value, "summerize_column")
              }
              className="width-100"
              value={data.summerize_by.summerize_column}
              options={pivotCols
                ?.filter(
                  (el) =>
                    el !== data.column &&
                    el !== data.row &&
                    (allColumns[el]?.data_type === "String" ||
                      allColumns[el]?.data_type === "Number")
                )
                .map((item) => {
                  return { value: item, label: item };
                })}
            />
          </div>
        </div>

        <div className="m-10">
          <div className="column_field_wrap">
            <Typography>Aggregator</Typography>
            <Select
              placeholder="Please select"
              onChange={(value) => handleOnChangeData(value, "aggregator")}
              className="width-100"
              value={data?.summerize_by?.aggregator}
              options={filteringCols()}
            />
          </div>
        </div>

        <div className="m-10">
          <div className="column_field_wrap">
            <Typography>Non Derived Value</Typography>

            {/* Aggregator selected: always Number input */}
            {data?.summerize_by?.aggregator ? (
              <Input
                placeholder="Enter number"
                className="width-100"
                onChange={(e) => {
                  const value = e.target.value;
                  if (value === "" || validateByDtype(value, "Number")) {
                    handleOnChangeData(value, "fill_null");
                  }
                }}
                onKeyDown={(e) => e.stopPropagation()}
                value={data?.fill_null}
              />
            ) : summerizedType === "Date" ? (
              <Input
                placeholder="dd-mm-yyyy"
                className="width-100"
                onChange={(e) => {
                  if (e.target.value.length <= 10) {
                    const raw = e.target.value.replace(/[^\d]/g, "");
                    let formatted = raw;

                    if (raw.length >= 3 && raw.length <= 4) {
                      formatted = raw.slice(0, 2) + "-" + raw.slice(2);
                    } else if (raw.length > 4 && raw.length <= 8) {
                      formatted =
                        raw.slice(0, 2) +
                        "-" +
                        raw.slice(2, 4) +
                        "-" +
                        raw.slice(4);
                    }

                    formatted = formatted.slice(0);
                    const isValid = validateByDtype(formatted, "Date");
                    if (isValid || formatted.length <= 10) {
                      handleOnChangeData(formatted, "fill_null");
                    }
                  }
                }}
                onKeyDown={(e) => e.stopPropagation()}
                value={data?.fill_null}
              />
            ) : (
              <Input
                placeholder="Enter value"
                className="width-100"
                onChange={(e) => {
                  const value = e.target.value;
                  if (value === "" || validateByDtype(value, summerizedType)) {
                    handleOnChangeData(value, "fill_null");
                  }
                }}
                onKeyDown={(e) => e.stopPropagation()}
                value={data?.fill_null}
              />
            )}
          </div>
        </div>

        <div className="flex-end-container">
          <Button
            danger
            variant="text"
            type="primary"
            onClick={() => {
              setData(emptySkeleton);
              setIsModified(true);
            }}
            disabled={disabledClearBtn()}
          >
            Clear
          </Button>
          <Button onClick={() => setOpen(false)} style={{ margin: "0px 5px" }}>
            Cancel
          </Button>
          <Button
            type="primary"
            onClick={handleSave}
            loading={isLoading}
            disabled={!isModified}
          >
            Save
          </Button>
        </div>
      </div>
    </ToolbarItem>
  );
};
Pivot.propTypes = {
  disabled: PropTypes.bool.isRequired,
  spec: PropTypes.object.isRequired,
  updateSpec: PropTypes.func.isRequired,
  allColumns: PropTypes.object.isRequired,
  step: PropTypes.array.isRequired,
  pivotCols: PropTypes.array,
  isDarkTheme: PropTypes.bool.isRequired,
  saveTransformation: PropTypes.func.isRequired,
  handleDeleteTransformation: PropTypes.func.isRequired,
  isLoading: PropTypes.bool.isRequired,
  handleGetColumns: PropTypes.func.isRequired,
};

export { Pivot };
