import { useState, useEffect } from "react";
import { CopyOutlined } from "@ant-design/icons";
import { Typography, Button, Select } from "antd";
import PropTypes from "prop-types";
import cloneDeep from "lodash/cloneDeep.js";
import isEqual from "lodash/isEqual.js";

import { ToolbarItem } from "../toolbar-item.jsx";
import { useTransformIdStore } from "../../../../store/transform-id-store.js";
import {
  getDistinctSpec,
  transformationTypes,
} from "../../no-code-model/helper.js";

const DropDuplicates = ({
  spec,
  disabled,
  tableCols,
  updateSpec = () => {},
  step,
  saveTransformation,
  isLoading,
  handleDeleteTransformation,
  handleGetColumns,
}) => {
  const [open, setOpen] = useState(false);
  const [value, setValue] = useState([]);
  const [isModified, setIsModified] = useState(false);
  const [label, setLabel] = useState("Drop Duplicates");
  const [colCount, setColCount] = useState(0);
  const { transformIds } = useTransformIdStore();

  const handleOpenChange = (value) => {
    setOpen(value);
    if (!value) {
      setValue(
        getDistinctSpec(spec?.transform, transformIds?.DISTINCT)?.columns
      );
    } else {
      handleGetColumns(transformIds?.DISTINCT, transformationTypes?.DISTINCT);
    }
  };
  const updateLabel = (count) => {
    const plural = count > 1 ? "s" : "";
    const newLabel = count
      ? `Droped duplicates by ${count} column${plural}`
      : "Drop Duplicates";
    setLabel(newLabel);
  };
  const handleSave = async () => {
    let result = {};
    if (value?.length === 0) {
      const body = {
        step_id: transformIds?.DISTINCT,
      };
      result = await handleDeleteTransformation(body);
    } else {
      const body = {
        type: "distinct",
        distinct: { columns: value },
      };
      result = await saveTransformation(body, transformIds?.DISTINCT);
    }

    if (result?.status === "success") {
      updateSpec(result?.spec);
      setOpen(false);
      updateLabel(value.length);
      setColCount(value.length);
    } else {
      setOpen(true);
    }
  };
  useEffect(() => {
    const newDistinctCols = cloneDeep(
      getDistinctSpec(spec?.transform, transformIds?.DISTINCT)?.columns
    );
    setValue(newDistinctCols);
    updateLabel(newDistinctCols?.length);
    setColCount(newDistinctCols?.length);
  }, [spec?.transform, transformIds?.DISTINCT]);

  useEffect(() => {
    const res = isEqual(
      value,
      getDistinctSpec(spec?.transform, transformIds?.DISTINCT)?.columns
    );
    setIsModified(res);
  }, [value]);

  const handleCancelBtn = () => {
    setOpen(false);
    setValue(getDistinctSpec(spec?.transform, transformIds?.DISTINCT))?.columns;
  };

  return (
    <ToolbarItem
      icon={<CopyOutlined style={{ color: "var(--icons-color)" }} />}
      label={label}
      open={open}
      setOpen={setOpen}
      disabled={disabled}
      handleOpenChange={handleOpenChange}
      className={
        colCount !== 0 ? "no-code-toolbar-dropdupe-cols-highlight" : ""
      }
      step={step}
    >
      <div className="ml-10 mr-10">
        <Typography.Title
          level={5}
          className="m-0 mb-16 group-title draggable-title"
        >
          Drop Duplicates
        </Typography.Title>
        <Select
          mode="multiple"
          placeholder="Column Names"
          maxTagCount="responsive"
          allowClear
          style={{ width: "100%" }}
          className="mb-16"
          value={value}
          onChange={(vals) => setValue(vals)}
          options={tableCols.map((el) => {
            return { label: el, value: el };
          })}
        />
        <div className="flex-end-container">
          <Button onClick={handleCancelBtn}>Cancel</Button>
          <Button
            className="ml-10"
            type="primary"
            onClick={handleSave}
            disabled={isModified}
            loading={isLoading}
          >
            Save
          </Button>
        </div>
      </div>
    </ToolbarItem>
  );
};

DropDuplicates.propTypes = {
  disabled: PropTypes.bool.isRequired,
  spec: PropTypes.object.isRequired,
  updateSpec: PropTypes.func,
  tableCols: PropTypes.array,
  step: PropTypes.array,
  isLoading: PropTypes.bool.isRequired,
  saveTransformation: PropTypes.func.isRequired,
  handleDeleteTransformation: PropTypes.func.isRequired,
  handleGetColumns: PropTypes.func.isRequired,
};

export { DropDuplicates };
