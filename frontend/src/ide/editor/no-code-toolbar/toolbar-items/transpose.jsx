import { Button } from "antd";
import PropTypes from "prop-types";
import { useState } from "react";
import { produce } from "immer";

// CSS for this component is added in the parent component's CSS file (no-code-model)
function Transpose({ ColumnList, updateSpec, spec, disabled }) {
  const [isTransposeActive, setIsTransposeActive] = useState(false);
  const handleTranspose = () => {
    setIsTransposeActive((prev) => {
      const newState = !prev;
      const newSpec = produce(spec, (draft) => {
        draft.transform.transpose = {
          state: newState,
          columnList: ColumnList,
        };
      });

      updateSpec(newSpec);

      return newState;
    });
  };
  return (
    <Button
      type="text"
      onClick={handleTranspose}
      className={`toolbar-item bg-transparent p-0 ${
        isTransposeActive ? "active" : ""
      }`}
      disabled={disabled}
    >
      {isTransposeActive ? "Transpose Active" : "Transpose Table"}
    </Button>
  );
}

Transpose.propTypes = {
  disabled: PropTypes.bool.isRequired,
  spec: PropTypes.object.isRequired,
  updateSpec: PropTypes.func.isRequired,
  ColumnList: PropTypes.array.isRequired,
};

export { Transpose };
