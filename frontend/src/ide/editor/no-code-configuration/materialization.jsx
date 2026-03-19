import { useEffect, useState } from "react";
import { Select } from "antd";
import PropTypes from "prop-types";
function Materialization({ spec, updateSpec, disabled }) {
  const [label, setLabel] = useState(spec?.source?.materialization);
  useEffect(() => {
    setLabel(spec.source.materialization);
  }, [spec]);

  const handleChange = (value) => {
    setLabel(value);
    const data = JSON.parse(JSON.stringify(spec));
    data.source.materialization = value;
    updateSpec(data);
  };
  return (
    <Select
      placeholder="Materialization"
      value={label}
      style={{ width: 150, margin: "0px 12px" }}
      onChange={handleChange}
      disabled={disabled}
      options={[
        { value: "TABLE", label: "Table" },
        { value: "VIEW", label: "View" },
      ]}
    />
  );
}

Materialization.propTypes = {
  updateSpec: PropTypes.func,
  spec: PropTypes.object,
  disabled: PropTypes.bool,
};

export { Materialization };
