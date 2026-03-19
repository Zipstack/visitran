import { useEffect, useState } from "react";
import PropTypes from "prop-types";

import { ToolbarItem } from "../toolbar-item";
import { JoinLightIcon, JoinDarkIcon } from "../../../../base/icons";

const Join = ({
  disabled,
  handleModalOpen,
  isDarkTheme,
  step,
  numberOfJoins,
}) => {
  const [label, setLabel] = useState("Join");

  useEffect(() => {
    if (numberOfJoins > 0) {
      const plural = numberOfJoins > 1 ? "s" : "";
      setLabel(`Joined with ${numberOfJoins} table${plural}`);
    } else {
      setLabel("Join");
    }
  }, [numberOfJoins]);

  return (
    <ToolbarItem
      icon={
        isDarkTheme ? (
          <JoinDarkIcon className="toolbar-item-icon" />
        ) : (
          <JoinLightIcon className="toolbar-item-icon" />
        )
      }
      label={label}
      open={false} // This doesn't use the built-in modal
      disabled={disabled}
      handleOpenChange={() => handleModalOpen("joins")} // Open the join modal
      className={
        numberOfJoins > 0 ? "no-code-toolbar-filter-conditions-highlight" : ""
      }
      step={step}
    >
      {/* This content won't be used since we're using the external modal */}
      <div />
    </ToolbarItem>
  );
};

Join.propTypes = {
  disabled: PropTypes.bool,
  handleModalOpen: PropTypes.func.isRequired,
  isDarkTheme: PropTypes.bool,
  step: PropTypes.array,
  numberOfJoins: PropTypes.number.isRequired,
};

Join.defaultProps = {
  disabled: false,
  isDarkTheme: false,
  step: [],
};

export { Join };
