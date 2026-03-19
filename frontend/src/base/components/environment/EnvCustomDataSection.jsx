import { memo } from "react";
import PropTypes from "prop-types";
import { Typography } from "antd";

import CustomDataList from "./CustomDataList";
import EnvironmentUsage from "./EnvironmentUsage";

const EnvCustomDataSection = memo(
  ({
    actionState,
    customData,
    AddnewEntry,
    handleCustomFieldChange,
    disabledAddCustomBtn,
    handleDelete,
    projListDep,
    id,
  }) => {
    return (
      <div>
        <Typography.Title level={5} className="sectionTitle">
          Custom Data
        </Typography.Title>

        {/* Render our CustomDataList sub-component */}
        <CustomDataList
          customData={customData}
          actionState={actionState}
          handleCustomFieldChange={handleCustomFieldChange}
          handleDelete={handleDelete}
          AddnewEntry={AddnewEntry}
          disabledAddCustomBtn={disabledAddCustomBtn}
        />

        {/* Render our EnvironmentUsage sub-component */}
        <EnvironmentUsage id={id} projListDep={projListDep} />
      </div>
    );
  }
);

EnvCustomDataSection.propTypes = {
  actionState: PropTypes.string.isRequired,
  customData: PropTypes.array.isRequired,
  AddnewEntry: PropTypes.func.isRequired,
  handleCustomFieldChange: PropTypes.func.isRequired,
  disabledAddCustomBtn: PropTypes.bool.isRequired,
  handleDelete: PropTypes.func.isRequired,
  projListDep: PropTypes.array.isRequired,
  id: PropTypes.string.isRequired,
};

EnvCustomDataSection.displayName = "EnvCustomDataSection";

export default EnvCustomDataSection;
