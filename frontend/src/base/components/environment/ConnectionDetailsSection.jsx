import { memo, useEffect } from "react";
import PropTypes from "prop-types";
import { Typography, Input, Select, Form } from "antd";

import ConnectionUsageSection from "./ConnectionUsageSection";
import {
  collapseSpaces,
  validateFormFieldDescription,
  validateFormFieldName,
} from "./helper";

const ConnectionDetailsSection = memo(
  ({
    connectionId,
    dbSelectionInfo,
    connectionDetailsForm,
    handleCardClick,
    mappedDataSources,
    dbUsage,
  }) => {
    // Populate form values when connection data is loaded
    // Apply collapseSpaces since setFieldsValue bypasses the normalize prop
    useEffect(() => {
      connectionDetailsForm.setFieldsValue({
        name: collapseSpaces(dbSelectionInfo.name || ""),
        description: dbSelectionInfo.description,
      });
    }, [
      connectionDetailsForm,
      connectionId,
      dbSelectionInfo.name,
      dbSelectionInfo.description,
    ]);

    return (
      <div className="createConnectionSection flex-1 createConnectionSectionDivider overflow-y-auto">
        <Typography className="sectionTitle">Connection Details</Typography>
        <div className="formFieldsWrapper">
          <Form form={connectionDetailsForm} layout="vertical">
            <Form.Item
              label="Name"
              name="name"
              normalize={collapseSpaces}
              rules={[
                { required: true, message: "Please enter the connection name" },
                { validator: validateFormFieldName },
              ]}
              required
            >
              <Input className="field" />
            </Form.Item>

            <Form.Item
              label="Description"
              name="description"
              rules={[{ validator: validateFormFieldDescription }]}
            >
              <Input.TextArea className="field" rows={2} />
            </Form.Item>

            <Form.Item label="Database" required>
              <Select
                placeholder="Select database"
                className="fieldSelect"
                value={dbSelectionInfo.datasource_name}
                onChange={handleCardClick}
                options={mappedDataSources}
                disabled={Boolean(connectionId)}
              />
            </Form.Item>
          </Form>
        </div>

        {connectionId && (
          <ConnectionUsageSection
            dbUsage={dbUsage}
            connectionId={connectionId}
          />
        )}
      </div>
    );
  }
);

ConnectionDetailsSection.propTypes = {
  connectionId: PropTypes.string,
  dbSelectionInfo: PropTypes.shape({
    datasource_name: PropTypes.string,
    name: PropTypes.string,
    description: PropTypes.string,
    icon: PropTypes.string,
  }).isRequired,
  connectionDetailsForm: PropTypes.object.isRequired,
  handleCardClick: PropTypes.func.isRequired,
  mappedDataSources: PropTypes.array.isRequired,
  dbUsage: PropTypes.shape({
    projects: PropTypes.array,
    environment: PropTypes.array,
  }).isRequired,
};

ConnectionDetailsSection.defaultProps = {
  connectionId: "",
};

ConnectionDetailsSection.displayName = "ConnectionDetailsSection";

export default ConnectionDetailsSection;
