import { memo } from "react";
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
    handleConnectionNameDesc,
    handleCardClick,
    mappedDataSources,
    dbUsage,
  }) => {
    return (
      <div className="createConnectionSection flex-1 createConnectionSectionDivider overflow-y-auto">
        <Typography className="sectionTitle">Connection Details</Typography>
        <div className="formFieldsWrapper">
          <Form layout="vertical">
            <Form.Item
              label="Name"
              getValueFromEvent={({ target: { value } }) =>
                // collapse any run of 2+ spaces only when followed by non-space
                collapseSpaces(value)
              }
              rules={[
                { required: true, message: "Please enter the connection name" },
                { validator: validateFormFieldName },
              ]}
              required
            >
              <Input
                className="field"
                value={dbSelectionInfo.name}
                onChange={(e) =>
                  handleConnectionNameDesc("name", e.target.value)
                }
              />
            </Form.Item>

            <Form.Item
              label="Description"
              rules={[{ validator: validateFormFieldDescription }]}
            >
              <Input.TextArea
                className="field"
                rows={2}
                value={dbSelectionInfo.description}
                onChange={(e) =>
                  handleConnectionNameDesc("description", e.target.value)
                }
              />
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
  handleConnectionNameDesc: PropTypes.func.isRequired,
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
