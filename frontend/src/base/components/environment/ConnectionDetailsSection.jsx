import { memo, useEffect, useMemo } from "react";
import PropTypes from "prop-types";
import { Typography, Input, Select, Form } from "antd";
import { debounce } from "lodash";

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
    const [form] = Form.useForm();

    // Debounce the parent state update to avoid stale closure issues
    const debouncedHandleConnectionNameDesc = useMemo(
      () => debounce(handleConnectionNameDesc, 300),
      [handleConnectionNameDesc]
    );

    // Cleanup debounce on unmount
    useEffect(() => {
      return () => {
        debouncedHandleConnectionNameDesc.cancel();
      };
    }, [debouncedHandleConnectionNameDesc]);

    // Populate form values only when connection is first loaded (connectionId changes)
    useEffect(() => {
      form.setFieldsValue({
        name: dbSelectionInfo.name,
        description: dbSelectionInfo.description,
      });
    }, [form, connectionId]);

    return (
      <div className="createConnectionSection flex-1 createConnectionSectionDivider overflow-y-auto">
        <Typography className="sectionTitle">Connection Details</Typography>
        <div className="formFieldsWrapper">
          <Form form={form} layout="vertical">
            <Form.Item
              label="Name"
              name="name"
              rules={[
                { required: true, message: "Please enter the connection name" },
                { validator: validateFormFieldName },
              ]}
              required
            >
              <Input
                className="field"
                onChange={(e) => {
                  const collapsed = collapseSpaces(e.target.value);
                  form.setFieldValue("name", collapsed);
                  debouncedHandleConnectionNameDesc("name", collapsed);
                }}
              />
            </Form.Item>

            <Form.Item
              label="Description"
              name="description"
              rules={[{ validator: validateFormFieldDescription }]}
            >
              <Input.TextArea
                className="field"
                rows={2}
                onChange={(e) =>
                  debouncedHandleConnectionNameDesc(
                    "description",
                    e.target.value
                  )
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
