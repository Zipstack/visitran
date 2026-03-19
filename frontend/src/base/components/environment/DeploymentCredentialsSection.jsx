import { memo, useState, useEffect, useMemo, useRef } from "react";
import { Typography, Button, Empty, Radio, Tooltip } from "antd";
import { EyeOutlined } from "@ant-design/icons";
import PropTypes from "prop-types";

import { RjsfFormLayout } from "../../rjsf-form-layout/RjstFormLayout";
import { SpinnerLoader } from "../../../widgets/spinner_loader";

import "./environment.css";

const TitleBlock = () => (
  <div>
    <Typography className="sectionTitle">Deployment Credentials</Typography>
  </div>
);

const DeploymentCredentialsSection = memo(
  ({
    isDataSourceListLoading,
    connectionId,
    connectionDetails,
    inputFields,
    setInputFields,
    handleTestConnection,
    isTestConnLoading,
    isTestConnSuccess,
    setIsTestConnSuccess,
    handleCreateOrUpdate,
    isCreateOrUpdateLoading,
    isEncryptionLoading = false,
    dbSelectionInfo,
    shouldWriteDbConnection = true,
    connType,
    setConnType,
    hasDetailsChanged = false,
    isCredentialsRevealed = false,
    isRevealLoading = false,
    handleRevealCredentials,
  }) => {
    const [uiSchema, setUiSchema] = useState({});
    const [schema, setSchema] = useState();
    const [originalConnectionData, setOriginalConnectionData] = useState({});
    const [formChangeCounter, setFormChangeCounter] = useState(0); // Add counter to force re-evaluation
    const hasCapturedOriginalRef = useRef(false);

    const options = [
      { label: "URL", value: "url" },
      { label: "Host", value: "host" },
    ];

    // Store original connection data when component mounts or connectionId changes
    useEffect(() => {
      if (
        connectionId &&
        Object.keys(inputFields).length > 0 &&
        !hasCapturedOriginalRef.current
      ) {
        // Store a deep copy of the original data only once
        const originalData = JSON.parse(JSON.stringify(inputFields));
        setOriginalConnectionData(originalData);
        hasCapturedOriginalRef.current = true;
      } else if (!connectionId) {
        setOriginalConnectionData({});
        hasCapturedOriginalRef.current = false;
      }
    }, [connectionId, inputFields]); // Keep inputFields to detect when data is first loaded

    // Check if there are any changes in the form data
    const hasChanges = useMemo(() => {
      if (!connectionId) {
        // For new connections or environment creation, check if any field has a value
        // This includes when connection details are loaded from an existing connection
        return Object.values(inputFields).some(
          (value) => value !== undefined && value !== null && value !== ""
        );
      }

      // For existing connections, we need both current and original data
      if (Object.keys(originalConnectionData).length === 0) {
        return false;
      }

      // For existing connections, compare with original data
      const currentData = { ...inputFields };
      const originalData = { ...originalConnectionData };

      // Remove connection_type from comparison as it's handled separately
      delete currentData.connection_type;
      delete originalData.connection_type;

      // Simple comparison - check if any field is different
      return JSON.stringify(currentData) !== JSON.stringify(originalData);
    }, [connectionId, inputFields, originalConnectionData, formChangeCounter]); // Add formChangeCounter to dependencies

    // Check if form has meaningful data for testing (independent of whether data changed)
    const hasValidData = useMemo(() => {
      return Object.values(inputFields).some(
        (value) => value !== undefined && value !== null && value !== ""
      );
    }, [inputFields]);

    const handleChange = ({ formData }) => {
      setInputFields(formData);
      setFormChangeCounter((prev) => prev + 1);
      if (isTestConnSuccess) {
        setIsTestConnSuccess(false);
      }
    };

    const handleFormSubmit = ({ formData }) => {
      setInputFields(formData);
      handleTestConnection(formData);
    };

    const handleConnTypeChange = ({ target: { value } }) => {
      setConnType(value);
      if (!connectionId) {
        setInputFields({});
      }
      const updatedProperties = { ...connectionDetails.properties };
      delete updatedProperties["connection_type"];
      const updatedRequired =
        value === "url"
          ? ["connection_url"]
          : connectionDetails?.required?.filter(
              (el) =>
                !["connection_url", "schema", "connection_type"].includes(el)
            );

      setSchema({
        type: "object",
        properties: updatedProperties,
        required: updatedRequired,
      });

      const ui = {};
      Object.keys(updatedProperties).forEach((key) => {
        ui[key] = {
          "ui:disabled":
            value === "url"
              ? key !== "connection_url"
              : key === "connection_url",
        };
      });
      setUiSchema({ ...ui, schema: { "ui:disabled": false } });
    };

    useEffect(() => {
      if (
        ["postgres", "snowflake"].includes(dbSelectionInfo?.datasource_name)
      ) {
        handleConnTypeChange({ target: { value: connType } });
      } else {
        setSchema(connectionDetails);
      }
    }, [dbSelectionInfo?.datasource_name, connectionDetails, connType]);

    if (isDataSourceListLoading) {
      return (
        <div className="deploymentCredSection flex-1">
          <div className="height-100 flex-direction-column">
            <TitleBlock />
            <div className="flex-1 center spinnerContainer">
              <SpinnerLoader />
            </div>
          </div>
        </div>
      );
    }

    if (Object.keys(connectionDetails).length === 0) {
      return (
        <div className="deploymentCredSection flex-1">
          <div className="height-100 flex-direction-column">
            <TitleBlock />
            <div className="flex-1 center">
              <Empty description="Please select the database to configure." />
            </div>
          </div>
        </div>
      );
    }

    return (
      <div
        className={`deploymentCredSection flex-1 overflow-y-auto ${
          !shouldWriteDbConnection && "deploymentCredSectionBorder"
        }`}
      >
        <div className="credentialsTitleRow">
          <TitleBlock />
          {connectionId && !isCredentialsRevealed && (
            <Tooltip title="Reveal stored credentials">
              <Button
                size="small"
                icon={<EyeOutlined />}
                loading={isRevealLoading}
                onClick={handleRevealCredentials}
              >
                Reveal
              </Button>
            </Tooltip>
          )}
        </div>
        {["postgres", "snowflake"].includes(
          dbSelectionInfo?.datasource_name
        ) && (
          <Radio.Group
            options={options}
            onChange={handleConnTypeChange}
            value={connType}
          />
        )}
        <RjsfFormLayout
          schema={schema}
          formData={inputFields}
          handleChange={handleChange}
          handleSubmit={handleFormSubmit}
          uiSchema={uiSchema}
        >
          <div className="buttonContainer">
            <Button
              block
              htmlType="submit"
              loading={isTestConnLoading}
              disabled={!hasValidData}
            >
              Test Connection
            </Button>

            {shouldWriteDbConnection && (
              <Button
                block
                onClick={handleCreateOrUpdate}
                type="primary"
                className="primary_button_style createConnectionBtn"
                loading={isCreateOrUpdateLoading || isEncryptionLoading}
                disabled={
                  !(
                    isTestConnSuccess ||
                    (connectionId && hasDetailsChanged && !hasChanges)
                  ) || isEncryptionLoading
                }
              >
                {connectionId ? "Update" : "Create"}
              </Button>
            )}
          </div>
        </RjsfFormLayout>
      </div>
    );
  }
);

DeploymentCredentialsSection.propTypes = {
  isDataSourceListLoading: PropTypes.bool.isRequired,
  connectionId: PropTypes.string,
  connectionDetails: PropTypes.object.isRequired,
  inputFields: PropTypes.object.isRequired,
  setInputFields: PropTypes.func.isRequired,
  handleTestConnection: PropTypes.func.isRequired,
  isTestConnLoading: PropTypes.bool.isRequired,
  isTestConnSuccess: PropTypes.bool.isRequired,
  setIsTestConnSuccess: PropTypes.func.isRequired,
  handleCreateOrUpdate: PropTypes.func.isRequired,
  isCreateOrUpdateLoading: PropTypes.bool.isRequired,
  isEncryptionLoading: PropTypes.bool,
  shouldWriteDbConnection: PropTypes.bool,
  connType: PropTypes.string,
  setConnType: PropTypes.func,
  dbSelectionInfo: PropTypes.object.isRequired,
  hasDetailsChanged: PropTypes.bool,
  isCredentialsRevealed: PropTypes.bool,
  isRevealLoading: PropTypes.bool,
  handleRevealCredentials: PropTypes.func,
};

DeploymentCredentialsSection.defaultProps = {
  connectionId: "",
};

DeploymentCredentialsSection.displayName = "DeploymentCredentialsSection";

export default DeploymentCredentialsSection;
