import { memo, useEffect, useState } from "react";
import PropTypes from "prop-types";
import { Typography, Form, Input, Select, Button } from "antd";
import { PlusOutlined } from "@ant-design/icons";

import {
  collapseSpaces,
  validateFormFieldDescription,
  validateFormFieldName,
} from "./helper";
import DeploymentCredentialsSection from "./DeploymentCredentialsSection";

const EnvGeneralSection = memo(
  ({
    envNameDescInfo,
    handleEnvNameDesChange,
    loading,
    connection,
    connectionList,
    connectionDetails,
    isConnDetailsLoading,
    handleDbTestConnection,
    isTestConnLoading,
    isTestConnSuccess,
    setIsTestConnSuccess,
    inputFields,
    setInputFields,
    handleConnectionChange,
    setIsModalOpen,
    id,
    connType,
    setConnType,
    connectionDataSource,
    isCredentialsRevealed,
    isRevealLoading,
    handleRevealCredentials,
  }) => {
    const [dropdownOptions, setDropdownOptions] = useState([]);
    const [form] = Form.useForm();

    /* sync form inputs when `envNameDescInfo` updates (edit mode) */
    useEffect(() => {
      form.setFieldsValue({
        name: collapseSpaces(envNameDescInfo.name),
        description: envNameDescInfo.description,
        deployment_type: envNameDescInfo.deployment_type,
      });
    }, [envNameDescInfo, form]);

    // build connection dropdown options
    useEffect(() => {
      const baseOptions = connectionList.map((el) => ({
        value: el.id,
        label: (
          <div className="flex-center" key={el.id}>
            <img
              src={el.db_icon}
              alt={el.name}
              height={20}
              width={20}
              className="ml5"
            />
            <span className="ml5">{el.name}</span>
          </div>
        ),
        title: el.datasource_name,
      }));

      const newConnectionOption = {
        label: (
          <Button
            type="link"
            icon={<PlusOutlined />}
            onClick={() => setIsModalOpen(true)}
          >
            New Connection
          </Button>
        ),
        value: "newConnection",
      };

      setDropdownOptions([...baseOptions, newConnectionOption]);
    }, [connectionList, setIsModalOpen]);

    return (
      <div>
        <Typography className="sectionTitle">General</Typography>
        <Form layout="vertical" form={form}>
          <Form.Item
            name="name"
            label="Environment Name"
            required
            rules={[
              { required: true, message: "Please enter the environment name" },
              { validator: validateFormFieldName },
            ]}
          >
            <Input
              onChange={(e) => handleEnvNameDesChange(e.target.value, "name")}
            />
          </Form.Item>

          <Form.Item
            name="description"
            label="Environment Description"
            required
            rules={[
              {
                required: true,
                message: "Please enter the environment description",
              },
              { validator: validateFormFieldDescription },
            ]}
          >
            <Input.TextArea
              rows={2}
              onChange={(e) =>
                handleEnvNameDesChange(e.target.value, "description")
              }
            />
          </Form.Item>

          <Form.Item label="Set Deployment Type" required>
            <Select
              name="deployment_type"
              onChange={(value) =>
                handleEnvNameDesChange(value, "deployment_type")
              }
              value={envNameDescInfo.deployment_type}
              placeholder="Select"
              options={[
                { label: "DEV", value: "DEV" },
                { label: "STG", value: "STG" },
                { label: "PROD", value: "PROD" },
              ]}
            />
          </Form.Item>

          <Form.Item label="Connection" required>
            <Select
              disabled={Boolean(id)}
              placeholder="Select"
              onChange={handleConnectionChange}
              loading={loading}
              value={{ value: connection.id }}
              options={dropdownOptions}
              labelInValue
            />
          </Form.Item>
        </Form>
        {connection?.id && (
          <DeploymentCredentialsSection
            isDataSourceListLoading={isConnDetailsLoading}
            connectionId={connection.id}
            connectionDetails={connectionDetails}
            inputFields={inputFields}
            setInputFields={setInputFields}
            handleTestConnection={handleDbTestConnection}
            isTestConnLoading={isTestConnLoading}
            isTestConnSuccess={isTestConnSuccess}
            setIsTestConnSuccess={setIsTestConnSuccess}
            handleCreateOrUpdate={() => {}}
            isCreateOrUpdateLoading={false}
            shouldWriteDbConnection={false}
            connType={connType}
            setConnType={setConnType}
            dbSelectionInfo={{ datasource_name: connectionDataSource }}
            isCredentialsRevealed={isCredentialsRevealed}
            isRevealLoading={isRevealLoading}
            handleRevealCredentials={handleRevealCredentials}
          />
        )}
      </div>
    );
  }
);

EnvGeneralSection.propTypes = {
  envNameDescInfo: PropTypes.shape({
    name: PropTypes.string,
    description: PropTypes.string,
    deployment_type: PropTypes.string,
  }).isRequired,
  handleEnvNameDesChange: PropTypes.func.isRequired,
  loading: PropTypes.bool.isRequired,
  connection: PropTypes.shape({
    id: PropTypes.string.isRequired,
  }).isRequired,
  connectionList: PropTypes.array.isRequired,
  connectionDetails: PropTypes.object.isRequired,
  isConnDetailsLoading: PropTypes.bool.isRequired,
  handleDbTestConnection: PropTypes.func.isRequired,
  isTestConnLoading: PropTypes.bool.isRequired,
  isTestConnSuccess: PropTypes.bool.isRequired,
  setIsTestConnSuccess: PropTypes.func.isRequired,
  inputFields: PropTypes.object.isRequired,
  setInputFields: PropTypes.func.isRequired,
  handleConnectionChange: PropTypes.func.isRequired,
  setIsModalOpen: PropTypes.func.isRequired,
  id: PropTypes.string.isRequired,
  connType: PropTypes.string,
  setConnType: PropTypes.func,
  connectionDataSource: PropTypes.shape({
    datasource_name: PropTypes.string.isRequired,
  }).isRequired,
  isCredentialsRevealed: PropTypes.bool,
  isRevealLoading: PropTypes.bool,
  handleRevealCredentials: PropTypes.func,
};

EnvGeneralSection.displayName = "EnvGeneralSection";

export default EnvGeneralSection;
