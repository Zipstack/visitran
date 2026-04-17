import { useEffect, useState, useCallback, useMemo } from "react";
import Cookies from "js-cookie";
import PropTypes from "prop-types";
import { Form } from "antd";

import { useAxiosPrivate } from "../../../service/axios-service.js";
import { orgStore } from "../../../store/org-store.js";
import encryptionService from "../../../service/encryption-service.js";
import ConnectionDetailsSection from "./ConnectionDetailsSection.jsx";
import DeploymentCredentialsSection from "./DeploymentCredentialsSection.jsx";
import {
  fetchDataSources,
  fetchDataSourceFields,
  createConnectionApi,
  updateConnectionApi,
  fetchSingleConnection,
  fetchConnectionUsage,
  revealConnectionCredentials,
  testConnectionApi,
} from "./environment-api-service.js";
import "./environment.css";
import { useNotificationService } from "../../../service/notification-service.js";
import { collapseSpaces } from "./helper";

const CreateConnection = ({
  setIsModalOpen,
  setConnectionDbType,
  getAllConnection,
  connectionId,
  setConnectionId,
}) => {
  const axiosRef = useAxiosPrivate();
  const { selectedOrgId } = orgStore();
  const csrfToken = Cookies.get("csrftoken");

  // States
  const [dataSources, setDataSources] = useState([]);
  const [isDataSourceListLoading, setIsDataSourceListLoading] = useState(false);
  const [connectionDetails, setConnectionDetails] = useState({});
  const [dbSelectionInfo, setDbSelectionInfo] = useState({
    datasource_name: "",
    name: "",
    description: "",
    icon: "",
  });
  const [connType, setConnType] = useState("url");
  // eslint-disable-next-line no-unused-vars
  const [connectionKey, setConnectionKey] = useState(0); // Add key to force re-fetch
  const [originalDbSelectionInfo, setOriginalDbSelectionInfo] = useState(null);
  const [dbUsage, setDbUsage] = useState({ projects: [], environment: [] });
  const [isTestConnLoading, setIsTestConnLoading] = useState(false);
  const [isTestConnSuccess, setIsTestConnSuccess] = useState(false);
  const [inputFields, setInputFields] = useState({});
  const [isCreateOrUpdateLoading, setIsCreateOrUpdateLoading] = useState(false);
  const [isEncryptionLoading, setIsEncryptionLoading] = useState(true);
  const [isCredentialsRevealed, setIsCredentialsRevealed] = useState(false);
  const [isRevealLoading, setIsRevealLoading] = useState(false);
  const { notify } = useNotificationService();
  const [connectionDetailsForm] = Form.useForm();

  // Watch form fields reactively for hasDetailsChanged comparison
  const formName = Form.useWatch("name", connectionDetailsForm);
  const formDescription = Form.useWatch("description", connectionDetailsForm);

  // Initialize encryption service
  useEffect(() => {
    const initEncryption = async () => {
      setIsEncryptionLoading(true);
      try {
        const orgId = selectedOrgId || "default_org";
        await encryptionService.initialize(orgId);
      } catch (error) {
        console.warn(
          "⚠️  Encryption service not available, proceeding without encryption:",
          error
        );
        notify({
          type: "warning",
          message: "Encryption Unavailable",
          description:
            "Your data will be sent without encryption. Please contact support if this persists.",
        });
      } finally {
        setIsEncryptionLoading(false);
      }
    };

    initEncryption();
  }, [selectedOrgId, notify]);

  // do not add axiosRef or toast in deps
  const getAllConnectionsCallback = useCallback(async () => {
    setIsDataSourceListLoading(true);
    try {
      const ds = await fetchDataSources(axiosRef, selectedOrgId);
      setDataSources(ds);
    } catch (error) {
      console.error(error);
      notify({ error });
    } finally {
      setIsDataSourceListLoading(false);
    }
  }, [selectedOrgId]);

  useEffect(() => {
    getAllConnectionsCallback();
  }, [getAllConnectionsCallback]);

  const getConnectionFields = useCallback(async () => {
    if (!dbSelectionInfo.datasource_name) return;
    try {
      const details = await fetchDataSourceFields(
        axiosRef,
        selectedOrgId,
        dbSelectionInfo.datasource_name
      );
      setConnectionDetails(details);
    } catch (error) {
      console.error(error);
      notify({ error });
    }
  }, [dbSelectionInfo.datasource_name, selectedOrgId]);

  useEffect(() => {
    getConnectionFields();
  }, [getConnectionFields]);

  const hasDetailsChanged = useMemo(() => {
    if (!connectionId || !originalDbSelectionInfo) return false;
    return (
      formName !== originalDbSelectionInfo.name ||
      formDescription !== originalDbSelectionInfo.description
    );
  }, [connectionId, formName, formDescription, originalDbSelectionInfo]);

  const handleCreateOrUpdate = useCallback(async () => {
    setIsCreateOrUpdateLoading(true);
    try {
      // Get name and description directly from the form (source of truth)
      const { name, description } = connectionDetailsForm.getFieldsValue();

      // Prepare connection data
      const connectionData = {
        ...dbSelectionInfo,
        name,
        description,
        connection_details: {
          ...inputFields,
          ...(["postgres", "snowflake"].includes(
            dbSelectionInfo.datasource_name
          ) && {
            schema: inputFields.schema || "",
            connection_type: connType,
          }),
        },
        ...(connectionId && hasDetailsChanged && { metadata_only: true }),
      };

      // Encrypt sensitive fields if encryption service is available
      if (encryptionService.isAvailable()) {
        connectionData.connection_details =
          await encryptionService.encryptSensitiveFields(
            connectionData.connection_details
          );
      } else {
        console.warn(
          "⚠️  Encryption service not available, sending data without encryption"
        );
      }

      let res;
      if (!connectionId) {
        // create
        res = await createConnectionApi(
          axiosRef,
          selectedOrgId,
          csrfToken,
          connectionData
        );
        if (res.status === 200) {
          const newConnection = res.data?.data || {};
          setConnectionId({ id: newConnection.id });
          setConnectionDbType(newConnection.datasource_name);
          getAllConnection(newConnection);
          setIsModalOpen(false);
          notify({
            type: "success",
            message: "Connection Created Successfully",
          });
        }
      } else {
        // update
        res = await updateConnectionApi(
          axiosRef,
          selectedOrgId,
          csrfToken,
          connectionId,
          connectionData
        );
        if (res.status === 200) {
          notify({
            type: "success",
            message: "Success",
            description: "Connection updated successfully.",
          });
          getAllConnection(res.data?.data);
          setIsModalOpen(false);
        }
      }
    } catch (error) {
      console.error(error);
      notify({ error });
    } finally {
      setIsCreateOrUpdateLoading(false);
    }
  }, [
    connectionId,
    connectionDetailsForm,
    hasDetailsChanged,
    dbSelectionInfo,
    inputFields,
    connType,
    selectedOrgId,
    csrfToken,
    axiosRef,
    getAllConnection,
    setIsModalOpen,
    notify,
  ]);

  const getSingleConnectionDetails = useCallback(async () => {
    if (!connectionId) return;
    try {
      const data = await fetchSingleConnection(
        axiosRef,
        selectedOrgId,
        connectionId
      );
      const {
        name,
        description,
        datasource_name,
        connection_details,
        db_icon,
      } = data;
      const selectionInfo = {
        datasource_name,
        name,
        description,
        icon: db_icon,
      };
      setDbSelectionInfo(selectionInfo);
      setOriginalDbSelectionInfo({
        ...selectionInfo,
        name: collapseSpaces(selectionInfo.name || ""),
      });
      // Process connection details to handle JSON objects for textarea fields
      const processedConnectionDetails = { ...connection_details };

      // For BigQuery, ensure credentials are a string for the textarea
      if (
        datasource_name === "bigquery" &&
        processedConnectionDetails.credentials
      ) {
        if (typeof processedConnectionDetails.credentials === "object") {
          processedConnectionDetails.credentials = JSON.stringify(
            processedConnectionDetails.credentials,
            null,
            2
          );
        }
      }

      setInputFields(processedConnectionDetails);
      if (["postgres", "snowflake"].includes(datasource_name)) {
        setConnType(
          connection_details.connection_type
            ? connection_details.connection_type
            : "host"
        );
      }
    } catch (error) {
      console.error(error);
      notify({ error });
    }
  }, [connectionId, selectedOrgId]);

  const getConenctionUsage = useCallback(async () => {
    if (!connectionId) return;
    try {
      const usage = await fetchConnectionUsage(
        axiosRef,
        selectedOrgId,
        connectionId
      );
      const { projects, environment } = usage;
      setDbUsage({ projects, environment });
    } catch (error) {
      console.error(error);
      notify({ error });
    }
  }, [connectionId, selectedOrgId]);

  const handleRevealCredentials = useCallback(async () => {
    if (!connectionId || isCredentialsRevealed) return;
    setIsRevealLoading(true);
    try {
      const credentials = await revealConnectionCredentials(
        axiosRef,
        selectedOrgId,
        connectionId
      );
      const processedCredentials = { ...credentials };
      // For BigQuery, ensure credentials are a string for the textarea
      if (
        dbSelectionInfo.datasource_name === "bigquery" &&
        processedCredentials.credentials
      ) {
        if (typeof processedCredentials.credentials === "object") {
          processedCredentials.credentials = JSON.stringify(
            processedCredentials.credentials,
            null,
            2
          );
        }
      }
      setInputFields(processedCredentials);
      setIsCredentialsRevealed(true);
    } catch (error) {
      console.error(error);
      notify({ error });
    } finally {
      setIsRevealLoading(false);
    }
  }, [
    connectionId,
    selectedOrgId,
    isCredentialsRevealed,
    dbSelectionInfo.datasource_name,
  ]);

  useEffect(() => {
    if (connectionId) {
      // Always fetch fresh data when component mounts or connectionId is set
      // This ensures API is called even if connectionId is the same
      getSingleConnectionDetails();
      getConenctionUsage();
      setIsCredentialsRevealed(false);
    } else {
      setDbSelectionInfo({
        datasource_name: "",
        name: "",
        description: "",
        icon: "",
      });
      setOriginalDbSelectionInfo(null);
      setConnectionDetails({});
      setInputFields({});
      setIsCredentialsRevealed(false);
    }
  }, [connectionId]); // Only depend on connectionId

  const testConnection = useCallback(
    async (formData) => {
      setIsTestConnLoading(true);
      try {
        // Prepare test data - only add datasource-specific fields
        const testData = {
          ...formData,
          ...(["postgres", "snowflake"].includes(
            dbSelectionInfo.datasource_name
          ) && {
            schema: formData.schema || "",
            connection_type: connType,
          }),
        };

        // Encrypt sensitive fields if encryption service is available
        if (encryptionService.isAvailable()) {
          const encryptedTestData =
            await encryptionService.encryptSensitiveFields(testData);
          await testConnectionApi(
            axiosRef,
            selectedOrgId,
            csrfToken,
            dbSelectionInfo.datasource_name,
            encryptedTestData,
            connectionId || null
          );
        } else {
          console.warn(
            "⚠️  Encryption service not available, testing without encryption"
          );
          await testConnectionApi(
            axiosRef,
            selectedOrgId,
            csrfToken,
            dbSelectionInfo.datasource_name,
            testData,
            connectionId || null
          );
        }

        notify({
          type: "success",
          message: "Test connection passed successfully",
        });
        setIsTestConnSuccess(true);
      } catch (err) {
        notify({ error: err });
      } finally {
        setIsTestConnLoading(false);
      }
    },
    [dbSelectionInfo.datasource_name, selectedOrgId, csrfToken, connType]
  );

  const mappedDataSources = useMemo(
    () =>
      dataSources.map((dSource) => ({
        value: dSource.value,
        label: (
          <div key={dSource.label} className="dataSourceItem">
            <img
              src={dSource.icon}
              alt={dSource.label}
              className="dataSourceImg"
            />
            <span>{dSource.label}</span>
          </div>
        ),
      })),
    [dataSources]
  );

  return (
    <div className="createConnectionContainer">
      <div className="createConnectionBody">
        <div className="createConnectionBody2">
          <ConnectionDetailsSection
            connectionId={connectionId}
            dbSelectionInfo={dbSelectionInfo}
            connectionDetailsForm={connectionDetailsForm}
            handleCardClick={(value) => {
              // Clear input fields when datasource changes to prevent old fields from being sent
              setInputFields({});
              setDbSelectionInfo({
                ...dbSelectionInfo,
                datasource_name: value,
              });
            }}
            mappedDataSources={mappedDataSources}
            dbUsage={dbUsage}
          />

          <DeploymentCredentialsSection
            isDataSourceListLoading={isDataSourceListLoading}
            connectionId={connectionId}
            connectionDetails={connectionDetails}
            inputFields={inputFields}
            setInputFields={setInputFields}
            dbSelectionInfo={dbSelectionInfo}
            handleTestConnection={testConnection}
            isTestConnLoading={isTestConnLoading}
            isTestConnSuccess={isTestConnSuccess}
            setIsTestConnSuccess={setIsTestConnSuccess}
            handleCreateOrUpdate={handleCreateOrUpdate}
            isCreateOrUpdateLoading={isCreateOrUpdateLoading}
            isEncryptionLoading={isEncryptionLoading}
            connType={connType}
            setConnType={setConnType}
            hasDetailsChanged={hasDetailsChanged}
            isCredentialsRevealed={isCredentialsRevealed}
            isRevealLoading={isRevealLoading}
            handleRevealCredentials={handleRevealCredentials}
          />
        </div>
      </div>
    </div>
  );
};

CreateConnection.propTypes = {
  setIsModalOpen: PropTypes.func.isRequired,
  getAllConnection: PropTypes.func.isRequired,
  connectionId: PropTypes.string,
  setConnectionId: PropTypes.func,
  setConnectionDbType: PropTypes.func,
};

CreateConnection.defaultProps = {
  connectionId: "",
  setConnectionId: () => {},
  setConnectionDbType: () => {},
};

CreateConnection.displayName = "CreateConnection";

export { CreateConnection };
