import { useState, useEffect, useCallback, useMemo } from "react";
import Cookies from "js-cookie";
import { Button, Divider, Modal, Space } from "antd";
import PropTypes from "prop-types";
import isEqual from "lodash/isEqual.js";

import { useAxiosPrivate } from "../../../service/axios-service";
import { orgStore } from "../../../store/org-store";
import { generateKey } from "../../../common/helpers";
import {
  fetchAllConnections,
  fetchProjectByConnection,
  fetchSingleEnvironment,
  updateEnvironmentApi,
  createEnvironmentApi,
  fetchDataSourceFields,
  testConnectionApi,
  fetchSingleConnection,
  revealConnectionCredentials,
  revealEnvironmentCredentials,
} from "./environment-api-service";
import encryptionService from "../../../service/encryption-service";

import "./environment.css";
import EnvGeneralSection from "./EnvGeneralSection";
import EnvCustomDataSection from "./EnvCustomDataSection";
import { CreateConnection } from "./CreateConnection";
import { useNotificationService } from "../../../service/notification-service";

const NewEnv = ({
  connectionId,
  setEnvironment,
  setIsEnvModalOpen,
  getAllEnvironments,
  id,
  actionState,
}) => {
  const axiosRef = useAxiosPrivate();
  const csrfToken = Cookies.get("csrftoken");
  const { selectedOrgId } = orgStore();

  const [isModalOpen, setIsModalOpen] = useState(false);
  const [connectionList, setConnectionList] = useState([]);
  const [customData, setCustomData] = useState([]);
  const [connection, setConnection] = useState({ id: "" });
  const [connectionDataSource, setConnectionDataSource] = useState(null);
  const [connectionSchema, setConnectionSchema] = useState({});
  const [isConnSchemaLoading, setIsConnSchemaLoading] = useState(false);
  const [isTestConnLoading, setIsTestConnLoading] = useState(false);
  const [isTestConnSuccess, setIsTestConnSuccess] = useState(false);
  const [loading, setLoading] = useState(false);
  const [inputFields, setInputFields] = useState({});
  const [connType, setConnType] = useState("url");
  const [connectionDetailsUpdated, setConnectionDetailsUpdated] =
    useState(false);
  const [projListDep, setProjListDep] = useState([]);
  const [connectDetailBackup, setConnectDetailBackup] = useState({});
  const [initialPrefillData, setInitialPrefillData] = useState({
    name: "",
    description: "",
    deployment_type: "",
    custom_data: [],
  });
  const [activeUpdateBtn, setActiveUpdateBtn] = useState(false);
  const [isEncryptionLoading, setIsEncryptionLoading] = useState(true);
  const [isCredentialsRevealed, setIsCredentialsRevealed] = useState(false);
  const [isRevealLoading, setIsRevealLoading] = useState(false);

  const [envNameDescInfo, setEnvNameDescInfo] = useState({
    name: "",
    description: "",
    deployment_type: "",
  });
  const { notify } = useNotificationService();

  // Initialize encryption service
  useEffect(() => {
    const initEncryption = async () => {
      setIsEncryptionLoading(true);
      try {
        if (selectedOrgId) {
          await encryptionService.initialize(selectedOrgId);
        }
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

  // Reset form fields when creating new environment (not editing)
  useEffect(() => {
    if (!id) {
      // Reset all form fields when opening Create New Environment modal
      setEnvNameDescInfo({
        name: "",
        description: "",
        deployment_type: "",
      });
      setCustomData([]);
      setInputFields({});
      setConnection({ id: "" });
      setConnectionDataSource(null);
      setConnectionSchema({});
      setIsTestConnSuccess(false);
      setConnectionDetailsUpdated(false);
      setConnType("url");
      setInitialPrefillData({
        name: "",
        description: "",
        deployment_type: "",
        custom_data: [],
      });
      setConnectDetailBackup({});
      setIsCredentialsRevealed(false);
    }
  }, [id]);

  // Reset form fields function
  const resetFormFields = useCallback(() => {
    setEnvNameDescInfo({
      name: "",
      description: "",
      deployment_type: "",
    });
    setCustomData([]);
    setInputFields({});
    setConnection({ id: "" });
    setConnectionDataSource(null);
    setConnectionSchema({});
    setIsTestConnSuccess(false);
    setConnectionDetailsUpdated(false);
    setConnType("url");
    setInitialPrefillData({
      name: "",
      description: "",
      deployment_type: "",
      custom_data: [],
    });
    setConnectDetailBackup({});
    setIsCredentialsRevealed(false);
  }, []);

  // Reset form fields when creating new environment (not editing)
  useEffect(() => {
    if (!id) {
      resetFormFields();
    }
  }, [id, resetFormFields]);

  useEffect(() => {
    if (!connection?.id || !connectionList?.length) {
      setConnectionDataSource(null);
    }

    setConnectionDataSource(
      () =>
        connectionList.find((conn) => conn?.id === connection?.id)
          ?.datasource_name
    );
  }, [connection, connectionList]);

  useEffect(() => {
    if (!connectionDataSource) {
      setConnectionSchema({});
      return;
    }

    fetchDbConnectionSchema(connectionDataSource);
  }, [connectionDataSource]);

  const fetchDbConnectionSchema = useCallback(async (dataSource) => {
    if (!dataSource) return;
    try {
      setIsConnSchemaLoading(true);
      const details = await fetchDataSourceFields(
        axiosRef,
        selectedOrgId,
        dataSource
      );

      setConnectionSchema(details);
    } catch (error) {
      console.error(error);
      notify({ error });
    } finally {
      setIsConnSchemaLoading(false);
    }
  }, []);

  const handleDbTestConnection = useCallback(
    async (formData) => {
      setIsTestConnLoading(true);
      try {
        // Prepare test data - only add datasource-specific fields
        const testData = {
          ...formData,
          ...(["postgres", "snowflake"].includes(connectionDataSource) && {
            schema: formData?.schema || "",
            connection_type: connType,
          }),
        };

        // Encrypt sensitive fields if encryption service is available
        let encryptedTestData = testData;
        if (encryptionService.isAvailable()) {
          try {
            encryptedTestData = await encryptionService.encryptSensitiveFields(
              testData
            );
          } catch (error) {
            console.warn("Failed to encrypt test data:", error);
            // Continue with unencrypted data if encryption fails
          }
        }

        await testConnectionApi(
          axiosRef,
          selectedOrgId,
          csrfToken,
          connectionDataSource,
          encryptedTestData,
          connection?.id || null
        );
        notify({
          type: "success",
          message: "Test connection passed successfully",
        });
        setIsTestConnSuccess(true);
      } catch (error) {
        console.error(error);
        notify({ error });
      } finally {
        setIsTestConnLoading(false);
      }
    },
    [connectionDataSource, selectedOrgId, csrfToken, connType, connection]
  );

  const getAllConnections = useCallback(async () => {
    setLoading(true);
    try {
      const data = await fetchAllConnections(axiosRef, selectedOrgId);
      setConnectionList(data?.filter((el) => !el?.is_sample_project));
    } catch (error) {
      console.error(error);
      notify({ error });
    } finally {
      setLoading(false);
    }
  }, [selectedOrgId]);

  const getProjectDependency = useCallback(async () => {
    try {
      const data = await fetchProjectByConnection(axiosRef, selectedOrgId, id);
      setProjListDep(data);
    } catch (error) {
      console.error(error);
      notify({ error });
    }
  }, [id, selectedOrgId]);

  useEffect(() => {
    getAllConnections();
  }, [getAllConnections]);

  const getSingleEnvironmentDetails = useCallback(async () => {
    try {
      const data = await fetchSingleEnvironment(axiosRef, selectedOrgId, id);
      const { connection, name, description, custom_data, deployment_type } =
        data;
      const connDetail = data.connection_details;

      setEnvNameDescInfo({ name, description, deployment_type });
      setInitialPrefillData({
        name,
        description,
        deployment_type,
        custom_data,
      });
      setConnectDetailBackup({ connection_details: connDetail });
      setConnType(
        connDetail?.connection_type ? connDetail?.connection_type : "host"
      );
      setConnection({ id: connection.id });
      setCustomData(Array.isArray(custom_data) ? custom_data : []);

      // Process connection details to handle JSON objects for textarea fields
      const processedConnDetail = { ...connDetail };

      // For BigQuery, ensure credentials are a string for the textarea
      if (
        processedConnDetail.credentials &&
        typeof processedConnDetail.credentials === "object"
      ) {
        processedConnDetail.credentials = JSON.stringify(
          processedConnDetail.credentials,
          null,
          2
        );
      }

      setInputFields(processedConnDetail);
    } catch (error) {
      console.error(error);
      notify({ error });
    }
  }, [id, selectedOrgId]);

  useEffect(() => {
    if (id) {
      getSingleEnvironmentDetails();
      getProjectDependency();
    } else {
      setConnection({ id: "" });
      setEnvNameDescInfo({ name: "", description: "", deployment_type: "" });
      setConnectionDataSource(null);
      setConnectionSchema({});
    }
  }, [id, getSingleEnvironmentDetails, getProjectDependency]);

  const handleEnvNameDesChange = (value, name) => {
    setEnvNameDescInfo((prev) => ({ ...prev, [name]: value }));
  };

  const handleConnectionChange = async (value) => {
    const selectedConn = connectionList.find((conn) => conn.id === value.value);
    if (selectedConn) {
      setConnection({
        id: value.value,
      });
      setIsCredentialsRevealed(false);

      // Fetch connection details from API
      try {
        const connectionData = await fetchSingleConnection(
          axiosRef,
          selectedOrgId,
          value.value
        );

        const { connection_details, datasource_name } = connectionData;

        // Set connection data source for deployment credentials section
        setConnectionDataSource(datasource_name);

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

        // Set connection type for postgres and snowflake
        if (["postgres", "snowflake"].includes(datasource_name)) {
          setConnType(
            connection_details.connection_type
              ? connection_details.connection_type
              : "host"
          );
        }

        setInputFields(processedConnectionDetails);
      } catch (error) {
        console.error("Error fetching connection details:", error);
        notify({ error });

        // Fallback to connection list data if API call fails
        setInputFields(selectedConn?.connection_details || {});
        setConnectionDataSource(selectedConn?.datasource_name);
      }
    }
  };

  const handleRevealCredentials = useCallback(async () => {
    if (isCredentialsRevealed) return;
    setIsRevealLoading(true);
    try {
      // Edit mode: reveal from environment; Create mode: reveal from connection
      const credentials = id
        ? await revealEnvironmentCredentials(axiosRef, selectedOrgId, id)
        : await revealConnectionCredentials(
            axiosRef,
            selectedOrgId,
            connection.id
          );
      const processedCredentials = { ...credentials };

      // For BigQuery, ensure credentials are a string for the textarea
      if (
        connectionDataSource === "bigquery" &&
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
    id,
    connection?.id,
    selectedOrgId,
    isCredentialsRevealed,
    connectionDataSource,
  ]);

  const AddnewEntry = () => {
    const singleData = { source_schema: "", destination_schema: "" };
    setCustomData((prev) => [...prev, { id: generateKey(), ...singleData }]);
  };

  const handleCustomFieldChange = (value, name, rowId) => {
    setCustomData((prev) =>
      prev.map((item) =>
        item.id === rowId ? { ...item, [name]: value } : item
      )
    );
  };

  const disabledAddCustomBtn = useMemo(() => {
    return customData.some(
      (item) => !item.source_schema || !item.destination_schema
    );
  }, [customData]);

  const handleDelete = (rowId) => {
    setCustomData((prev) => prev.filter((el) => el.id !== rowId));
  };

  const updateEnvironment = async () => {
    try {
      // Prepare environment data
      const environmentData = {
        ...envNameDescInfo,
        connection: { id: connection.id },
        connection_details: {
          ...inputFields,
          ...(["postgres", "snowflake"].includes(connectionDataSource) && {
            connection_type: connType,
          }),
        },
        custom_data: customData,
      };

      // Encrypt sensitive fields if encryption service is available
      let encryptedData = environmentData;
      if (encryptionService.isAvailable()) {
        try {
          encryptedData = await encryptionService.encryptSensitiveFields(
            environmentData
          );
        } catch (error) {
          console.warn("Failed to encrypt environment data:", error);
          // Continue with unencrypted data if encryption fails
        }
      } else {
        console.warn(
          "⚠️ Encryption service not available, sending unencrypted data"
        );
      }

      const res = await updateEnvironmentApi(
        axiosRef,
        selectedOrgId,
        csrfToken,
        id,
        encryptedData
      );
      if (res.status === "success") {
        setEnvironment({ id: res.data.id });
        getAllEnvironments();
        setIsEnvModalOpen(false);
        notify({
          type: "success",
          message: "Environment Updated Successfully",
        });
      }
    } catch (error) {
      console.error(error);
      notify({ error });
    }
  };

  const createEnvironment = async () => {
    try {
      // Prepare environment data
      const environmentData = {
        ...envNameDescInfo,
        connection: { id: connection.id },
        connection_details: {
          ...inputFields,
          ...(["postgres", "snowflake"].includes(connectionDataSource) && {
            connection_type: connType,
          }),
        },
        custom_data: customData,
      };

      // Encrypt sensitive fields if encryption service is available
      let encryptedData = environmentData;
      if (encryptionService.isAvailable()) {
        try {
          encryptedData = await encryptionService.encryptSensitiveFields(
            environmentData
          );
        } catch (error) {
          console.warn("Failed to encrypt environment data:", error);
          // Continue with unencrypted data if encryption fails
        }
      } else {
        console.warn(
          "⚠️ Encryption service not available, sending unencrypted data"
        );
      }

      const res = await createEnvironmentApi(
        axiosRef,
        selectedOrgId,
        csrfToken,
        encryptedData
      );
      if (res.status === "success") {
        setEnvironment({ id: res.data.id });
        getAllEnvironments();
        setIsEnvModalOpen(false);
        notify({
          type: "success",
          message: "Environment Created Successfully",
        });
        setEnvNameDescInfo({ name: "", description: "", deployment_type: "" });
        setCustomData([]);
        setConnection({ id: "" });
      }
    } catch (error) {
      console.error(error);
      notify({ error });
    }
  };

  useEffect(() => {
    if (connectionId) {
      setConnection({ id: connectionId });
    }
  }, [connectionId]);

  useEffect(() => {
    const res = isEqual(initialPrefillData, {
      name: envNameDescInfo.name,
      description: envNameDescInfo.description,
      deployment_type: envNameDescInfo.deployment_type,
      custom_data: customData,
    });
    setActiveUpdateBtn(!res);
  }, [envNameDescInfo, customData, initialPrefillData]);

  useEffect(() => {
    const res = isEqual(connectDetailBackup, {
      connection_details: inputFields,
    });
    setConnectionDetailsUpdated(!res);
  }, [inputFields, connectDetailBackup]);

  return (
    <>
      <div className="envMainContainer">
        <div className="envMainWrap">
          <EnvGeneralSection
            actionState={actionState}
            envNameDescInfo={envNameDescInfo}
            handleEnvNameDesChange={handleEnvNameDesChange}
            loading={loading}
            connection={connection}
            connectionList={connectionList}
            connectionDetails={connectionSchema}
            isConnDetailsLoading={isConnSchemaLoading}
            handleDbTestConnection={handleDbTestConnection}
            isTestConnLoading={isTestConnLoading}
            isTestConnSuccess={isTestConnSuccess}
            setIsTestConnSuccess={setIsTestConnSuccess}
            inputFields={inputFields}
            setInputFields={setInputFields}
            handleConnectionChange={handleConnectionChange}
            setIsModalOpen={setIsModalOpen}
            id={id}
            connType={connType}
            setConnType={setConnType}
            connectionDataSource={connectionDataSource}
            isCredentialsRevealed={isCredentialsRevealed}
            isRevealLoading={isRevealLoading}
            handleRevealCredentials={handleRevealCredentials}
          />

          <Divider className="divider-modal" />

          <EnvCustomDataSection
            actionState={actionState}
            customData={customData}
            AddnewEntry={AddnewEntry}
            handleCustomFieldChange={handleCustomFieldChange}
            disabledAddCustomBtn={disabledAddCustomBtn}
            handleDelete={handleDelete}
            projListDep={projListDep}
            id={id}
          />

          <Divider className="divider-modal" />

          <div className="envBtnWrap">
            <Space>
              <Button onClick={() => setIsEnvModalOpen(false)}>Cancel</Button>
              {id ? (
                <Button
                  type="primary"
                  className="primary_button_style"
                  onClick={updateEnvironment}
                  disabled={
                    (!activeUpdateBtn && !connectionDetailsUpdated) ||
                    !envNameDescInfo.name ||
                    (connectionDetailsUpdated && !isTestConnSuccess) ||
                    isEncryptionLoading
                  }
                  loading={isEncryptionLoading}
                >
                  Update
                </Button>
              ) : (
                <Button
                  type="primary"
                  className="primary_button_style"
                  onClick={createEnvironment}
                  disabled={
                    !envNameDescInfo.name ||
                    !envNameDescInfo.deployment_type ||
                    !connection?.id ||
                    !isTestConnSuccess ||
                    isEncryptionLoading
                  }
                  loading={isEncryptionLoading}
                >
                  Create
                </Button>
              )}
            </Space>
          </div>
        </div>
      </div>

      <Modal
        title="Create Connection"
        open={isModalOpen}
        onCancel={() => setIsModalOpen(false)}
        footer={null}
        centered
        width={1000}
        maskClosable={false}
      >
        <CreateConnection
          setIsModalOpen={setIsModalOpen}
          setConnectionId={setConnection}
          getAllConnection={getAllConnections}
        />
      </Modal>
    </>
  );
};

NewEnv.propTypes = {
  connectionId: PropTypes.string,
  setEnvironment: PropTypes.func,
  setIsEnvModalOpen: PropTypes.func,
  getAllEnvironments: PropTypes.func,
  id: PropTypes.string,
  actionState: PropTypes.string,
};

NewEnv.defaultProps = {
  connectionId: "",
  setEnvironment: () => {},
  setIsEnvModalOpen: () => {},
  getAllEnvironments: () => {},
  id: "",
  actionState: "",
};

NewEnv.displayName = "NewEnv";

export default NewEnv;
