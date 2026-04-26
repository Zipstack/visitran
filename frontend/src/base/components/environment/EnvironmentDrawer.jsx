/* eslint-disable react/prop-types */
import { useState, useEffect, useCallback, useMemo, useRef } from "react";
import Cookies from "js-cookie";
import isEqual from "lodash/isEqual.js";
import {
  Drawer,
  Form,
  Input,
  Button,
  Space,
  Typography,
  Alert,
  Divider,
  Tag,
  Row,
  Col,
  Card,
  Select,
  Segmented,
} from "antd";
import {
  DatabaseOutlined,
  LinkOutlined,
  InfoCircleOutlined,
  ThunderboltOutlined,
  CheckCircleFilled,
  CloseCircleFilled,
  ExclamationCircleFilled,
  PlusOutlined,
  EyeOutlined,
  EyeInvisibleOutlined,
  SafetyCertificateOutlined,
} from "@ant-design/icons";
import RjsfForm from "@rjsf/antd";
import validator from "@rjsf/validator-ajv8";

import { useAxiosPrivate } from "../../../service/axios-service";
import { orgStore } from "../../../store/org-store";
import encryptionService from "../../../service/encryption-service";
import {
  fetchAllConnections,
  fetchSingleEnvironment,
  fetchSingleConnection,
  fetchDataSourceFields,
  createEnvironmentApi,
  updateEnvironmentApi,
  testConnectionApi,
  revealEnvironmentCredentials,
  revealConnectionCredentials,
} from "./environment-api-service";
import { useNotificationService } from "../../../service/notification-service";
import {
  validateFormFieldName,
  validateFormFieldDescription,
  collapseSpaces,
} from "./helper";
import { SpinnerLoader } from "../../../widgets/spinner_loader";

const { Text } = Typography;
const { TextArea } = Input;

/* ── Deployment type tile data ── */
const DEPLOY_TYPES = [
  {
    value: "PROD",
    label: "Production",
    desc: "Live data, careful changes. Requires approvals on deploy.",
    color: "#ef4444",
  },
  {
    value: "STG",
    label: "Staging",
    desc: "Mirror of prod for pre-release testing.",
    color: "#f59e0b",
  },
  {
    value: "DEV",
    label: "Development",
    desc: "Personal or team sandbox. Freely editable.",
    color: "#3b82f6",
  },
];

/* ── Fields that render side-by-side ── */
const HALF_WIDTH_FIELDS = new Set([
  "host",
  "port",
  "user",
  "passw",
  "account",
  "warehouse",
  "catalog",
  "schema",
  "dbname",
  "database",
  "project_id",
  "dataset_id",
  "token",
]);

const GridObjectFieldTemplate = (props) => (
  <div className="conn-cred-grid">
    {props.properties.map((prop) => (
      <div
        key={prop.name}
        className={
          HALF_WIDTH_FIELDS.has(prop.name)
            ? "conn-cred-field-half"
            : "conn-cred-field-full"
        }
      >
        {prop.content}
      </div>
    ))}
  </div>
);

/* ── Status tag ── */
const StatusTag = ({ flag }) => {
  if (flag === "GREEN")
    return (
      <Tag icon={<CheckCircleFilled />} color="success">
        Healthy
      </Tag>
    );
  if (flag === "YELLOW")
    return (
      <Tag icon={<ExclamationCircleFilled />} color="warning">
        Stale
      </Tag>
    );
  if (flag === "RED")
    return (
      <Tag icon={<CloseCircleFilled />} color="error">
        Error
      </Tag>
    );
  return null;
};

const EnvironmentDrawer = ({ open, onClose, envId, onSaved, getContainer }) => {
  const axiosRef = useAxiosPrivate();
  const { selectedOrgId } = orgStore();
  const csrfToken = Cookies.get("csrftoken");
  const { notify } = useNotificationService();
  const [form] = Form.useForm();

  // General
  const [deployType, setDeployType] = useState("PROD");
  const [connectionList, setConnectionList] = useState([]);
  const [connListLoading, setConnListLoading] = useState(false);
  const [selectedConnId, setSelectedConnId] = useState(null);
  const [selectedConnInfo, setSelectedConnInfo] = useState(null);

  // Credentials
  const [connectionDataSource, setConnectionDataSource] = useState(null);
  const [connectionSchema, setConnectionSchema] = useState({});
  const [schema, setSchema] = useState(null);
  const [uiSchema, setUiSchema] = useState({});
  const [inputFields, setInputFields] = useState({});
  const [connType, setConnType] = useState("host");
  const [isCredentialsRevealed, setIsCredentialsRevealed] = useState(false);
  const [isRevealLoading, setIsRevealLoading] = useState(false);

  // Test & Save
  const [isTestLoading, setIsTestLoading] = useState(false);
  const [isTestSuccess, setIsTestSuccess] = useState(false);
  const [testError, setTestError] = useState(null);
  const [showErrorDetail, setShowErrorDetail] = useState(false);
  const [isSaveLoading, setIsSaveLoading] = useState(false);
  const [isEncryptionLoading, setIsEncryptionLoading] = useState(true);

  // Change detection
  const [initialData, setInitialData] = useState(null);
  const [connectDetailBackup, setConnectDetailBackup] = useState({});
  const hasCapturedRef = useRef(false);

  const isEditing = Boolean(envId);

  /* ── Init encryption ── */
  useEffect(() => {
    if (!open) return;
    const init = async () => {
      setIsEncryptionLoading(true);
      try {
        await encryptionService.initialize(selectedOrgId || "default_org");
      } catch {
        // proceed without
      } finally {
        setIsEncryptionLoading(false);
      }
    };
    init();
  }, [open, selectedOrgId]);

  /* ── Fetch connection list ── */
  useEffect(() => {
    if (!open) return;
    const load = async () => {
      setConnListLoading(true);
      try {
        const data = await fetchAllConnections(axiosRef, selectedOrgId);
        setConnectionList(data?.filter((el) => !el?.is_sample_project) || []);
      } catch (error) {
        notify({ error });
      } finally {
        setConnListLoading(false);
      }
    };
    load();
  }, [open, selectedOrgId]);

  /* ── Fetch field schema when datasource changes ── */
  useEffect(() => {
    if (!connectionDataSource || !open) return;
    const load = async () => {
      try {
        const details = await fetchDataSourceFields(
          axiosRef,
          selectedOrgId,
          connectionDataSource
        );
        setConnectionSchema(details);
      } catch (error) {
        notify({ error });
      }
    };
    load();
  }, [connectionDataSource, selectedOrgId, open]);

  /* ── Build RJSF schema ── */
  useEffect(() => {
    if (Object.keys(connectionSchema).length === 0) {
      setSchema(null);
      return;
    }
    if (["postgres", "snowflake"].includes(connectionDataSource)) {
      const updatedProperties = { ...connectionSchema.properties };
      delete updatedProperties["connection_type"];
      const updatedRequired =
        connType === "url"
          ? ["connection_url"]
          : connectionSchema?.required?.filter(
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
            connType === "url"
              ? key !== "connection_url"
              : key === "connection_url",
        };
      });
      setUiSchema({ ...ui, schema: { "ui:disabled": false } });
    } else {
      setSchema(connectionSchema);
      setUiSchema({});
    }
  }, [connectionSchema, connType, connectionDataSource]);

  /* ── Load existing environment for edit ── */
  useEffect(() => {
    if (!envId || !open) return;
    hasCapturedRef.current = false;
    const load = async () => {
      try {
        const data = await fetchSingleEnvironment(
          axiosRef,
          selectedOrgId,
          envId
        );
        const { connection, name, description, deployment_type } = data;
        const connDetail = data.connection_details || {};
        form.setFieldsValue({ name, description });
        setDeployType(deployment_type);
        setSelectedConnId(connection.id);
        setConnectionDataSource(connection.datasource_name);
        setInitialData({ name, description, deployment_type });
        setConnectDetailBackup({ connection_details: connDetail });

        const processed = { ...connDetail };
        if (
          processed.credentials &&
          typeof processed.credentials === "object"
        ) {
          processed.credentials = JSON.stringify(
            processed.credentials,
            null,
            2
          );
        }
        setInputFields(processed);
        if (["postgres", "snowflake"].includes(connection.datasource_name)) {
          setConnType(connDetail?.connection_type || "host");
        }
        setIsCredentialsRevealed(false);

        // Find connection info for display
        setSelectedConnInfo({
          name: connection.name,
          datasource_name: connection.datasource_name,
          db_icon: connection.db_icon,
          connection_flag: connection.connection_flag,
        });
      } catch (error) {
        notify({ error });
      }
    };
    load();
  }, [envId, open]);

  /* ── Handle connection selection ── */
  const handleConnectionChange = useCallback(
    async (connId) => {
      if (connId === "__create__") {
        // TODO: open nested connection drawer
        return;
      }
      setSelectedConnId(connId);
      setIsTestSuccess(false);
      setTestError(null);
      setIsCredentialsRevealed(false);
      try {
        const connData = await fetchSingleConnection(
          axiosRef,
          selectedOrgId,
          connId
        );
        const { connection_details, datasource_name, name, db_icon } = connData;
        setConnectionDataSource(datasource_name);
        setSelectedConnInfo({
          name,
          datasource_name,
          db_icon,
          connection_flag: connData.connection_flag,
        });
        const processed = { ...connection_details };
        if (
          datasource_name === "bigquery" &&
          processed.credentials &&
          typeof processed.credentials === "object"
        ) {
          processed.credentials = JSON.stringify(
            processed.credentials,
            null,
            2
          );
        }
        setInputFields(processed);
        if (["postgres", "snowflake"].includes(datasource_name)) {
          setConnType(connection_details?.connection_type || "host");
        }
      } catch (error) {
        notify({ error });
      }
    },
    [selectedOrgId]
  );

  /* ── Reveal credentials ── */
  const handleReveal = useCallback(async () => {
    if (isCredentialsRevealed) {
      setIsCredentialsRevealed(false);
      return;
    }
    setIsRevealLoading(true);
    try {
      const creds = envId
        ? await revealEnvironmentCredentials(axiosRef, selectedOrgId, envId)
        : await revealConnectionCredentials(
            axiosRef,
            selectedOrgId,
            selectedConnId
          );
      const processed = { ...creds };
      if (
        connectionDataSource === "bigquery" &&
        processed.credentials &&
        typeof processed.credentials === "object"
      ) {
        processed.credentials = JSON.stringify(processed.credentials, null, 2);
      }
      setInputFields(processed);
      setIsCredentialsRevealed(true);
    } catch (error) {
      notify({ error });
    } finally {
      setIsRevealLoading(false);
    }
  }, [
    envId,
    selectedConnId,
    selectedOrgId,
    isCredentialsRevealed,
    connectionDataSource,
  ]);

  /* ── Test connection ── */
  const handleTest = useCallback(async () => {
    setIsTestLoading(true);
    setIsTestSuccess(false);
    setTestError(null);
    setShowErrorDetail(false);
    try {
      const testData = {
        ...inputFields,
        ...(["postgres", "snowflake"].includes(connectionDataSource) && {
          schema: inputFields.schema || "",
          connection_type: connType,
        }),
      };
      const data = encryptionService.isAvailable()
        ? await encryptionService.encryptSensitiveFields(testData)
        : testData;
      await testConnectionApi(
        axiosRef,
        selectedOrgId,
        csrfToken,
        connectionDataSource,
        data,
        selectedConnId || null
      );
      setIsTestSuccess(true);
    } catch (error) {
      const errorData = error?.response?.data;
      setTestError({
        summary: error?.response?.status
          ? `Error ${error.response.status}`
          : "Connection test failed",
        detail:
          errorData?.error_message ||
          errorData?.message ||
          errorData?.error ||
          error?.message ||
          "Connection test failed",
      });
    } finally {
      setIsTestLoading(false);
    }
  }, [
    inputFields,
    connectionDataSource,
    connType,
    selectedOrgId,
    csrfToken,
    selectedConnId,
  ]);

  /* ── Change detection ── */
  const hasGeneralChanges = useMemo(() => {
    if (!envId || !initialData) return false;
    const formVals = form.getFieldsValue();
    return (
      formVals.name !== initialData.name ||
      formVals.description !== initialData.description ||
      deployType !== initialData.deployment_type
    );
  }, [envId, initialData, deployType, form]);

  const hasCredChanges = useMemo(() => {
    return !isEqual(connectDetailBackup, { connection_details: inputFields });
  }, [inputFields, connectDetailBackup]);

  /* ── Save ── */
  const handleSave = useCallback(async () => {
    try {
      await form.validateFields();
    } catch {
      return;
    }
    if (!selectedConnId) {
      notify({ type: "warning", message: "Please select a connection" });
      return;
    }
    setIsSaveLoading(true);
    try {
      const { name, description } = form.getFieldsValue();
      const payload = {
        name,
        description,
        deployment_type: deployType,
        connection: { id: selectedConnId },
        connection_details: {
          ...inputFields,
          ...(["postgres", "snowflake"].includes(connectionDataSource) && {
            connection_type: connType,
          }),
        },
      };
      if (encryptionService.isAvailable()) {
        try {
          const encrypted = await encryptionService.encryptSensitiveFields(
            payload
          );
          Object.assign(payload, encrypted);
        } catch {
          // proceed unencrypted
        }
      }
      if (!envId) {
        const res = await createEnvironmentApi(
          axiosRef,
          selectedOrgId,
          csrfToken,
          payload
        );
        if (res.status === "success") {
          notify({
            type: "success",
            message: "Environment created successfully",
          });
          onSaved?.();
          onClose();
        }
      } else {
        const res = await updateEnvironmentApi(
          axiosRef,
          selectedOrgId,
          csrfToken,
          envId,
          payload
        );
        if (res.status === "success") {
          notify({
            type: "success",
            message: "Environment updated successfully",
          });
          onSaved?.();
          onClose();
        }
      }
    } catch (error) {
      notify({ error });
    } finally {
      setIsSaveLoading(false);
    }
  }, [
    form,
    deployType,
    selectedConnId,
    inputFields,
    connectionDataSource,
    connType,
    envId,
    selectedOrgId,
    csrfToken,
  ]);

  /* ── Reset on close ── */
  useEffect(() => {
    if (!open) {
      form.resetFields();
      setDeployType("PROD");
      setSelectedConnId(null);
      setSelectedConnInfo(null);
      setConnectionDataSource(null);
      setConnectionSchema({});
      setSchema(null);
      setInputFields({});
      setIsTestSuccess(false);
      setTestError(null);
      setInitialData(null);
      setConnectDetailBackup({});
      setIsCredentialsRevealed(false);
      hasCapturedRef.current = false;
    }
  }, [open]);

  /* ── RJSF handlers ── */
  const handleFieldChange = ({ formData }) => {
    setInputFields(formData);
    if (isTestSuccess) setIsTestSuccess(false);
    if (testError) {
      setTestError(null);
      setShowErrorDetail(false);
    }
  };

  const handleConnTypeChange = (value) => {
    setConnType(value);
  };

  const canSave = isEditing
    ? (hasGeneralChanges && !hasCredChanges) ||
      (hasCredChanges && isTestSuccess) ||
      (hasGeneralChanges && hasCredChanges && isTestSuccess)
    : isTestSuccess;

  /* ── Connection dropdown options ── */
  const connOptions = useMemo(() => {
    const statusIcon = (flag) => {
      if (flag === "GREEN")
        return <CheckCircleFilled style={{ color: "#10b981" }} />;
      if (flag === "YELLOW")
        return <ExclamationCircleFilled style={{ color: "#f59e0b" }} />;
      if (flag === "RED")
        return <CloseCircleFilled style={{ color: "#ef4444" }} />;
      return null;
    };
    const options = connectionList.map((c) => ({
      value: c.id,
      label: (
        <div
          key={c.id}
          style={{
            display: "flex",
            alignItems: "center",
            gap: 8,
            padding: "2px 0",
          }}
        >
          <img
            src={c.db_icon}
            alt={c.name}
            width={20}
            height={20}
            style={{ borderRadius: 4, flexShrink: 0 }}
          />
          <span style={{ fontWeight: 500 }}>{c.name}</span>
          <Tag style={{ fontSize: 10, margin: 0 }}>{c.datasource_name}</Tag>
          {c.host && (
            <span
              style={{
                fontSize: 11,
                color: "#94a3b8",
                fontFamily: "monospace",
              }}
            >
              · {c.host}
            </span>
          )}
          <span style={{ marginLeft: "auto" }}>
            {statusIcon(c.connection_flag)}
          </span>
        </div>
      ),
    }));
    options.push({
      value: "__create__",
      label: (
        <Space size={6} style={{ color: "#2563eb", fontWeight: 600 }}>
          <PlusOutlined /> Create new connection
        </Space>
      ),
    });
    return options;
  }, [connectionList]);

  return (
    <Drawer
      title={
        <Space>
          <DatabaseOutlined />
          <Text strong>
            {isEditing ? "Edit Environment" : "New Environment"}
          </Text>
        </Space>
      }
      width={620}
      open={open}
      onClose={onClose}
      destroyOnClose
      keyboard={false}
      maskClosable={false}
      getContainer={getContainer}
      className="conn-drawer"
      footer={
        <Row justify="end" gutter={8}>
          <Col>
            <Button onClick={onClose}>Cancel</Button>
          </Col>
          <Col>
            <Button
              type="primary"
              onClick={handleSave}
              loading={isSaveLoading || isEncryptionLoading}
              disabled={!canSave || isEncryptionLoading}
            >
              {isEditing ? "Save changes" : "Create environment"}
            </Button>
          </Col>
        </Row>
      }
    >
      <Form form={form} layout="vertical" size="middle" component="div">
        {/* Info alert */}
        <Alert
          type="info"
          showIcon
          icon={<InfoCircleOutlined />}
          message={
            <Text style={{ fontSize: 12 }}>
              <strong>An environment</strong> tells your jobs <em>where</em> to
              run. Pick a type, then pick a connection (the database).
            </Text>
          }
          style={{ marginBottom: 16 }}
        />

        {/* ── General ── */}
        <div className="conn-section-heading">General</div>
        <Form.Item
          label="Environment name"
          name="name"
          required
          rules={[
            { required: true, message: "Required" },
            { validator: validateFormFieldName },
          ]}
          extra="Lowercase, no spaces. Appears in logs and CLI commands."
          normalize={collapseSpaces}
        >
          <Input placeholder="e.g., production" />
        </Form.Item>
        <Form.Item
          label="Description"
          name="description"
          rules={[{ validator: validateFormFieldDescription }]}
        >
          <TextArea rows={2} placeholder="What is this environment used for?" />
        </Form.Item>

        <Divider />

        {/* ── Deployment Type ── */}
        <div className="conn-section-heading">Deployment Type *</div>
        <div className="env-type-grid" style={{ marginBottom: 16 }}>
          {DEPLOY_TYPES.map((dt) => (
            <div
              key={dt.value}
              className={
                "env-type-tile" + (deployType === dt.value ? " active" : "")
              }
              onClick={() => setDeployType(dt.value)}
            >
              <div className="env-type-head">
                <span
                  className="env-type-dot"
                  style={{ background: dt.color }}
                />
                <span className="env-type-title">{dt.label}</span>
              </div>
              <div className="env-type-desc">{dt.desc}</div>
            </div>
          ))}
        </div>

        <Divider />

        {/* ── Connection ── */}
        <div className="conn-section-heading">Connection *</div>
        <Select
          value={selectedConnId}
          onChange={handleConnectionChange}
          placeholder="Select a connection"
          loading={connListLoading}
          disabled={isEditing}
          style={{ width: "100%", marginBottom: 8 }}
          options={connOptions}
        />
        <Text
          type="secondary"
          style={{ fontSize: 11, display: "block", marginBottom: 12 }}
        >
          Choose an existing connection or create one inline.
        </Text>

        {/* Selected connection info card */}
        {selectedConnInfo && (
          <Card
            size="small"
            style={{ marginBottom: 16 }}
            styles={{ body: { padding: 12 } }}
          >
            <Row gutter={10} align="middle">
              <Col flex="32px">
                <img
                  src={selectedConnInfo.db_icon}
                  alt={selectedConnInfo.datasource_name}
                  width={28}
                  height={28}
                  style={{ borderRadius: 4 }}
                />
              </Col>
              <Col flex="auto">
                <Text strong style={{ fontSize: 13 }}>
                  {selectedConnInfo.name}
                </Text>
                <div
                  style={{
                    fontSize: 11,
                    color: "#94a3b8",
                  }}
                >
                  {selectedConnInfo.datasource_name}
                </div>
              </Col>
              <Col>
                <StatusTag flag={selectedConnInfo.connection_flag} />
              </Col>
            </Row>
          </Card>
        )}

        {/* ── Deployment Credentials (only when connection selected) ── */}
        {selectedConnId && connectionDataSource && (
          <>
            <Divider />
            <Row
              justify="space-between"
              align="middle"
              style={{ marginBottom: 6 }}
            >
              <Col>
                <div
                  className="conn-section-heading"
                  style={{ marginBottom: 0 }}
                >
                  Deployment Credentials
                  <Tag
                    color="blue"
                    style={{
                      marginLeft: 8,
                      fontSize: 10,
                      fontWeight: 500,
                    }}
                  >
                    {connectionDataSource.toUpperCase()}
                  </Tag>
                </div>
              </Col>
              <Col>
                <Button
                  size="small"
                  icon={
                    isCredentialsRevealed ? (
                      <EyeInvisibleOutlined />
                    ) : (
                      <EyeOutlined />
                    )
                  }
                  loading={isRevealLoading}
                  onClick={handleReveal}
                >
                  {isCredentialsRevealed ? "Hide" : "Reveal"}
                </Button>
              </Col>
            </Row>
            <Alert
              type="info"
              showIcon
              icon={<SafetyCertificateOutlined />}
              message={
                <Text style={{ fontSize: 12 }}>
                  Pre-filled from <strong>{selectedConnInfo?.name}</strong>.
                  Override here to use different credentials for this
                  environment.
                </Text>
              }
              style={{ marginBottom: 14 }}
            />

            {/* URL vs Host toggle */}
            {["postgres", "snowflake"].includes(connectionDataSource) && (
              <div style={{ marginBottom: 14 }}>
                <Segmented
                  value={connType}
                  onChange={handleConnTypeChange}
                  block
                  options={[
                    {
                      value: "host",
                      label: (
                        <Space size={6}>
                          <DatabaseOutlined /> Individual fields
                        </Space>
                      ),
                    },
                    {
                      value: "url",
                      label: (
                        <Space size={6}>
                          <LinkOutlined /> Connection URL
                        </Space>
                      ),
                    },
                  ]}
                />
              </div>
            )}

            {/* RJSF credential fields */}
            {!schema ? (
              <SpinnerLoader />
            ) : (
              <RjsfForm
                className="compactForm"
                schema={schema}
                validator={validator}
                formData={inputFields}
                onChange={handleFieldChange}
                onSubmit={() => handleTest()}
                uiSchema={uiSchema}
                templates={{
                  ObjectFieldTemplate: GridObjectFieldTemplate,
                  ErrorListTemplate: () => null,
                }}
                transformErrors={(errors) =>
                  errors.map((e) => {
                    if (e.name === "required") {
                      const prop = e.params.missingProperty;
                      const title = schema?.properties?.[prop]?.title || prop;
                      return {
                        ...e,
                        message: `Please enter ${title}`,
                      };
                    }
                    return e;
                  })
                }
                omitExtraData
                liveOmit
              >
                {/* Test connection card */}
                <Card
                  size="small"
                  styles={{ body: { padding: 12 } }}
                  style={{ marginTop: 12 }}
                >
                  <Row justify="space-between" align="middle">
                    <Col>
                      <div style={{ fontSize: 13, fontWeight: 500 }}>
                        Test this connection
                      </div>
                      <Text type="secondary" style={{ fontSize: 11 }}>
                        Verify before saving. No data is read.
                      </Text>
                    </Col>
                    <Col>
                      <Button
                        htmlType="submit"
                        icon={<ThunderboltOutlined />}
                        loading={isTestLoading}
                        style={{
                          background: "#f59e0b",
                          borderColor: "#f59e0b",
                          color: "white",
                        }}
                      >
                        Test connection
                      </Button>
                    </Col>
                  </Row>
                  {isTestSuccess && (
                    <Alert
                      type="success"
                      showIcon
                      icon={<CheckCircleFilled />}
                      style={{ marginTop: 10 }}
                      message={
                        <Text strong style={{ fontSize: 12 }}>
                          Connection verified
                        </Text>
                      }
                    />
                  )}
                  {testError && (
                    <Alert
                      type="error"
                      showIcon
                      style={{ marginTop: 10 }}
                      message={
                        <Text strong style={{ fontSize: 12 }}>
                          {testError.summary}
                        </Text>
                      }
                      description={
                        <div>
                          {!showErrorDetail ? (
                            <Button
                              type="link"
                              size="small"
                              style={{ padding: 0, fontSize: 11 }}
                              onClick={() => setShowErrorDetail(true)}
                            >
                              View details
                            </Button>
                          ) : (
                            <>
                              <Button
                                type="link"
                                size="small"
                                style={{
                                  padding: 0,
                                  fontSize: 11,
                                  marginBottom: 4,
                                }}
                                onClick={() => setShowErrorDetail(false)}
                              >
                                Hide details
                              </Button>
                              <div
                                style={{
                                  fontSize: 11,
                                  fontFamily: "monospace",
                                  background: "var(--page-bg-1, #fafafa)",
                                  border: "1px solid var(--border-color-4)",
                                  padding: 8,
                                  borderRadius: 4,
                                  maxHeight: 150,
                                  overflowY: "auto",
                                  whiteSpace: "pre-wrap",
                                  wordBreak: "break-word",
                                }}
                              >
                                {testError.detail}
                              </div>
                            </>
                          )}
                        </div>
                      }
                    />
                  )}
                </Card>
              </RjsfForm>
            )}

            <Divider />
            <Form.Item
              label="Default schema"
              extra="Models build here unless a specific schema is set per-model."
            >
              <Input
                placeholder="public"
                style={{ fontFamily: "monospace" }}
                value={inputFields.schema || ""}
                onChange={(e) =>
                  setInputFields({
                    ...inputFields,
                    schema: e.target.value,
                  })
                }
              />
            </Form.Item>
          </>
        )}
      </Form>
    </Drawer>
  );
};

EnvironmentDrawer.displayName = "EnvironmentDrawer";

export { EnvironmentDrawer };
