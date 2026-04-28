/* eslint-disable react/prop-types */
import { useState, useEffect, useCallback, useMemo, useRef } from "react";
import Cookies from "js-cookie";
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
  Tooltip,
  Segmented,
  Empty,
} from "antd";
import {
  LinkOutlined,
  DatabaseOutlined,
  SafetyCertificateOutlined,
  ThunderboltOutlined,
  CheckCircleFilled,
  LockOutlined,
  EyeOutlined,
} from "@ant-design/icons";
import RjsfForm from "@rjsf/antd";
import validator from "@rjsf/validator-ajv8";

import { useAxiosPrivate } from "../../../service/axios-service";
import { orgStore } from "../../../store/org-store";
import encryptionService from "../../../service/encryption-service";
import {
  fetchDataSources,
  fetchDataSourceFields,
  createConnectionApi,
  updateConnectionApi,
  fetchSingleConnection,
  revealConnectionCredentials,
  testConnectionApi,
} from "../environment/environment-api-service";
import { useNotificationService } from "../../../service/notification-service";
import {
  validateFormFieldName,
  validateFormFieldDescription,
  collapseSpaces,
} from "../environment/helper";
import isEqual from "lodash/isEqual.js";
import { SpinnerLoader } from "../../../widgets/spinner_loader";
import { GridObjectFieldTemplate } from "./shared";

const { Text } = Typography;
const { TextArea } = Input;

/* ── DB Tile component — uses real logo from API ── */
const DBTile = ({ db, isActive, isDisabled, onClick }) => (
  <div
    className={"conn-db-tile" + (isActive ? " active" : "")}
    style={isDisabled ? { opacity: 0.35, cursor: "not-allowed" } : undefined}
    onClick={isDisabled ? undefined : onClick}
  >
    <div className="conn-db-tile-logo-wrap">
      <img src={db.icon} alt={db.label} className="conn-db-tile-logo-img" />
    </div>
    <div className="conn-db-tile-name">{db.label}</div>
  </div>
);

const ConnectionDrawer = ({
  open,
  onClose,
  connectionId,
  onSaved,
  getContainer,
}) => {
  const axiosRef = useAxiosPrivate();
  const { selectedOrgId } = orgStore();
  const csrfToken = Cookies.get("csrftoken");
  const { notify } = useNotificationService();
  const [form] = Form.useForm();
  const watchedName = Form.useWatch("name", form);
  const watchedDesc = Form.useWatch("description", form);

  // Data sources
  const [dataSources, setDataSources] = useState([]);
  const [dsLoading, setDsLoading] = useState(false);

  // Selected DB and fields
  const [selectedDb, setSelectedDb] = useState("");
  const [connectionDetails, setConnectionDetails] = useState({});
  const [inputFields, setInputFields] = useState({});
  const [connType, setConnType] = useState("host");

  // Edit mode
  const [originalInfo, setOriginalInfo] = useState(null);
  const [isCredentialsRevealed, setIsCredentialsRevealed] = useState(false);
  const [isRevealLoading, setIsRevealLoading] = useState(false);

  // Schema for RJSF
  const [schema, setSchema] = useState(null);
  const [uiSchema, setUiSchema] = useState({});

  // Actions
  const [isTestLoading, setIsTestLoading] = useState(false);
  const [isTestSuccess, setIsTestSuccess] = useState(false);
  const [testError, setTestError] = useState(null);
  const [showErrorDetail, setShowErrorDetail] = useState(false);
  const [isSaveLoading, setIsSaveLoading] = useState(false);
  const [isEncryptionLoading, setIsEncryptionLoading] = useState(true);

  const hasCapturedOriginalRef = useRef(false);
  const [originalConnectionData, setOriginalConnectionData] = useState({});

  const isEditing = Boolean(connectionId);

  /* ── Init encryption ── */
  useEffect(() => {
    if (!open) return;
    const init = async () => {
      setIsEncryptionLoading(true);
      try {
        await encryptionService.initialize(selectedOrgId || "default_org");
      } catch {
        // Encryption unavailable — proceed without
      } finally {
        setIsEncryptionLoading(false);
      }
    };
    init();
  }, [open, selectedOrgId]);

  /* ── Fetch datasources ── */
  useEffect(() => {
    if (!open) return;
    const load = async () => {
      setDsLoading(true);
      try {
        const ds = await fetchDataSources(axiosRef, selectedOrgId);
        setDataSources(ds);
        if (!connectionId && ds.length > 0 && !selectedDb) {
          setSelectedDb(ds[0].value);
        }
      } catch (error) {
        notify({ error });
      } finally {
        setDsLoading(false);
      }
    };
    load();
  }, [open, selectedOrgId]);

  /* ── Fetch field schema when DB changes ── */
  useEffect(() => {
    if (!selectedDb || !open) return;
    const load = async () => {
      try {
        const details = await fetchDataSourceFields(
          axiosRef,
          selectedOrgId,
          selectedDb
        );
        setConnectionDetails(details);
      } catch (error) {
        notify({ error });
      }
    };
    load();
  }, [selectedDb, selectedOrgId, open]);

  /* ── Build RJSF schema from connectionDetails + connType ── */
  useEffect(() => {
    if (Object.keys(connectionDetails).length === 0) {
      setSchema(null);
      return;
    }
    if (["postgres", "snowflake"].includes(selectedDb)) {
      const updatedProperties = { ...connectionDetails.properties };
      delete updatedProperties["connection_type"];
      const updatedRequired =
        connType === "url"
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
            connType === "url"
              ? key !== "connection_url"
              : key === "connection_url",
        };
      });
      setUiSchema({ ...ui, schema: { "ui:disabled": false } });
    } else {
      setSchema(connectionDetails);
      setUiSchema({});
    }
  }, [connectionDetails, connType, selectedDb]);

  /* ── Load existing connection for edit ── */
  useEffect(() => {
    if (!connectionId || !open) return;
    hasCapturedOriginalRef.current = false;
    const load = async () => {
      try {
        const data = await fetchSingleConnection(
          axiosRef,
          selectedOrgId,
          connectionId
        );
        const { name, description, datasource_name, connection_details } = data;
        setSelectedDb(datasource_name);
        setOriginalInfo({ name: collapseSpaces(name || ""), description });
        form.setFieldsValue({ name, description });

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
          setConnType(connection_details.connection_type || "host");
        }
        setIsCredentialsRevealed(false);
      } catch (error) {
        notify({ error });
      }
    };
    load();
  }, [connectionId, open]);

  /* ── Capture original data for change detection ── */
  useEffect(() => {
    if (
      connectionId &&
      Object.keys(inputFields).length > 0 &&
      !hasCapturedOriginalRef.current
    ) {
      setOriginalConnectionData(structuredClone(inputFields));
      hasCapturedOriginalRef.current = true;
    } else if (!connectionId) {
      setOriginalConnectionData({});
      hasCapturedOriginalRef.current = false;
    }
  }, [connectionId, inputFields]);

  /* ── Reset on close ── */
  useEffect(() => {
    if (!open) {
      form.resetFields();
      setSelectedDb("");
      setInputFields({});
      setConnectionDetails({});
      setSchema(null);
      setIsTestSuccess(false);
      setTestError(null);
      setShowErrorDetail(false);
      setOriginalInfo(null);
      setIsCredentialsRevealed(false);
      hasCapturedOriginalRef.current = false;
      setOriginalConnectionData({});
    }
  }, [open]);

  /* ── Has credential data changed? ── */
  const hasCredChanges = useMemo(() => {
    if (!connectionId) {
      return Object.values(inputFields).some(
        (v) => v !== undefined && v !== null && v !== ""
      );
    }
    if (Object.keys(originalConnectionData).length === 0) return false;
    const curr = { ...inputFields };
    const orig = { ...originalConnectionData };
    delete curr.connection_type;
    delete orig.connection_type;
    return !isEqual(curr, orig);
  }, [connectionId, inputFields, originalConnectionData]);

  const hasDetailsChanged = useMemo(() => {
    if (!connectionId || !originalInfo) return false;
    return (
      watchedName !== originalInfo.name ||
      watchedDesc !== originalInfo.description
    );
  }, [connectionId, originalInfo, watchedName, watchedDesc]);

  const hasValidData = useMemo(() => {
    return Object.values(inputFields).some(
      (v) => v !== undefined && v !== null && v !== ""
    );
  }, [inputFields]);

  /* ── Reveal credentials ── */
  const handleReveal = useCallback(async () => {
    if (!connectionId || isCredentialsRevealed) return;
    setIsRevealLoading(true);
    try {
      const creds = await revealConnectionCredentials(
        axiosRef,
        selectedOrgId,
        connectionId
      );
      const processed = { ...creds };
      if (
        selectedDb === "bigquery" &&
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
  }, [connectionId, selectedOrgId, isCredentialsRevealed, selectedDb]);

  /* ── Test connection ── */
  const handleTest = useCallback(async () => {
    setIsTestLoading(true);
    setIsTestSuccess(false);
    setTestError(null);
    setShowErrorDetail(false);
    try {
      const testData = {
        ...inputFields,
        ...(["postgres", "snowflake"].includes(selectedDb) && {
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
        selectedDb,
        data,
        connectionId || null
      );
      setIsTestSuccess(true);
    } catch (error) {
      const errorData = error?.response?.data;
      const errMsg =
        errorData?.error_message ||
        errorData?.message ||
        errorData?.error ||
        error?.message ||
        "Connection test failed";
      const statusCode = error?.response?.status;
      setTestError({
        summary: statusCode ? `Error ${statusCode}` : "Connection test failed",
        detail: errMsg,
      });
    } finally {
      setIsTestLoading(false);
    }
  }, [
    inputFields,
    selectedDb,
    connType,
    selectedOrgId,
    csrfToken,
    connectionId,
  ]);

  /* ── Save connection ── */
  const handleSave = useCallback(async () => {
    try {
      await form.validateFields();
    } catch {
      return;
    }
    setIsSaveLoading(true);
    try {
      const { name, description } = form.getFieldsValue();
      const payload = {
        datasource_name: selectedDb,
        name,
        description,
        connection_details: {
          ...inputFields,
          ...(["postgres", "snowflake"].includes(selectedDb) && {
            schema: inputFields.schema || "",
            connection_type: connType,
          }),
        },
        ...(connectionId &&
          hasDetailsChanged &&
          !hasCredChanges && { metadata_only: true }),
      };
      if (encryptionService.isAvailable()) {
        payload.connection_details =
          await encryptionService.encryptSensitiveFields(
            payload.connection_details
          );
      }
      if (!connectionId) {
        const res = await createConnectionApi(
          axiosRef,
          selectedOrgId,
          csrfToken,
          payload
        );
        if (res.status === 200) {
          notify({
            type: "success",
            message: "Connection created successfully",
          });
          onSaved?.();
          onClose();
        }
      } else {
        const res = await updateConnectionApi(
          axiosRef,
          selectedOrgId,
          csrfToken,
          connectionId,
          payload
        );
        if (res.status === 200) {
          notify({
            type: "success",
            message: "Connection updated successfully",
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
    selectedDb,
    inputFields,
    connType,
    connectionId,
    hasDetailsChanged,
    hasCredChanges,
    selectedOrgId,
    csrfToken,
  ]);

  /* ── RJSF handlers ── */
  const handleFieldChange = ({ formData }) => {
    setInputFields(formData);
    if (isTestSuccess) setIsTestSuccess(false);
    if (testError) {
      setTestError(null);
      setShowErrorDetail(false);
    }
  };

  const handleFieldSubmit = ({ formData }) => {
    setInputFields(formData);
    handleTest();
  };

  const handleConnTypeChange = (value) => {
    setConnType(value);
    if (!connectionId) setInputFields({});
  };

  /* ── Derive selected DB info ── */
  const selectedDbInfo = dataSources.find((d) => d.value === selectedDb);
  const dbLabel =
    selectedDbInfo?.label ||
    selectedDb.charAt(0).toUpperCase() + selectedDb.slice(1);

  const canSave =
    isTestSuccess || (connectionId && hasDetailsChanged && !hasCredChanges);

  return (
    <Drawer
      title={
        <Space>
          <LinkOutlined />
          <Text strong>{isEditing ? "Edit Connection" : "New Connection"}</Text>
        </Space>
      }
      width={640}
      open={open}
      onClose={onClose}
      destroyOnClose
      keyboard={false}
      maskClosable={false}
      getContainer={getContainer}
      className="conn-drawer"
      footer={
        <Row justify="space-between" align="middle">
          <Col>
            {isTestSuccess && (
              <Text style={{ fontSize: 12, color: "#10b981" }}>
                <CheckCircleFilled style={{ marginRight: 4 }} />
                Tested
              </Text>
            )}
          </Col>
          <Col>
            <Space>
              <Button onClick={onClose}>Cancel</Button>
              <Button
                type="primary"
                icon={<CheckCircleFilled />}
                onClick={handleSave}
                loading={isSaveLoading || isEncryptionLoading}
                disabled={!canSave || isEncryptionLoading}
              >
                Save connection
              </Button>
            </Space>
          </Col>
        </Row>
      }
    >
      <Form form={form} layout="vertical" size="middle" component="div">
        {/* ── STEP 1: Database picker ── */}
        <div className="conn-section-heading">
          1.{" "}
          {isEditing ? (
            <span>
              <LockOutlined /> Database{" "}
              <Text type="secondary" style={{ fontWeight: 400, marginLeft: 6 }}>
                · Locked after creation
              </Text>
            </span>
          ) : (
            "Pick your database"
          )}
        </div>
        {!isEditing && (
          <Text
            type="secondary"
            style={{ fontSize: 12, display: "block", marginBottom: 10 }}
          >
            The fields below will adjust based on your choice.
          </Text>
        )}
        {isEditing && (
          <Alert
            type="info"
            showIcon
            icon={<LockOutlined />}
            message={
              <Text style={{ fontSize: 12 }}>
                <strong>Database can&apos;t be changed after creation.</strong>{" "}
                Create a new connection for a different database.
              </Text>
            }
            style={{ marginBottom: 14 }}
          />
        )}
        {dsLoading ? (
          <SpinnerLoader />
        ) : (
          <div
            className="conn-db-grid"
            style={{
              marginBottom: 18,
              opacity: isEditing ? 0.85 : 1,
            }}
          >
            {dataSources.map((db) => {
              const isActive = selectedDb === db.value;
              const isDisabled = isEditing && !isActive;
              return (
                <DBTile
                  key={db.value}
                  db={db}
                  isActive={isActive}
                  isDisabled={isDisabled}
                  onClick={() => {
                    if (!isEditing) {
                      setSelectedDb(db.value);
                      setInputFields({});
                      setIsTestSuccess(false);
                    }
                  }}
                />
              );
            })}
          </div>
        )}

        <Divider />

        {/* ── STEP 2: Name & Describe ── */}
        <div className="conn-section-heading">2. Name &amp; describe</div>
        <Form.Item
          label="Connection name"
          name="name"
          required
          rules={[
            { required: true, message: "Required" },
            { validator: validateFormFieldName },
          ]}
          extra="Lowercase, hyphen-separated. Shown in environment pickers and logs."
          normalize={collapseSpaces}
        >
          <Input
            placeholder="e.g., jaffle-postgres-prod"
            prefix={
              <DatabaseOutlined style={{ color: "var(--question-icon)" }} />
            }
          />
        </Form.Item>
        <Form.Item
          label="Description"
          name="description"
          rules={[{ validator: validateFormFieldDescription }]}
        >
          <TextArea rows={2} placeholder="Who uses this and for what?" />
        </Form.Item>

        <Divider />

        {/* ── STEP 3: Deployment Credentials ── */}
        <div className="conn-section-heading">
          3. Deployment Credentials
          {selectedDb && (
            <Tag
              color="blue"
              style={{ fontSize: 10, marginLeft: 8, verticalAlign: "middle" }}
            >
              {dbLabel.toUpperCase()}
            </Tag>
          )}
        </div>
        <Alert
          type="info"
          showIcon
          icon={<SafetyCertificateOutlined />}
          message={
            <Text style={{ fontSize: 12 }}>
              Credentials are encrypted at rest and never appear in logs.
            </Text>
          }
          style={{ marginBottom: 14 }}
        />

        {/* URL vs Host toggle + Reveal button */}
        {["postgres", "snowflake"].includes(selectedDb) ? (
          <div style={{ marginBottom: 14 }}>
            <Text
              type="secondary"
              style={{
                fontSize: 11,
                display: "block",
                marginBottom: 6,
                fontWeight: 500,
              }}
            >
              How would you like to enter credentials?
            </Text>
            <Row justify="space-between" align="middle">
              <Col flex="auto">
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
              </Col>
              {isEditing && !isCredentialsRevealed && (
                <Col>
                  <Tooltip title="Reveal stored credentials">
                    <Button
                      size="small"
                      icon={<EyeOutlined />}
                      loading={isRevealLoading}
                      onClick={handleReveal}
                    >
                      Reveal
                    </Button>
                  </Tooltip>
                </Col>
              )}
            </Row>
          </div>
        ) : (
          isEditing &&
          !isCredentialsRevealed && (
            <div style={{ marginBottom: 12, textAlign: "right" }}>
              <Tooltip title="Reveal stored credentials">
                <Button
                  size="small"
                  icon={<EyeOutlined />}
                  loading={isRevealLoading}
                  onClick={handleReveal}
                >
                  Reveal
                </Button>
              </Tooltip>
            </div>
          )
        )}

        {/* Dynamic credential fields via RJSF */}
        {!selectedDb ? (
          <Empty description="Select a database above to see credential fields." />
        ) : !schema ? (
          <SpinnerLoader />
        ) : (
          <RjsfForm
            className="compactForm"
            schema={schema}
            validator={validator}
            formData={inputFields}
            onChange={handleFieldChange}
            onSubmit={handleFieldSubmit}
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
                  return { ...e, message: `Please enter ${title}` };
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
                    disabled={!hasValidData}
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
                      {testError.summary || "Connection test failed"}
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
      </Form>
    </Drawer>
  );
};

ConnectionDrawer.displayName = "ConnectionDrawer";

export { ConnectionDrawer };
