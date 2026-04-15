import {
  useState,
  useEffect,
  useCallback,
  useReducer,
  useMemo,
  memo,
} from "react";
import PropTypes from "prop-types";
import {
  Drawer,
  Form,
  Input,
  InputNumber,
  Select,
  Button,
  Space,
  Typography,
  Segmented,
  Switch,
  Divider,
  Spin,
  Collapse,
} from "antd";
import {
  ClockCircleOutlined,
  FieldTimeOutlined,
  ApartmentOutlined,
  SettingOutlined,
  BellOutlined,
  RetweetOutlined,
  LinkOutlined,
  ExpandAltOutlined,
  ShrinkOutlined,
} from "@ant-design/icons";

import { checkPermission } from "../../common/helpers";
import { useNotificationService } from "../../service/notification-service";
import { useProjectStore } from "../../store/project-store";
import { useSessionStore } from "../../store/session-store";
import { useJobService } from "./service";
import { CronFields } from "./CronFields";
import { IntervalFields } from "./IntervalFields";
import ModelConfigsTable from "./ModelConfigsTable";

const { TextArea } = Input;

const TASK_TYPES = {
  CRON: "cron",
  INTERVAL: "interval",
};

const DEFAULT_CRON = "30 * * * *";

const EMAIL_REGEX = /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/;

/* ─── cron values reducer (matches CronFields interface) ─── */
const cronReducer = (state, action) => {
  switch (action.type) {
    case "set_values":
      return { cronValue: action.value, inputValue: action.value };
    case "set_cron_value":
      return { ...state, cronValue: action.value };
    case "set_input_value":
      return { ...state, inputValue: action.value };
    case "reset":
      return { cronValue: DEFAULT_CRON, inputValue: DEFAULT_CRON };
    default:
      return state;
  }
};

/* ─── Flatten model tree into list of model names ─── */
const extractModelNames = (explorerData) => {
  const models = [];
  const walk = (node) => {
    if (!node) return;
    if (
      !node.is_folder &&
      (node.type === "NO_CODE_MODEL" || node.type === "FULL_CODE_MODEL")
    ) {
      models.push(node.title);
    }
    if (Array.isArray(node.children)) {
      node.children.forEach(walk);
    }
  };
  walk(explorerData);
  return models;
};

/* ═══════════════════════════════════════════════════════ */

const JobDeploy = memo(function JobDeploy({
  open,
  setOpen,
  selectedJobDeployId,
  setIsJobListModified,
}) {
  const [form] = Form.useForm();
  const canWrite = checkPermission("JOB_DEPLOYMENT", "can_write");
  const { notify } = useNotificationService();
  const { projectId } = useProjectStore();
  const { sessionDetails } = useSessionStore();
  const userEmail = sessionDetails?.email || "";
  const {
    createTask,
    updateTask,
    getPeriodicTask,
    listPeriodicTasks,
    getEnvironments,
    getProjects,
    getProjectModels,
  } = useJobService();

  const isEditMode = !!selectedJobDeployId;

  /* ─── local state ─── */
  const [loading, setLoading] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [taskType, setTaskType] = useState(TASK_TYPES.CRON);
  const [environments, setEnvironments] = useState([]);
  const [projects, setProjects] = useState([]);
  const [selectedProjectId, setSelectedProjectId] = useState(projectId);
  const [modelNames, setModelNames] = useState([]);
  const [modelsLoading, setModelsLoading] = useState(false);
  const [cronError, setCronError] = useState(null);
  const [cronValues, dispatchCron] = useReducer(cronReducer, {
    cronValue: DEFAULT_CRON,
    inputValue: DEFAULT_CRON,
  });
  const [modelConfigs, setModelConfigs] = useState({});
  const [selectedEnvironmentId, setSelectedEnvironmentId] = useState(null);
  const [allJobs, setAllJobs] = useState([]);
  const [drawerExpanded, setDrawerExpanded] = useState(false);
  const [modelConfigActiveKey, setModelConfigActiveKey] = useState([]);

  /* ─── derived options ─── */
  const projectOptions = useMemo(
    () =>
      projects.map((p) => ({
        label: p.project_name,
        value: p.project_id,
      })),
    [projects]
  );

  const envOptions = useMemo(
    () =>
      environments.map((e) => ({
        label: `${e.name} (${e.deployment_type || ""})`,
        value: e.id,
      })),
    [environments]
  );

  const jobOptions = useMemo(
    () =>
      allJobs
        .filter((j) => j.user_task_id !== selectedJobDeployId)
        .map((j) => ({ label: j.task_name, value: j.user_task_id })),
    [allJobs, selectedJobDeployId]
  );

  /* ─── fetch environments & projects ─── */
  useEffect(() => {
    if (!open) return;
    getEnvironments()
      .then(setEnvironments)
      .catch((err) => {
        console.error("Failed to load environments", err);
        notify({ error: err });
      });
    getProjects()
      .then((data) => {
        setProjects(data);
        // Pre-select project from store if available
        if (projectId && !form.getFieldValue("project")) {
          form.setFieldsValue({ project: projectId });
          setSelectedProjectId(projectId);
        }
      })
      .catch((err) => {
        console.error("Failed to load projects", err);
        notify({ error: err });
      });
    // Fetch all jobs for chaining dropdown
    listPeriodicTasks(1, 100)
      .then((res) => {
        setAllJobs(res?.data?.page_items || []);
      })
      .catch(() => {});
  }, [open]);

  /* ─── fetch models when project changes ─── */
  useEffect(() => {
    if (!open || !selectedProjectId) return;
    setModelsLoading(true);
    getProjectModels(selectedProjectId)
      .then((data) => {
        const names = extractModelNames(data);
        setModelNames(names);
        // Reset model configs when project changes (unless editing)
        if (!isEditMode) {
          setModelConfigs({});
        }
      })
      .catch((err) => {
        console.error("Failed to load models", err);
      })
      .finally(() => setModelsLoading(false));
  }, [open, selectedProjectId, isEditMode]);

  /* ─── auto-open Model Configuration when project is selected ─── */
  useEffect(() => {
    if (selectedProjectId) {
      setModelConfigActiveKey((prev) =>
        prev.includes("model-config") ? prev : ["model-config"]
      );
    }
  }, [selectedProjectId]);

  /* ─── load existing job when editing ─── */
  useEffect(() => {
    if (!open || !selectedJobDeployId) return;

    setLoading(true);
    getPeriodicTask(selectedJobDeployId)
      .then((res) => {
        const job = Array.isArray(res?.data) ? res.data[0] : res?.data;
        if (!job) return;

        const type = job.task_type || TASK_TYPES.CRON;
        setTaskType(type);

        form.setFieldsValue({
          task_name: job.task_name,
          description: job.description,
          project: job.project?.id,
          environment: job.environment?.id,
          enabled: job.periodic_task_details?.enabled ?? true,
          // Advanced settings
          run_timeout_seconds: job.run_timeout_seconds || 3600,
          max_retries: job.max_retries || 0,
          notify_on_failure: job.notify_on_failure || false,
          notify_on_success: job.notify_on_success || false,
          notification_emails: job.notification_emails || [],
          trigger_on_complete: job.trigger_on_complete || null,
        });

        setSelectedProjectId(job.project?.id);
        setSelectedEnvironmentId(job.environment?.id);

        // Load per-model configs
        if (job.model_configs && Object.keys(job.model_configs).length > 0) {
          setModelConfigs(job.model_configs);
        }

        if (type === TASK_TYPES.CRON) {
          const cronExpr =
            job.periodic_task_details?.cron?.cron_expression || DEFAULT_CRON;
          dispatchCron({ type: "set_values", value: cronExpr });
        } else {
          form.setFieldsValue({
            period: job.periodic_task_details?.interval?.period || "Minutes",
            every: job.periodic_task_details?.interval?.every || 30,
          });
        }
      })
      .catch((err) => {
        console.error("Failed to load job", err);
        notify({ error: err });
      })
      .finally(() => setLoading(false));
  }, [open, selectedJobDeployId]);

  /* ─── reset on close ─── */
  const handleClose = useCallback(() => {
    form.resetFields();
    dispatchCron({ type: "reset" });
    setCronError(null);
    setTaskType(TASK_TYPES.CRON);
    setModelConfigs({});
    setSelectedProjectId(projectId);
    setSelectedEnvironmentId(null);
    setModelConfigActiveKey([]);
    setOpen(false);
  }, [form, setOpen, projectId]);

  /* ─── submit ─── */
  const handleSubmit = useCallback(async () => {
    try {
      const values = await form.validateFields();

      // Validate cron expression if cron type
      if (taskType === TASK_TYPES.CRON && cronError) {
        notify({
          type: "error",
          message: "Invalid cron expression",
          description: cronError.description,
        });
        return;
      }

      setSubmitting(true);

      const payload = {
        task_type: taskType,
        task_name: values.task_name,
        environment: values.environment,
        description: values.description || "",
        enabled: values.enabled ?? true,
        // Per-model deployment configuration
        model_configs: modelConfigs,
        // Execution controls
        run_timeout_seconds: values.run_timeout_seconds || 0,
        max_retries: values.max_retries || 0,
        // Notifications
        notify_on_failure: values.notify_on_failure || false,
        notify_on_success: values.notify_on_success || false,
        notification_emails: values.notification_emails || [],
        // Job chaining
        trigger_on_complete: values.trigger_on_complete || null,
      };

      if (taskType === TASK_TYPES.CRON) {
        payload.cron_expression = cronValues.cronValue;
      } else {
        payload.every = values.every;
        payload.period = values.period;
      }

      const projId = values.project;
      if (isEditMode) {
        await updateTask(projId, selectedJobDeployId, payload);
        notify({ type: "success", message: "Job updated successfully" });
      } else {
        await createTask(payload, projId);
        notify({ type: "success", message: "Job created successfully" });
      }

      setIsJobListModified(true);
      handleClose();
    } catch (err) {
      if (err?.errorFields) {
        // Form validation error — Ant Design handles display
        return;
      }
      console.error("Failed to save job", err);
      notify({ error: err });
    } finally {
      setSubmitting(false);
    }
  }, [
    form,
    taskType,
    cronValues,
    cronError,
    modelConfigs,
    isEditMode,
    selectedJobDeployId,
    createTask,
    updateTask,
    setIsJobListModified,
    handleClose,
    notify,
  ]);

  return (
    <Drawer
      title={
        <Space>
          <span>{isEditMode ? "Edit Job" : "Create Job"}</span>
          <Button
            type="text"
            size="small"
            icon={drawerExpanded ? <ShrinkOutlined /> : <ExpandAltOutlined />}
            onClick={() => setDrawerExpanded(!drawerExpanded)}
            title={drawerExpanded ? "Shrink" : "Expand"}
          />
        </Space>
      }
      placement="right"
      width={drawerExpanded ? "90vw" : 720}
      open={open}
      onClose={handleClose}
      maskClosable={false}
      destroyOnClose
      extra={
        <Space>
          <Button onClick={handleClose}>Cancel</Button>
          <Button
            type="primary"
            loading={submitting}
            disabled={!canWrite}
            onClick={handleSubmit}
          >
            {isEditMode ? "Update" : "Create"}
          </Button>
        </Space>
      }
    >
      {loading ? (
        <div style={{ textAlign: "center", padding: 48 }}>
          <Spin size="large" />
        </div>
      ) : (
        <Form
          form={form}
          layout="vertical"
          className="job-deploy-form"
          initialValues={{
            enabled: true,
            run_timeout_seconds: 3600,
            max_retries: 0,
            notify_on_failure: false,
            notify_on_success: false,
            notification_emails: userEmail ? [userEmail] : [],
            trigger_on_complete: null,
          }}
        >
          {/* ─── Basic Info ─── */}
          <Form.Item
            label="Job Name"
            name="task_name"
            rules={[{ required: true, message: "Job name is required" }]}
          >
            <Input
              placeholder="e.g., daily-transform-sync"
              disabled={!canWrite}
            />
          </Form.Item>

          <Form.Item label="Description" name="description">
            <TextArea
              rows={2}
              placeholder="Describe what this job does"
              disabled={!canWrite}
            />
          </Form.Item>

          <Form.Item
            label="Project"
            name="project"
            rules={[{ required: true, message: "Please select a project" }]}
          >
            <Select
              showSearch
              placeholder="Select project"
              options={projectOptions}
              optionFilterProp="label"
              disabled={!canWrite}
              onChange={(val) => {
                setSelectedProjectId(val);
                // Reset model configs when project changes
                setModelConfigs({});
              }}
            />
          </Form.Item>

          <Form.Item
            label="Environment"
            name="environment"
            rules={[
              { required: true, message: "Please select an environment" },
            ]}
          >
            <Select
              showSearch
              placeholder="Select environment"
              options={envOptions}
              optionFilterProp="label"
              disabled={!canWrite}
              onChange={setSelectedEnvironmentId}
            />
          </Form.Item>

          <Form.Item label="Enabled" name="enabled" valuePropName="checked">
            <Switch
              disabled={!canWrite}
              checkedChildren="Active"
              unCheckedChildren="Paused"
            />
          </Form.Item>

          <Divider />

          {/* ─── Schedule ─── */}
          <Collapse
            expandIconPosition="end"
            className="advanced-settings-collapse"
            items={[
              {
                key: "schedule",
                label: (
                  <Typography.Text strong>
                    <ClockCircleOutlined style={{ marginRight: 8 }} />
                    Schedule
                  </Typography.Text>
                ),
                children: (
                  <div style={{ padding: "12px 16px" }}>
                    <Segmented
                      block
                      value={taskType}
                      onChange={setTaskType}
                      disabled={!canWrite}
                      options={[
                        {
                          label: (
                            <Space>
                              <FieldTimeOutlined />
                              Cron
                            </Space>
                          ),
                          value: TASK_TYPES.CRON,
                        },
                        {
                          label: (
                            <Space>
                              <ClockCircleOutlined />
                              Interval
                            </Space>
                          ),
                          value: TASK_TYPES.INTERVAL,
                        },
                      ]}
                      style={{ marginBottom: 16 }}
                    />

                    {taskType === TASK_TYPES.CRON ? (
                      <CronFields
                        canWrite={canWrite}
                        values={cronValues}
                        dispatchValues={dispatchCron}
                        error={cronError}
                        setError={setCronError}
                      />
                    ) : (
                      <IntervalFields canWrite={canWrite} />
                    )}
                  </div>
                ),
              },
            ]}
          />

          <Divider />

          {/* ─── Model Configuration ─── */}
          <Collapse
            expandIconPosition="end"
            activeKey={modelConfigActiveKey}
            onChange={setModelConfigActiveKey}
            className="advanced-settings-collapse"
            items={[
              {
                key: "model-config",
                label: (
                  <Typography.Text strong>
                    <ApartmentOutlined style={{ marginRight: 8 }} />
                    Model Configuration
                  </Typography.Text>
                ),
                children: (
                  <div style={{ padding: "12px 16px" }}>
                    <ModelConfigsTable
                      models={modelNames}
                      modelConfigs={modelConfigs}
                      onModelConfigsChange={setModelConfigs}
                      projectId={selectedProjectId}
                      environmentId={selectedEnvironmentId}
                      disabled={!canWrite}
                      loading={modelsLoading}
                      expanded={drawerExpanded}
                    />
                  </div>
                ),
              },
            ]}
          />

          <Divider />

          {/* ─── Advanced Settings ─── */}
          <Collapse
            expandIconPosition="end"
            className="advanced-settings-collapse"
            items={[
              {
                key: "advanced",
                label: (
                  <Typography.Text strong>
                    <SettingOutlined style={{ marginRight: 8 }} />
                    Advanced Settings
                  </Typography.Text>
                ),
                children: (
                  <div className="advanced-settings-content">
                    {/* Execution controls */}
                    <div className="advanced-settings-section">
                      <Typography.Text
                        type="secondary"
                        className="advanced-settings-section-title"
                      >
                        <RetweetOutlined style={{ marginRight: 4 }} />
                        Execution Controls
                      </Typography.Text>

                      <div style={{ display: "flex", gap: 16 }}>
                        <Form.Item
                          label="Max Retries"
                          name="max_retries"
                          tooltip="Number of automatic retries on failure."
                          style={{ flex: 1 }}
                        >
                          <InputNumber
                            min={0}
                            max={5}
                            style={{ width: "100%" }}
                            disabled={!canWrite}
                          />
                        </Form.Item>

                        <Form.Item
                          label="Run Timeout (seconds)"
                          name="run_timeout_seconds"
                          tooltip="Max run duration before the job is stopped. Also used for stuck job recovery."
                          style={{ flex: 1 }}
                        >
                          <InputNumber
                            min={0}
                            step={60}
                            style={{ width: "100%" }}
                            placeholder="3600 (1 hour)"
                            disabled={!canWrite}
                          />
                        </Form.Item>
                      </div>
                    </div>

                    <Divider dashed />

                    {/* Notifications */}
                    <div className="advanced-settings-section">
                      <Typography.Text
                        type="secondary"
                        className="advanced-settings-section-title"
                      >
                        <BellOutlined style={{ marginRight: 4 }} />
                        Notifications
                      </Typography.Text>

                      <div style={{ display: "flex", gap: 16 }}>
                        <Form.Item
                          label="Notify on Failure"
                          name="notify_on_failure"
                          valuePropName="checked"
                          style={{ flex: 1 }}
                        >
                          <Switch disabled={!canWrite} />
                        </Form.Item>

                        <Form.Item
                          label="Notify on Success"
                          name="notify_on_success"
                          valuePropName="checked"
                          style={{ flex: 1 }}
                        >
                          <Switch disabled={!canWrite} />
                        </Form.Item>
                      </div>

                      <Form.Item
                        label="Notification Emails"
                        name="notification_emails"
                        tooltip="Enter email addresses"
                        rules={[
                          {
                            validator: (_, value) => {
                              if (!value || value.length === 0)
                                return Promise.resolve();
                              const invalid = value.filter(
                                (e) => !EMAIL_REGEX.test(e)
                              );
                              if (invalid.length > 0) {
                                return Promise.reject(
                                  new Error(
                                    `Invalid email(s): ${invalid.join(", ")}`
                                  )
                                );
                              }
                              return Promise.resolve();
                            },
                          },
                        ]}
                      >
                        <Select
                          mode="tags"
                          placeholder="Enter email addresses"
                          disabled={!canWrite}
                          tokenSeparators={[","]}
                        />
                      </Form.Item>
                    </div>

                    {/* Job chaining */}
                    <div className="advanced-settings-section">
                      <Typography.Text
                        type="secondary"
                        className="advanced-settings-section-title"
                      >
                        <LinkOutlined style={{ marginRight: 4 }} />
                        Job Chaining
                      </Typography.Text>

                      <Form.Item
                        label="Trigger on Complete"
                        name="trigger_on_complete"
                        tooltip="Run another job when this one completes successfully."
                      >
                        <Select
                          allowClear
                          showSearch
                          placeholder="None (no chaining)"
                          options={jobOptions}
                          optionFilterProp="label"
                          disabled={!canWrite}
                        />
                      </Form.Item>
                    </div>
                  </div>
                ),
              },
            ]}
          />
        </Form>
      )}
    </Drawer>
  );
});

JobDeploy.propTypes = {
  open: PropTypes.bool.isRequired,
  setOpen: PropTypes.func.isRequired,
  selectedJobDeployId: PropTypes.oneOfType([
    PropTypes.string,
    PropTypes.number,
  ]),
  setIsJobListModified: PropTypes.func.isRequired,
};

JobDeploy.displayName = "JobDeploy";

export { JobDeploy };
