import PropTypes from "prop-types";
import {
  Form,
  Input,
  Select,
  Radio,
  Switch,
  Button,
  Row,
  Space,
  Typography,
  Divider,
} from "antd";

import { CronFields } from "./CronFields";
import { IntervalFields } from "./IntervalFields";
import {
  collapseSpaces,
  validateFormFieldDescription,
  validateFormFieldName,
} from "../../base/components/environment/helper";

const filterOption = (input, option) =>
  option.label.toLowerCase().includes(input.toLowerCase());

const JobDeployForm = ({
  loading,
  initialValues,
  projects,
  envs,
  onFinish,
  onDelete,
  onCancel,
  isEdit,
  canWrite,
  canDelete,
  values,
  dispatchValues,
  error,
  setError,
}) => (
  <div className="height-100 pad-18">
    <Form
      layout="vertical"
      initialValues={{ task_type: "interval", enabled: true, ...initialValues }}
      onFinish={onFinish}
      className="job-deploy-form"
    >
      {/* ───── General ───── */}
      <Typography className="sectionTitle">General</Typography>

      <Form.Item
        label="Job name"
        name="task_name"
        getValueFromEvent={({ target: { value } }) =>
          // collapse any run of 2+ spaces only when followed by non-space
          collapseSpaces(value)
        }
        rules={[
          { required: true, message: "Please enter the job name" },
          { validator: validateFormFieldName },
        ]}
      >
        <Input disabled={isEdit || !canWrite} />
      </Form.Item>

      <Form.Item
        label="Description"
        name="description"
        rules={[{ validator: validateFormFieldDescription }]}
      >
        <Input.TextArea rows={2} disabled={isEdit || !canWrite} />
      </Form.Item>

      <Form.Item label="Project" name="project" rules={[{ required: true }]}>
        <Select
          showSearch
          placeholder="Select project"
          options={projects}
          filterOption={filterOption}
          disabled={isEdit || !canWrite}
        />
      </Form.Item>

      <Form.Item
        label="Environment"
        name="environment"
        rules={[{ required: true }]}
      >
        <Select
          showSearch
          placeholder="Select environment"
          options={envs}
          filterOption={filterOption}
          disabled={isEdit || !canWrite}
        />
      </Form.Item>

      <Form.Item label="Schedule type" name="task_type">
        <Radio.Group disabled={isEdit || !canWrite}>
          <Radio value="interval">Interval</Radio>
          <Radio value="cron">Cron</Radio>
        </Radio.Group>
      </Form.Item>

      <Divider />

      {/* ───── Dynamic schedule fields ───── */}

      <Form.Item
        shouldUpdate={(prev, cur) => prev.task_type !== cur.task_type}
        noStyle
      >
        {({ getFieldValue }) =>
          getFieldValue("task_type") === "cron" ? (
            <CronFields
              canWrite={canWrite}
              values={values}
              dispatchValues={dispatchValues}
              error={error}
              setError={setError}
            />
          ) : (
            <IntervalFields canWrite={canWrite} />
          )
        }
      </Form.Item>
      <Divider />
      <Form.Item
        label="Enable"
        name="enabled"
        valuePropName="checked"
        disabled={!canWrite}
      >
        <Switch />
      </Form.Item>

      <Divider />

      <Row justify="end">
        <Space>
          <Button onClick={onCancel}>Cancel</Button>
          {onDelete && (
            <Button danger onClick={onDelete} disabled={!canDelete}>
              Delete
            </Button>
          )}
          <Button type="primary" htmlType="submit" loading={loading}>
            {isEdit ? "Update" : "Save"}
          </Button>
        </Space>
      </Row>
    </Form>
  </div>
);

JobDeployForm.propTypes = {
  loading: PropTypes.bool,
  initialValues: PropTypes.object,
  projects: PropTypes.array,
  envs: PropTypes.array,
  values: PropTypes.object,
  dispatchValues: PropTypes.func,
  onFinish: PropTypes.func,
  onDelete: PropTypes.func,
  onCancel: PropTypes.func,
  isEdit: PropTypes.bool,
  canWrite: PropTypes.bool.isRequired,
  canDelete: PropTypes.bool.isRequired,
  error: PropTypes.object,
  setError: PropTypes.func,
};

export { JobDeployForm };
