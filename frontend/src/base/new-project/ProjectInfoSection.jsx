import { memo } from "react";
import { Form, Input, Typography } from "antd";

import {
  collapseSpaces,
  validateFormFieldDescription,
  validateFormFieldName,
} from "../components/environment/helper";

const ProjectInfoSection = memo(() => (
  <div>
    <Typography className="sectionTitle">Project Info</Typography>

    <Form.Item
      name="project_name"
      label="Project Name"
      getValueFromEvent={({ target: { value } }) =>
        // collapse any run of 2+ spaces only when followed by non-space
        collapseSpaces(value)
      }
      rules={[
        { required: true, message: "Please enter the project name" },
        { validator: validateFormFieldName },
      ]}
    >
      <Input />
    </Form.Item>

    <Form.Item
      name="description"
      label="Project Description"
      rules={[
        { required: true, message: "Please enter the project description" },
        { validator: validateFormFieldDescription },
      ]}
    >
      <Input.TextArea rows={2} />
    </Form.Item>
  </div>
));

ProjectInfoSection.displayName = "ProjectInfoSection";

export { ProjectInfoSection };
