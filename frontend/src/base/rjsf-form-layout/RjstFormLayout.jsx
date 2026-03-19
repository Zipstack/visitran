import Form from "@rjsf/antd";
import PropTypes from "prop-types";
import validator from "@rjsf/validator-ajv8";

function RjsfFormLayout({
  children,
  schema = {},
  formData = {},
  handleChange = () => {},
  handleSubmit = () => {},
  uiSchema = {},
}) {
  return (
    <Form
      className="compactForm"
      schema={schema}
      validator={validator}
      formData={formData}
      onChange={handleChange}
      onSubmit={handleSubmit}
      uiSchema={uiSchema}
      omitExtraData
      liveOmit
    >
      {children}
    </Form>
  );
}

RjsfFormLayout.propTypes = {
  children: PropTypes.oneOfType([PropTypes.node, PropTypes.element]),
  schema: PropTypes.object,
  formData: PropTypes.object,
  uiSchema: PropTypes.object,
  handleChange: PropTypes.func,
  handleSubmit: PropTypes.func,
};

export { RjsfFormLayout };
