import Typography from "antd/es/typography/Typography";
import PropTypes from "prop-types";

const HelperText = ({ text }) => {
  return <Typography.Text type="danger">{text}</Typography.Text>;
};

HelperText.propTypes = {
  text: PropTypes.string,
};

export { HelperText };
