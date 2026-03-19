import PropTypes from "prop-types";
import { Button, Space } from "antd";

function ActionsView({ data }) {
  return (
    <Space>
      {data.map((item, index) => (
        <Button
          key={index}
          type={item.button_type === "PRIMARY" ? "primary" : "default"}
        >
          {item.text}
        </Button>
      ))}
    </Space>
  );
}

ActionsView.propTypes = {
  data: PropTypes.arrayOf(
    PropTypes.shape({
      button_type: PropTypes.string.isRequired,
      action_type: PropTypes.string,
      text: PropTypes.string.isRequired,
      api_name: PropTypes.string,
      link: PropTypes.string,
    })
  ).isRequired,
};

export { ActionsView };
