import { Button } from "antd";
import PropTypes from "prop-types";

function PreviewTimeTravel({ isLoading, disabled, disablePreview }) {
  return !disabled ? (
    <Button
      onClick={() => disablePreview()}
      className="toolbar-item"
      disabled={isLoading || disabled}
    >
      Disable Preview
    </Button>
  ) : null;
}

PreviewTimeTravel.propTypes = {
  isLoading: PropTypes.bool.isRequired,
  disabled: PropTypes.bool.isRequired,
  disablePreview: PropTypes.func,
};

export { PreviewTimeTravel };
