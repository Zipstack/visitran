import PropTypes from "prop-types";
import { Spin } from "antd";

import "./spinner-loader.css";

const SPINNER_SIZE = {
  SMALL: "small",
  LARGE: "large",
};
const SPINNER_ALIGNMENT = {
  DEFAULT: "default",
};

function SpinnerLoader({ size = "default", delay = 0, text = "" }) {
  return (
    <div className="height-100 center">
      <Spin size={size} delay={delay} tip={text} />
    </div>
  );
}

SpinnerLoader.propTypes = {
  size: PropTypes.string,
  delay: PropTypes.number,
  text: PropTypes.string,
};

export { SpinnerLoader, SPINNER_SIZE, SPINNER_ALIGNMENT };
