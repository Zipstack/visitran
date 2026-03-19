import { memo } from "react";
import PropTypes from "prop-types";
import { Typography, Button } from "antd";

const FeatureCard = memo(
  ({ heading, description, clickFunc, buttonText, icon, className }) => {
    return (
      <div className={`feature-card ${className}`}>
        <div className="feature-icon">{icon}</div>
        <Typography.Title level={4}>{heading}</Typography.Title>
        <Typography.Text>{description}</Typography.Text>
        <Button type="primary" className="feature-btn" onClick={clickFunc}>
          {buttonText}
        </Button>
      </div>
    );
  }
);
FeatureCard.displayName = "FeatureCard";

FeatureCard.propTypes = {
  clickFunc: PropTypes.func.isRequired,
  heading: PropTypes.string,
  description: PropTypes.string,
  buttonText: PropTypes.string,
  icon: PropTypes.any,
  className: PropTypes.string,
};

export default FeatureCard;
