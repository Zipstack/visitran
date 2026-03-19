import { memo, useEffect, useState } from "react";
import PropTypes from "prop-types";
import { Button, Typography } from "antd";
import { CheckCircleOutlined } from "@ant-design/icons";

import { useUserStore } from "../../store/user-store";
import { VisitranAILightIcon } from "../../base/icons";
import "./OnboardingCompletionPopup.css";

const { Title, Text } = Typography;

const OnboardingCompletionPopup = memo(function OnboardingCompletionPopup({
  visible,
  onClose,
  onContinue,
}) {
  const currentTheme = useUserStore(
    (state) => state?.userDetails?.currentTheme
  );
  const [showContent, setShowContent] = useState(false);
  const [showButton, setShowButton] = useState(false);

  useEffect(() => {
    if (visible) {
      // Stagger the animations
      const timer1 = setTimeout(() => setShowContent(true), 500);
      const timer2 = setTimeout(() => setShowButton(true), 1500);

      return () => {
        clearTimeout(timer1);
        clearTimeout(timer2);
      };
    } else {
      setShowContent(false);
      setShowButton(false);
    }
  }, [visible]);

  const handleContinue = () => {
    if (onContinue) {
      onContinue();
    } else {
      onClose();
    }
  };

  if (!visible) return null;

  return (
    <>
      <div
        className={`completion-popup-container ${
          currentTheme === "dark" ? "dark" : "light"
        }`}
      >
        {/* Animated Check Mark */}
        <div className="completion-icon-container">
          <div className={`completion-icon ${visible ? "animate" : ""}`}>
            <CheckCircleOutlined />
          </div>
          <div
            className={`completion-ripple ${visible ? "animate" : ""}`}
          ></div>
          <div
            className={`completion-ripple-2 ${visible ? "animate" : ""}`}
          ></div>
        </div>

        {/* Content */}
        <div className={`completion-content ${showContent ? "show" : ""}`}>
          <Title level={2} className="completion-title">
            All Done!
          </Title>

          <Text className="completion-message">
            You&apos;ve successfully completed all onboarding steps. You&apos;re
            now ready to create your first transformation.
          </Text>
        </div>

        {/* Action Button */}
        <div className={`completion-actions ${showButton ? "show" : ""}`}>
          <Button
            type="primary"
            size="large"
            onClick={handleContinue}
            className={`completion-button ${
              currentTheme === "dark" ? "dark" : "light"
            }`}
          >
            <VisitranAILightIcon className="completion-button-icon" />
            Let&apos;s Build Your First Transformation
          </Button>
        </div>
      </div>
    </>
  );
});

OnboardingCompletionPopup.propTypes = {
  visible: PropTypes.bool.isRequired,
  onClose: PropTypes.func.isRequired,
  onContinue: PropTypes.func,
};

export { OnboardingCompletionPopup };
