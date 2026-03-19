import { memo, useState, useEffect } from "react";
import PropTypes from "prop-types";
import { Space, Button, Typography } from "antd";
import { CheckCircleOutlined, CloseOutlined } from "@ant-design/icons";

import { useUserStore } from "../../store/user-store";

const { Text } = Typography;

const OnboardingProgress = memo(function OnboardingProgress({
  visible = false,
  currentStep = 0,
  totalSteps = 4,
  onSkip,
}) {
  const [showProgress, setShowProgress] = useState(false);

  const currentTheme = useUserStore(
    (state) => state?.userDetails?.currentTheme
  );

  useEffect(() => {
    if (visible) {
      const timer = setTimeout(() => {
        setShowProgress(true);
      }, 100);
      return () => clearTimeout(timer);
    } else {
      setShowProgress(false);
    }
  }, [visible]);

  if (!visible) {
    return null;
  }

  return (
    <div
      className={`onboarding-progress ${
        currentTheme === "dark" ? "dark" : "light"
      } ${showProgress ? "show" : ""}`}
    >
      <Space size={12}>
        <CheckCircleOutlined style={{ color: "#0ea5e9", fontSize: "16px" }} />
        <div>
          <Text
            strong
            className={`progress-title ${
              currentTheme === "dark" ? "dark" : "light"
            }`}
          >
            Onboarding Progress
          </Text>
          <div>
            <Text
              className={`progress-step ${
                currentTheme === "dark" ? "dark" : "light"
              }`}
            >
              Step {currentStep} of {totalSteps}
            </Text>
          </div>
        </div>
      </Space>

      <Space size={8} align="center">
        <div className="progress-badge">
          {currentStep}/{totalSteps}
        </div>

        <Button
          type="text"
          size="small"
          icon={<CloseOutlined />}
          onClick={onSkip}
          className={`skip-button ${
            currentTheme === "dark" ? "dark" : "light"
          }`}
        >
          Skip
        </Button>
      </Space>
    </div>
  );
});

OnboardingProgress.propTypes = {
  visible: PropTypes.bool,
  currentStep: PropTypes.number,
  totalSteps: PropTypes.number,
  onSkip: PropTypes.func.isRequired,
};

export { OnboardingProgress };
