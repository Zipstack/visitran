import { Typography, Button, Space } from "antd";
import { HomeOutlined, ReloadOutlined, BugOutlined } from "@ant-design/icons";
import "./global-error-fallback.css";

const { Title, Paragraph } = Typography;

const GlobalErrorFallback = () => {
  const handleGoHome = () => {
    window.location.href = "/project/list";
  };

  const handleReload = () => {
    window.location.reload();
  };

  return (
    <div className="global-error-fallback-overlay">
      <div className="global-error-fallback-modal">
        <div className="global-error-fallback-content">
          <BugOutlined className="global-error-fallback-icon" />

          <Title level={3} className="global-error-fallback-title">
            Oops! Something went wrong
          </Title>

          <Paragraph className="global-error-fallback-description">
            We encountered an unexpected error. Don&apos;t worry - your work is
            safe. Please try one of the options below to continue.
          </Paragraph>

          <Space
            direction="vertical"
            size="middle"
            className="global-error-fallback-actions"
          >
            <Button
              type="primary"
              size="large"
              icon={<HomeOutlined />}
              onClick={handleGoHome}
              className="global-error-fallback-button"
            >
              Go to Home
            </Button>

            <Button
              size="large"
              icon={<ReloadOutlined />}
              onClick={handleReload}
              className="global-error-fallback-button"
            >
              Reload Page
            </Button>
          </Space>
        </div>
      </div>
    </div>
  );
};

export { GlobalErrorFallback };
