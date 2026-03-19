import { useLocation, useNavigate } from "react-router-dom";
import { useEffect, useState } from "react";
import { Modal, Typography, Button } from "antd";
import { SettingOutlined, LoginOutlined } from "@ant-design/icons";
import "./error.css";
const ErrorPage = () => {
  const location = useLocation();
  const navigate = useNavigate();

  const [decodedError, setDecodedError] = useState("");

  useEffect(() => {
    const params = new URLSearchParams(location.search);
    const errorMessage = params.get("error_message");

    if (errorMessage) {
      try {
        let decoded = decodeURIComponent(errorMessage);
        decoded = decoded.replace(/^{?'?|"?/g, "").replace(/'?"?}?$/g, "");
        setDecodedError(decoded);
      } catch (e) {
        setDecodedError("Failed to decode error message.");
      }
    }
  }, [location.search]);

  return (
    <div>
      <Modal
        open={true}
        footer={null}
        closable={false}
        width={500}
        centered
        keyboard={true}
        maskTransitionName="fade"
        transitionName="zoom"
        aria-labelledby="error-modal-title"
        aria-describedby="error-modal-description"
        maskClosable={false}
      >
        <div className="modal-main-wrap">
          <div className="setting-icon top-left-large">
            <SettingOutlined className="icon-120 blue" />
          </div>

          <div className="setting-icon bottom-left-small">
            <SettingOutlined className="icon-60 blue" spin />
          </div>

          <div className="setting-icon right-middle">
            <SettingOutlined className="icon-80 orange" spin />
          </div>

          <div className="setting-icon bottom-left-small">
            <SettingOutlined className="icon-60 blue" spin />
          </div>

          {/* Auth Error Modal Content */}
          <div className="text-wrap">
            <Typography.Title className="error-modal-title" level={3}>
              Login cannot be completed at this time.
            </Typography.Title>

            <Typography.Paragraph className="error-modal-description">
              {decodedError || "Unknown error occurred."}
            </Typography.Paragraph>

            <Typography.Paragraph className="error-helper-text">
              Please try again. If the issue persists, contact support.
            </Typography.Paragraph>

            {/* Auth Error Action Button */}
            <div className="btn-wrap">
              <Button
                className="primary_button_style "
                type="primary"
                icon={<LoginOutlined />}
                onClick={() => navigate("/org")}
                aria-label="Try login again"
              >
                Login
              </Button>
            </div>
          </div>
        </div>
      </Modal>
    </div>
  );
};

export default ErrorPage;
