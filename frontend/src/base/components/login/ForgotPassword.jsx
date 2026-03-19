import { useState, useRef } from "react";
import { Link, useNavigate } from "react-router-dom";
import { Form, Input, Button, Typography, Card, Alert } from "antd";
import { MailOutlined } from "@ant-design/icons";
import axios from "axios";
import "./Login.css";

const { Title } = Typography;

function ForgotPassword() {
  const navigate = useNavigate();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const inFlightRef = useRef(false);

  const onFinish = async (values) => {
    if (inFlightRef.current) return;
    inFlightRef.current = true;
    setLoading(true);
    setError("");
    try {
      const res = await axios.post("/api/v1/forgot-password", {
        email: values.email,
      });
      if (res?.status === 200 && res?.data?.reset_url) {
        // Extract the path from the full reset URL and navigate directly
        const url = new URL(res.data.reset_url);
        navigate(url.pathname);
      }
    } catch (err) {
      const resp = err?.response?.data;
      const errorData = resp?.error || resp?.detail;
      let message = "Request failed. Please try again.";
      if (typeof errorData === "string") {
        message = errorData;
      } else if (errorData && typeof errorData === "object") {
        const firstError = Object.values(errorData)?.[0];
        message = Array.isArray(firstError)
          ? firstError?.[0] || message
          : firstError || message;
      }
      setError(message);
    } finally {
      inFlightRef.current = false;
      setLoading(false);
    }
  };

  return (
    <div className="login-container">
      <Card className="login-card">
        <Title level={3} style={{ textAlign: "center", marginBottom: 24 }}>
          Forgot Password
        </Title>
        {error && (
          <Alert
            message={error}
            type="error"
            showIcon
            style={{ marginBottom: 16 }}
          />
        )}
        <Form
          name="forgot-password"
          onFinish={onFinish}
          autoComplete="off"
          size="large"
        >
          <Form.Item
            name="email"
            rules={[
              { required: true, message: "Please enter your email" },
              { type: "email", message: "Please enter a valid email" },
            ]}
          >
            <Input prefix={<MailOutlined />} placeholder="Email" />
          </Form.Item>
          <Form.Item>
            <Button
              type="primary"
              htmlType="submit"
              loading={loading}
              disabled={loading}
              block
            >
              Reset Password
            </Button>
          </Form.Item>
        </Form>
        <div style={{ textAlign: "center" }}>
          <Link to="/login">Back to Login</Link>
        </div>
      </Card>
    </div>
  );
}

export { ForgotPassword };
