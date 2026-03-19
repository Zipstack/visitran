import { useState, useRef } from "react";
import { Link } from "react-router-dom";
import { Form, Input, Button, Typography, Card, Alert } from "antd";
import { LockOutlined, MailOutlined } from "@ant-design/icons";
import axios from "axios";
import "./Login.css";

const { Title, Text } = Typography;

function Login() {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const inFlightRef = useRef(false);

  const onFinish = async (values) => {
    if (inFlightRef.current) return;
    inFlightRef.current = true;
    setLoading(true);
    setError("");
    try {
      const res = await axios.post("/api/v1/login", values);
      if (res.status === 200) {
        window.location.href = "/";
      }
    } catch (err) {
      const message =
        err?.response?.data?.error || "Login failed. Please try again.";
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
          Visitran
        </Title>
        {error && (
          <Alert
            message={error}
            type="error"
            showIcon
            style={{ marginBottom: 16 }}
          />
        )}
        <Form name="login" onFinish={onFinish} autoComplete="off" size="large">
          <Form.Item
            name="email"
            rules={[
              { required: true, message: "Please enter your email" },
              { type: "email", message: "Please enter a valid email" },
            ]}
          >
            <Input prefix={<MailOutlined />} placeholder="Email" />
          </Form.Item>
          <Form.Item
            name="password"
            rules={[{ required: true, message: "Please enter your password" }]}
          >
            <Input.Password prefix={<LockOutlined />} placeholder="Password" />
          </Form.Item>
          <Form.Item>
            <Button
              type="primary"
              htmlType="submit"
              loading={loading}
              disabled={loading}
              block
            >
              Log in
            </Button>
          </Form.Item>
        </Form>
        <div style={{ textAlign: "center", marginBottom: 8 }}>
          <Link to="/forgot-password">Forgot password?</Link>
        </div>
        <div style={{ textAlign: "center" }}>
          <Text>Don&apos;t have an account? </Text>
          <Link to="/signup">Sign up</Link>
        </div>
      </Card>
    </div>
  );
}

export { Login };
