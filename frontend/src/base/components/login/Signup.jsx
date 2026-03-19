import { useState, useRef } from "react";
import { useNavigate, Link } from "react-router-dom";
import { Form, Input, Button, Typography, Card, Alert } from "antd";
import { LockOutlined, MailOutlined, UserOutlined } from "@ant-design/icons";
import axios from "axios";
import "./Login.css";

const { Title, Text } = Typography;

function Signup() {
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
      const res = await axios.post("/api/v1/signup", {
        email: values.email,
        password: values.password,
        confirm_password: values.confirmPassword,
        display_name: values.displayName || "",
      });
      if (res.status === 201) {
        navigate("/project/list");
      }
    } catch (err) {
      const errorData = err?.response?.data?.error;
      let message = "Signup failed. Please try again.";
      if (typeof errorData === "string") {
        message = errorData;
      } else if (typeof errorData === "object") {
        const firstError = Object.values(errorData)[0];
        message = Array.isArray(firstError) ? firstError[0] : firstError;
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
          Create Account
        </Title>
        {error && (
          <Alert
            message={error}
            type="error"
            showIcon
            style={{ marginBottom: 16 }}
          />
        )}
        <Form name="signup" onFinish={onFinish} autoComplete="off" size="large">
          <Form.Item
            name="email"
            rules={[
              { required: true, message: "Please enter your email" },
              { type: "email", message: "Please enter a valid email" },
            ]}
          >
            <Input prefix={<MailOutlined />} placeholder="Email" />
          </Form.Item>
          <Form.Item name="displayName">
            <Input
              prefix={<UserOutlined />}
              placeholder="Display Name (optional)"
            />
          </Form.Item>
          <Form.Item
            name="password"
            rules={[
              { required: true, message: "Please enter your password" },
              { min: 8, message: "Password must be at least 8 characters" },
            ]}
          >
            <Input.Password prefix={<LockOutlined />} placeholder="Password" />
          </Form.Item>
          <Form.Item
            name="confirmPassword"
            dependencies={["password"]}
            rules={[
              { required: true, message: "Please confirm your password" },
              ({ getFieldValue }) => ({
                validator(_, value) {
                  if (!value || getFieldValue("password") === value) {
                    return Promise.resolve();
                  }
                  return Promise.reject(new Error("Passwords do not match"));
                },
              }),
            ]}
          >
            <Input.Password
              prefix={<LockOutlined />}
              placeholder="Confirm Password"
            />
          </Form.Item>
          <Form.Item>
            <Button
              type="primary"
              htmlType="submit"
              loading={loading}
              disabled={loading}
              block
            >
              Sign Up
            </Button>
          </Form.Item>
        </Form>
        <div style={{ textAlign: "center" }}>
          <Text>Already have an account? </Text>
          <Link to="/login">Log in</Link>
        </div>
      </Card>
    </div>
  );
}

export { Signup };
