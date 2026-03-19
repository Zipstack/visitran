import { useState, useRef, useEffect } from "react";
import { useParams, Link } from "react-router-dom";
import { Form, Input, Button, Typography, Card, Alert, Spin } from "antd";
import { LockOutlined } from "@ant-design/icons";
import axios from "axios";
import "./Login.css";

const { Title } = Typography;

function ResetPassword() {
  const { uid, token } = useParams();
  const [loading, setLoading] = useState(false);
  const [validating, setValidating] = useState(true);
  const [tokenValid, setTokenValid] = useState(false);
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");

  useEffect(() => {
    const validateToken = async () => {
      try {
        const res = await axios.post("/api/v1/validate-reset-token", {
          uid,
          token,
        });
        if (res?.status === 200 && res?.data?.valid) {
          setTokenValid(true);
        } else {
          setError("This reset link is invalid or has expired.");
        }
      } catch {
        setError("This reset link is invalid or has expired.");
      } finally {
        setValidating(false);
      }
    };
    validateToken();
  }, [uid, token]);
  const inFlightRef = useRef(false);

  const onFinish = async (values) => {
    if (inFlightRef.current) return;
    inFlightRef.current = true;
    setLoading(true);
    setError("");
    setSuccess("");
    try {
      const res = await axios.post("/api/v1/reset-password", {
        uid,
        token,
        password: values.password,
        confirm_password: values.confirmPassword,
      });
      if (res?.status === 200) {
        setSuccess(res?.data?.message || "Password reset successful.");
      }
    } catch (err) {
      const resp = err?.response?.data;
      const errorData = resp?.error || resp?.detail;
      let message = "Password reset failed. Please try again.";
      if (typeof errorData === "string") {
        message = errorData;
      } else if (errorData && typeof errorData === "object") {
        const values = Object.values(errorData);
        const firstError = values?.[0];
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

  if (validating) {
    return (
      <div className="login-container">
        <Card className="login-card" style={{ textAlign: "center" }}>
          <Spin size="large" />
          <p style={{ marginTop: 16 }}>Validating reset link...</p>
        </Card>
      </div>
    );
  }

  return (
    <div className="login-container">
      <Card className="login-card">
        <Title level={3} style={{ textAlign: "center", marginBottom: 24 }}>
          Reset Password
        </Title>
        {error && (
          <Alert
            message={error}
            type="error"
            showIcon
            style={{ marginBottom: 16 }}
          />
        )}
        {success ? (
          <div style={{ textAlign: "center" }}>
            <Alert
              message={success}
              type="success"
              showIcon
              style={{ marginBottom: 16 }}
            />
            <Link to="/login">Go to Login</Link>
          </div>
        ) : tokenValid ? (
          <Form
            name="reset-password"
            onFinish={onFinish}
            autoComplete="off"
            size="large"
          >
            <Form.Item
              name="password"
              rules={[
                { required: true, message: "Please enter your new password" },
                {
                  min: 8,
                  message: "Password must be at least 8 characters",
                },
              ]}
            >
              <Input.Password
                prefix={<LockOutlined />}
                placeholder="New Password"
              />
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
                Reset Password
              </Button>
            </Form.Item>
          </Form>
        ) : (
          <div style={{ textAlign: "center" }}>
            <Link to="/login">Back to Login</Link>
          </div>
        )}
        {!success && tokenValid && (
          <div style={{ textAlign: "center" }}>
            <Link to="/login">Back to Login</Link>
          </div>
        )}
      </Card>
    </div>
  );
}

export { ResetPassword };
