import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Form, Input, Button, Typography, Space } from "antd";
import { CopyOutlined, CheckOutlined, ReloadOutlined } from "@ant-design/icons";
import Cookies from "js-cookie";
import isEqual from "lodash/isEqual.js";
import { useNavigate } from "react-router-dom";

import { useAxiosPrivate } from "../../../../service/axios-service";
import { orgStore } from "../../../../store/org-store";
import { useSessionStore } from "../../../../store/session-store";
import { useNotificationService } from "../../../../service/notification-service";

import "./Profile.css";

const nameSanitizer = (v = "") => v.replace(/\s{2,}(?=\S)/g, " "); // collapse multi-spaces just before text

const Profile = () => {
  const [form] = Form.useForm();
  const axios = useAxiosPrivate();
  const navigate = useNavigate();
  const { selectedOrgId } = orgStore();
  const { sessionDetails } = useSessionStore();
  const { notify } = useNotificationService();
  const csrfToken = Cookies.get("csrftoken");

  /* ------------------------------------------------------------------ */
  /*                         local component state                      */
  /* ------------------------------------------------------------------ */
  const [copySuccess, setCopySuccess] = useState(false);
  const [loading, setLoading] = useState(false);
  const initialRef = useRef({ first_name: "", last_name: "", token: "" });

  /* ------------------------------------------------------------------ */
  /*                       derived / watched values                     */
  /* ------------------------------------------------------------------ */
  const firstName = Form.useWatch("first_name", form);
  const lastName = Form.useWatch("last_name", form);
  const token = Form.useWatch("token", form);

  const isModified = useMemo(
    () =>
      !isEqual(
        { first_name: firstName, last_name: lastName, token },
        initialRef.current
      ),
    [firstName, lastName, token]
  );

  /* ------------------------------------------------------------------ */
  /*                         helpers / API calls                        */
  /* ------------------------------------------------------------------ */
  const fetchProfile = useCallback(async () => {
    try {
      const { data } = await axios.get(
        `/api/v1/visitran/${selectedOrgId || "default_org"}/profile`
      );
      form.setFieldsValue({ ...data, role: sessionDetails.user_role });
      const { first_name, last_name, token } = data;
      initialRef.current = { first_name, last_name, token };
    } catch (error) {
      notify({ error });
    }
  }, [selectedOrgId, form, sessionDetails.user_role]);

  const saveProfile = useCallback(
    async (values) => {
      if (!isModified) {
        notify({
          type: "info",
          message: "No Changes Detected",
          description: "Your profile remains unchanged.",
        });
        return;
      }

      try {
        await axios.put(
          `/api/v1/visitran/${selectedOrgId || "default_org"}/profile/update`,
          {
            first_name: values.first_name,
            last_name: values.last_name,
            token: values.token,
          },
          { headers: { "X-CSRFToken": csrfToken } }
        );
        notify({
          type: "success",
          message: "Profile Updated",
          description: "Your changes have been successfully saved.",
        });
      } catch (error) {
        notify({ error });
      }
    },
    [selectedOrgId, isModified]
  );

  const regenerateToken = useCallback(async () => {
    setLoading(true);
    try {
      const { data } = await axios.post(
        `/api/v1/visitran/${selectedOrgId || "default_org"}/token/generate`,
        {},
        { headers: { "X-CSRFToken": csrfToken } }
      );
      form.setFieldsValue({ token: data.token });
    } catch (error) {
      notify({ error });
    } finally {
      setLoading(false);
    }
  }, [selectedOrgId, form]);

  const copyToClipboard = useCallback(async (text) => {
    try {
      await navigator.clipboard.writeText(text);
      setCopySuccess(true);
      setTimeout(() => setCopySuccess(false), 2000);
    } catch {
      setCopySuccess(false);
    }
  }, []);

  /* ------------------------------------------------------------------ */
  /*                               effects                              */
  /* ------------------------------------------------------------------ */
  useEffect(() => {
    fetchProfile();
  }, []);

  /* ------------------------------------------------------------------ */
  /*                              render                                */
  /* ------------------------------------------------------------------ */
  return (
    <div className="profile-wrap">
      <Space direction="vertical" size={10} className="input-wrap">
        <Typography.Title level={5}>Profile</Typography.Title>

        <Form
          form={form}
          layout="vertical"
          className="profile-form"
          onFinish={saveProfile}
        >
          {/* ------------------- first name ------------------- */}
          <Form.Item
            label="First Name"
            name="first_name"
            rules={[
              { required: true, message: "Please enter first name" },
              {
                pattern: /^[A-Za-z\s]*$/,
                message: "Only alphabets and single spaces allowed",
              },
            ]}
            getValueFromEvent={({ target: { value } }) => nameSanitizer(value)} // keep user input intact
          >
            <Input autoComplete="given-name" className="input-300" />
          </Form.Item>

          {/* ------------------- last name -------------------- */}
          <Form.Item
            label="Last Name"
            name="last_name"
            rules={[
              { required: true, message: "Please enter last name" },
              {
                pattern: /^[A-Za-z\s]*$/,
                message: "Only alphabets and single spaces allowed",
              },
            ]}
            getValueFromEvent={({ target: { value } }) => nameSanitizer(value)} // keep user input intact
          >
            <Input autoComplete="family-name" className="input-300" />
          </Form.Item>

          {/* ---------------------- email --------------------- */}
          <Form.Item
            label="Email"
            name="email"
            rules={[
              {
                type: "email",
                required: true,
                message: "Enter a valid email address",
              },
            ]}
          >
            <Input disabled className="input-300" />
          </Form.Item>

          {/* ---------------------- role ---------------------- */}
          <Form.Item
            label="Role"
            name="role"
            rules={[
              {
                required: true,
                pattern: /^(?!_)[a-z_]+(?<!_)$/,
                message:
                  "Lower-case letters & underscores only (cannot start/end with underscore)",
              },
            ]}
          >
            <Input disabled className="input-300" />
          </Form.Item>

          {/* -------------------- API token ------------------- */}
          <Form.Item
            label="API Key"
            name="token"
            rules={[
              {
                required: true,
                message:
                  "API key is required. Regenerate the key to get a new one",
              },
            ]}
          >
            <div className="token-row">
              <Form.Item name="token" noStyle>
                <Input
                  readOnly
                  className="token-input"
                  suffix={
                    token ? (
                      copySuccess ? (
                        <CheckOutlined className="icon-success" />
                      ) : (
                        <CopyOutlined
                          className="icon-copy"
                          onClick={() => copyToClipboard(token)}
                        />
                      )
                    ) : null
                  }
                />
              </Form.Item>
              <Button
                onClick={regenerateToken}
                loading={loading}
                className="token-button"
                icon={<ReloadOutlined />}
              />
            </div>
          </Form.Item>

          {/* ----------------- actions ------------------------ */}
          <Form.Item className="actions">
            <Button onClick={() => navigate(-1)}>Back</Button>
            <Button
              type="primary"
              htmlType="submit"
              className="ml-10px primary_button_style"
            >
              Save
            </Button>
          </Form.Item>
        </Form>
      </Space>
    </div>
  );
};

export { Profile };
export default Profile;
