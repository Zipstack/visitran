import { memo, useMemo, useCallback } from "react";
import { Form, Select, Typography, Button, Divider } from "antd";
import { PlusOutlined } from "@ant-design/icons";
import PropTypes from "prop-types";

const ConnectionEnvSection = memo(
  ({ connectionList, envDataList, setIsModalOpen, setIsEnvModalOpen, id }) => {
    const connectionOptions = useMemo(() => {
      return connectionList
        .filter((el) => !el.is_sample_project)
        .map((el) => ({
          value: el.id,
          label: (
            <div className="dFlex">
              <img src={el.db_icon} alt={el.name} height={20} width={20} />
              <span style={{ marginLeft: "5px" }}>{el.name}</span>
            </div>
          ),
          title: el.datasource_name,
        }));
    }, [connectionList]);

    const environmentOptions = useMemo(() => {
      return envDataList.map((el) => ({
        value: el.id,
        label: el.name,
      }));
    }, [envDataList]);

    const renderConnectionDropdown = useCallback(
      (menu) => (
        <>
          {menu}
          <Divider style={{ margin: "8px 0" }} />
          <Button
            type="text"
            icon={<PlusOutlined />}
            onClick={() => setIsModalOpen(true)}
            style={{ width: "100%", textAlign: "left" }}
          >
            Add New Connection
          </Button>
        </>
      ),
      [setIsModalOpen]
    );

    const renderEnvironmentDropdown = useCallback(
      (menu) => (
        <>
          {menu}
          <Divider style={{ margin: "8px 0" }} />
          <Button
            type="text"
            icon={<PlusOutlined />}
            onClick={() => setIsEnvModalOpen(true)}
            style={{ width: "100%", textAlign: "left" }}
          >
            Add New Environment
          </Button>
        </>
      ),
      [setIsEnvModalOpen]
    );

    return (
      <div className="connectionEnvSection">
        <Typography.Title level={5} className="sectionTitle">
          Configure Connection / Environment
        </Typography.Title>

        <Form.Item
          name="connection"
          label="Connection"
          rules={[{ required: true }]}
        >
          <Select
            disabled={id}
            placeholder="Select connection"
            options={connectionOptions}
            dropdownRender={renderConnectionDropdown}
          />
        </Form.Item>

        <Form.Item name="environment" label="Environment">
          <Select
            placeholder="Select environment"
            options={environmentOptions}
            dropdownRender={renderEnvironmentDropdown}
          />
        </Form.Item>
      </div>
    );
  }
);

ConnectionEnvSection.propTypes = {
  connectionList: PropTypes.array.isRequired,
  envDataList: PropTypes.array.isRequired,
  setIsModalOpen: PropTypes.func.isRequired,
  setIsEnvModalOpen: PropTypes.func.isRequired,
  id: PropTypes.string,
};

ConnectionEnvSection.displayName = "ConnectionEnvSection";

export { ConnectionEnvSection };
