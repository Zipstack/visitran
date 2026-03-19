import { useState, useRef } from "react";
import PropTypes from "prop-types";
import { Modal, Typography, Input, Radio, Select } from "antd";

import "./top-menu.css";

const BODY_CSS = {
  display: "grid",
  rowGap: "16px",
  paddingBlock: "10px",
};

const STORAGE_OPTIONS = [
  {
    label: "Local",
    value: "local",
  },
  {
    label: "Github",
    value: "github",
  },
  {
    label: "Gitlab",
    value: "gitlab",
  },
  {
    label: "Visitran Cloud",
    value: "visitran_cloud",
  },
];

const WAREHOUSE_OPTIONS = [
  {
    label: "Snowflake",
    value: "snowflake",
  },
  {
    label: "Snowflake",
    value: "snowflake2",
  },
  {
    label: "Snowflake",
    value: "snowflake3",
  },
];

function NewProject({ onClose = () => {} }) {
  const { Text } = Typography;
  const [openModal, setOpenModal] = useState(true);
  const [allowCreation, setAllowCreation] = useState(true);
  const [storage, setStorage] = useState(STORAGE_OPTIONS[0].value);
  const [warehouse, setWarehouse] = useState(WAREHOUSE_OPTIONS[0].value);
  const nameRef = useRef("");

  function updateName({ target: { value } }) {
    nameRef.current = value;
    if (value === "" || value.trim() === "") {
      setAllowCreation(true);
      return;
    }
    setAllowCreation(false);
  }

  function onStorageChange({ target: { value } }) {
    setStorage(value);
  }

  function onWarehouseChange(value) {
    setWarehouse(value);
  }

  function onCancel() {
    setOpenModal(false);
    onClose();
  }

  function onCreate() {
    onCancel();
  }

  return (
    <Modal
      title={"New Project"}
      open={openModal}
      onCancel={onCancel}
      onOk={onCreate}
      centered
      maskClosable={false}
      okText={"Create Project"}
      width="400px"
      bodyStyle={BODY_CSS}
      okButtonProps={{ disabled: allowCreation }}
    >
      <div className="modalField">
        <Text>Project Name *</Text>
        <Input onChange={updateName} />
      </div>
      <div className="modalField">
        <Text>Storage</Text>
        <div>
          <Radio.Group
            value={storage}
            onChange={onStorageChange}
            options={STORAGE_OPTIONS}
            optionType="button"
            buttonStyle="solid"
          />
        </div>
      </div>
      <div className="modalField">
        <Text>Configured Repository Details</Text>
        <Input.TextArea autoSize={{ minRows: 3, maxRows: 6 }} />
      </div>
      <div className="modalField">
        <Text>Warehouse</Text>
        <Select
          value={warehouse}
          onChange={onWarehouseChange}
          options={WAREHOUSE_OPTIONS}
        />
      </div>
    </Modal>
  );
}

NewProject.propTypes = {
  onClose: PropTypes.func,
};

export { NewProject };
