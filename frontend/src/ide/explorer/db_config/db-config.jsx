import { useState, useRef, useEffect } from "react";
import PropTypes from "prop-types";
import { Modal, Typography, Button, Descriptions } from "antd";

import { SpinnerLoader } from "../../../widgets/spinner_loader";
import { dbConfigService } from "./db-config-service.js";
import { useProjectStore } from "../../../store/project-store.js";
import { EditIcon } from "../../../base/icons";

import "./db-config.css";
import { useNavigate } from "react-router-dom";

// const TWO_COLUMN_FIELD = ["host", "port"];

const BODY_CSS = {
  paddingBlock: "8px",
  maxHeight: "500px",
  overflowY: "auto",
};

const FOOTER_BUTTON_STYLE = {
  marginInlineStart: "8px",
};

export function DatabaseConfig({
  // setOpenDbConfig = () => {},
  onClose = () => {},
}) {
  const { Text } = Typography;
  const dbConfService = dbConfigService();
  const navigate = useNavigate();
  const { setDbConfigDetails, projectId, dbConfigDetails } = useProjectStore();

  const [openModal, setOpenModal] = useState(true);
  const [dsFields, setDsFields] = useState([]);
  // const [delModalOpen, setDelModalOpen] = useState(false);

  const dsRef = useRef("");

  useEffect(() => {
    Promise.allSettled([
      dbConfService.getDbConfig(projectId),
      dbConfService.getDsList(),
    ])
      .then(([res1, res2]) => {
        // for first time, datasource_name is empty
        setDbConfigDetails(res1.value.data); // updating store
        dsRef.current = res1.value.data.datasource_name;
        const fields = res1.value.data.connection_details;
        setDsFields(fields);
      })
      .catch((err) => {
        console.error(err);
      });
  }, []);

  function onCancel() {
    setOpenModal(false);
    onClose();
  }

  function handleOK() {
    navigate("/project/connection/list", {
      state: {
        cId: dbConfigDetails.connection_id,
      },
    });
  }

  function getFooter() {
    return (
      <div className="dbConfigFooter">
        <Button onClick={onCancel}>Cancel</Button>
        <Button
          style={FOOTER_BUTTON_STYLE}
          type="primary"
          onClick={handleOK}
          icon={<EditIcon />}
        >
          Edit
        </Button>
      </div>
    );
  }

  return (
    <Modal
      title={"Database Configuration"}
      open={openModal}
      onCancel={onCancel}
      maskClosable={false}
      width="800px"
      bodyStyle={BODY_CSS}
      centered
      footer={Object.keys(dsFields).length > 0 && getFooter()}
    >
      <>
        {Object.keys(dsFields)?.length > 0 && (
          <Descriptions bordered column={1}>
            <Descriptions.Item
              styles={{ label: { width: "25%" }, content: { width: "75%" } }}
              label={<Text strong>Data Store</Text>}
            >
              {dsRef.current}
            </Descriptions.Item>
            {Object.keys(dsFields).map((field) => {
              const label = field === "passw" ? "password" : field;
              return (
                <Descriptions.Item
                  styles={{
                    label: { width: "25%" },
                    content: { width: "75%" },
                  }}
                  key={label}
                  label={
                    <Text strong>
                      {label[0].toUpperCase() + label.slice(1, label.length)}
                    </Text>
                  }
                >
                  {label === "password"
                    ? Array(dsFields[field].length).fill("*").join("")
                    : dsFields[field]}
                </Descriptions.Item>
              );
            })}
          </Descriptions>
        )}

        {!Object.keys(dsFields).length && <SpinnerLoader />}
      </>
    </Modal>
  );
}
DatabaseConfig.propTypes = {
  onClose: PropTypes.func,
};
