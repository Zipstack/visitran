import { useState, useEffect } from "react";
import PropTypes from "prop-types";
import { Modal, Select, Space, Typography } from "antd";

import { useAxiosPrivate } from "../../service/axios-service";
import { useNotificationService } from "../../service/notification-service";
import { useProjectStore } from "../../store/project-store";
import { orgStore } from "../../store/org-store";
import { SpinnerLoader } from "../../widgets/spinner_loader";
import { DiffViewer } from "./DiffViewer";
import { fetchVersionDetail, fetchVersionHistory } from "./services";

const MODAL_WIDTH = "90vw";
const CURRENT_STATE_VALUE = "current";

function CompareModal({ open, onClose, initialVersionA, initialVersionB }) {
  const [versionA, setVersionA] = useState(initialVersionA);
  const [versionB, setVersionB] = useState(initialVersionB);
  const [contentA, setContentA] = useState("");
  const [contentB, setContentB] = useState("");
  const [versionOptions, setVersionOptions] = useState([]);
  const [loading, setLoading] = useState(false);

  const axiosRef = useAxiosPrivate();
  const { notify } = useNotificationService();
  const projectId = useProjectStore((state) => state.projectId);
  const orgId = orgStore.getState().selectedOrgId;

  useEffect(() => {
    if (!open) return;
    const loadOptions = async () => {
      try {
        const data = await fetchVersionHistory(
          axiosRef,
          orgId,
          projectId,
          1,
          100
        );
        const items = (data.page_items || []).map((v) => ({
          label: `v${v.version_number} — ${v.commit_message || "No message"}`,
          value: v.version_number,
        }));
        items.unshift({
          label: "Current (working)",
          value: CURRENT_STATE_VALUE,
        });
        setVersionOptions(items);
      } catch (error) {
        notify({ error });
      }
    };
    loadOptions();
  }, [open]); // eslint-disable-line

  useEffect(() => {
    setVersionA(initialVersionA);
    setVersionB(initialVersionB);
  }, [initialVersionA, initialVersionB]);

  useEffect(() => {
    if (!open || !versionA || !versionB) return;
    const loadContents = async () => {
      setLoading(true);
      try {
        const [dataA, dataB] = await Promise.all([
          loadVersionContent(versionA),
          loadVersionContent(versionB),
        ]);
        setContentA(dataA);
        setContentB(dataB);
      } catch (error) {
        notify({ error });
      } finally {
        setLoading(false);
      }
    };
    loadContents();
  }, [open, versionA, versionB]); // eslint-disable-line

  const loadVersionContent = async (version) => {
    if (version === CURRENT_STATE_VALUE) {
      const data = await fetchVersionHistory(axiosRef, orgId, projectId, 1, 1);
      const latest = data.page_items?.[0];
      if (latest) {
        const detail = await fetchVersionDetail(
          axiosRef,
          orgId,
          projectId,
          latest.version_number
        );
        return (
          detail.yaml_content || JSON.stringify(detail.model_data, null, 2)
        );
      }
      return "";
    }
    const data = await fetchVersionDetail(axiosRef, orgId, projectId, version);
    return data.yaml_content || JSON.stringify(data.model_data, null, 2);
  };

  const getLabel = (version) =>
    version === CURRENT_STATE_VALUE
      ? "Current"
      : version
      ? `v${version}`
      : "Base";
  const handleClose = () => {
    setContentA("");
    setContentB("");
    onClose();
  };

  return (
    <Modal
      title="Compare Versions"
      open={open}
      onCancel={handleClose}
      footer={null}
      width={MODAL_WIDTH}
      centered
      maskClosable={false}
      destroyOnClose
    >
      <Space style={{ marginBottom: 12 }}>
        <Typography.Text>From:</Typography.Text>
        <Select
          value={versionA}
          onChange={setVersionA}
          options={versionOptions}
          style={{ width: 300 }}
          placeholder="Select base version"
        />
        <Typography.Text>To:</Typography.Text>
        <Select
          value={versionB}
          onChange={setVersionB}
          options={versionOptions}
          style={{ width: 300 }}
          placeholder="Select target version"
        />
      </Space>
      <div style={{ height: 500 }}>
        {loading ? (
          <SpinnerLoader />
        ) : (
          <DiffViewer
            originalContent={contentA}
            modifiedContent={contentB}
            originalTitle={getLabel(versionA)}
            modifiedTitle={getLabel(versionB)}
          />
        )}
      </div>
    </Modal>
  );
}

CompareModal.propTypes = {
  open: PropTypes.bool.isRequired,
  onClose: PropTypes.func.isRequired,
  initialVersionA: PropTypes.oneOfType([PropTypes.number, PropTypes.string]),
  initialVersionB: PropTypes.oneOfType([PropTypes.number, PropTypes.string]),
};

export { CompareModal };
