import { useState, useEffect } from "react";
import PropTypes from "prop-types";
import { Modal, Select, Space, Typography } from "antd";

import { useAxiosPrivate } from "../../service/axios-service";
import { useNotificationService } from "../../service/notification-service";
import { useProjectStore } from "../../store/project-store";
import { orgStore } from "../../store/org-store";
import { SpinnerLoader } from "../../widgets/spinner_loader";
import { DiffViewer } from "./DiffViewer";
import { fetchPendingChanges, fetchVersionDetail, fetchVersionHistory } from "./services";

const MODAL_WIDTH = "90vw";
const CURRENT_STATE_VALUE = "current";

function CompareModal({ open, onClose, initialVersionA, initialVersionB, mode }) {
  const isDraftMode = mode === "draft";
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
        const data = await fetchVersionHistory(axiosRef, orgId, projectId, 1, 100);
        const items = (data.page_items || []).map((v) => ({ label: `v${v.version_number} — ${v.commit_message || "No message"}`, value: v.version_number }));
        items.unshift({ label: "Current (working)", value: CURRENT_STATE_VALUE });
        setVersionOptions(items);
      } catch (error) { notify({ error }); }
    };
    loadOptions();
  }, [open]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => { setVersionA(initialVersionA); setVersionB(initialVersionB); }, [initialVersionA, initialVersionB]);

  useEffect(() => {
    if (!open) return;
    if (isDraftMode) {
      const loadDraftComparison = async () => {
        setLoading(true);
        try {
          const data = await fetchPendingChanges(axiosRef, orgId, projectId);
          const changes = data?.changes || [];
          const oldParts = [];
          const newParts = [];
          for (const c of changes) {
            oldParts.push(`# --- ${c.model_name} (committed) ---\n${c.old_yaml || ""}`);
            newParts.push(`# --- ${c.model_name} (draft) ---\n${c.new_yaml || ""}`);
          }
          setContentA(oldParts.join("\n") || "# No committed version");
          setContentB(newParts.join("\n") || "# No draft changes");
        } catch (error) { notify({ error }); }
        finally { setLoading(false); }
      };
      loadDraftComparison();
      return;
    }
    if (!versionA || !versionB) return;
    const loadContents = async () => {
      setLoading(true);
      try {
        const [dataA, dataB] = await Promise.all([loadVersionContent(versionA), loadVersionContent(versionB)]);
        setContentA(dataA);
        setContentB(dataB);
      } catch (error) { notify({ error }); }
      finally { setLoading(false); }
    };
    loadContents();
  }, [open, versionA, versionB, isDraftMode]); // eslint-disable-line react-hooks/exhaustive-deps

  const loadVersionContent = async (version) => {
    if (version === CURRENT_STATE_VALUE) {
      const data = await fetchVersionHistory(axiosRef, orgId, projectId, 1, 1);
      const latest = data.page_items?.[0];
      if (latest) {
        const detail = await fetchVersionDetail(axiosRef, orgId, projectId, latest.version_number);
        return detail.yaml_content || JSON.stringify(detail.model_data, null, 2);
      }
      return "";
    }
    const data = await fetchVersionDetail(axiosRef, orgId, projectId, version);
    return data.yaml_content || JSON.stringify(data.model_data, null, 2);
  };

  const getLabel = (version) => version === CURRENT_STATE_VALUE ? "Current" : version ? `v${version}` : "Base";
  const handleClose = () => { setContentA(""); setContentB(""); onClose(); };

  return (
    <Modal title={isDraftMode ? "Draft vs Committed" : "Compare Versions"} open={open} onCancel={handleClose} footer={null} width={MODAL_WIDTH} centered maskClosable={false} destroyOnClose>
      {!isDraftMode && (
        <Space style={{ marginBottom: 12 }}>
          <Typography.Text>From:</Typography.Text>
          <Select value={versionA} onChange={setVersionA} options={versionOptions} style={{ width: 300 }} placeholder="Select base version" />
          <Typography.Text>To:</Typography.Text>
          <Select value={versionB} onChange={setVersionB} options={versionOptions} style={{ width: 300 }} placeholder="Select target version" />
        </Space>
      )}
      <div style={{ height: 500 }}>
        {loading ? <SpinnerLoader /> : <DiffViewer originalContent={contentA} modifiedContent={contentB} originalTitle={isDraftMode ? "Last Committed" : getLabel(versionA)} modifiedTitle={isDraftMode ? "Current Draft" : getLabel(versionB)} />}
      </div>
    </Modal>
  );
}

CompareModal.propTypes = { open: PropTypes.bool.isRequired, onClose: PropTypes.func.isRequired, initialVersionA: PropTypes.oneOfType([PropTypes.number, PropTypes.string]), initialVersionB: PropTypes.oneOfType([PropTypes.number, PropTypes.string]), mode: PropTypes.string };

export { CompareModal };
