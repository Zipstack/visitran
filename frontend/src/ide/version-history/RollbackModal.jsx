import { useState, useEffect, useCallback } from "react";
import PropTypes from "prop-types";
import { Modal, Typography, Tag, Checkbox, Input, Alert, Steps } from "antd";
import Cookies from "js-cookie";

import { useAxiosPrivate } from "../../service/axios-service";
import { useNotificationService } from "../../service/notification-service";
import { useProjectStore } from "../../store/project-store";
import { orgStore } from "../../store/org-store";
import { SpinnerLoader } from "../../widgets/spinner_loader";
import { executeRollback } from "./services";

const MODAL_WIDTH = 800;

function RollbackModal({ open, onClose, targetVersion, onRollbackSuccess }) {
  const [loading, setLoading] = useState(false);
  const [executing, setExecuting] = useState(false);
  const [confirmed, setConfirmed] = useState(false);
  const [reason, setReason] = useState("");

  const axiosRef = useAxiosPrivate();
  const { notify } = useNotificationService();
  const projectId = useProjectStore((state) => state.projectId);
  const orgId = orgStore.getState().selectedOrgId;
  const csrfToken = Cookies.get("csrftoken");

  useEffect(() => {
    if (open) { setConfirmed(false); setReason(""); }
  }, [open]);

  const handleConfirm = async () => {
    setExecuting(true);
    try {
      await executeRollback(axiosRef, orgId, projectId, csrfToken, targetVersion, reason || `Rollback to v${targetVersion}`);
      notify({ type: "success", message: "Rollback completed successfully" });
      onRollbackSuccess?.();
    } catch (error) { notify({ error }); }
    finally { setExecuting(false); }
  };

  const canConfirm = confirmed && !loading && !executing;

  return (
    <Modal title={`Rollback to v${targetVersion}`} open={open} onCancel={onClose} onOk={handleConfirm} okText="Confirm Rollback" okButtonProps={{ disabled: !canConfirm, loading: executing, danger: true }} width={MODAL_WIDTH} centered maskClosable={false} destroyOnClose>
      <Alert type="warning" showIcon message={`This will rollback the project to version ${targetVersion}`} description="A new version will be created with the rolled-back content. The current state will be preserved in version history." style={{ marginBottom: 16 }} />
      <Input value={reason} onChange={(e) => setReason(e.target.value)} placeholder="Reason for rollback (optional)" style={{ marginBottom: 12 }} />
      <Checkbox checked={confirmed} onChange={(e) => setConfirmed(e.target.checked)} className="rollback-confirm-checkbox">
        I understand the impact of this rollback
      </Checkbox>
    </Modal>
  );
}

RollbackModal.propTypes = { open: PropTypes.bool.isRequired, onClose: PropTypes.func.isRequired, targetVersion: PropTypes.number, onRollbackSuccess: PropTypes.func };

export { RollbackModal };
