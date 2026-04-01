import { useState, useEffect, useCallback } from "react";
import PropTypes from "prop-types";
import { Modal, Button, Typography, Tag, Alert, Space } from "antd";
import { CheckCircleOutlined, CloseCircleOutlined, WarningOutlined } from "@ant-design/icons";
import Cookies from "js-cookie";

import { useAxiosPrivate } from "../../service/axios-service";
import { useNotificationService } from "../../service/notification-service";
import { useProjectStore } from "../../store/project-store";
import { orgStore } from "../../store/org-store";
import { SpinnerLoader } from "../../widgets/spinner_loader";
import { getConflicts, resolveSingleConflict, finalizeConflictResolutions, previewResolution } from "./services";

const MODAL_WIDTH = 1000;

function ConflictResolutionModal({ open, onClose, modelName, onFinalize }) {
  const [conflicts, setConflicts] = useState([]);
  const [previewData, setPreviewData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [resolving, setResolving] = useState(null);
  const [finalizing, setFinalizing] = useState(false);

  const axiosRef = useAxiosPrivate();
  const { notify } = useNotificationService();
  const projectId = useProjectStore((state) => state.projectId);
  const orgId = orgStore.getState().selectedOrgId;
  const csrfToken = Cookies.get("csrftoken");

  const loadConflicts = useCallback(async () => {
    if (!open || !modelName) return;
    setLoading(true);
    try {
      const data = await getConflicts(axiosRef, orgId, projectId, modelName);
      setConflicts(data || []);
    } catch (error) { notify({ error }); }
    finally { setLoading(false); }
  }, [open, modelName, axiosRef, orgId, projectId, notify]);

  useEffect(() => { if (open) { loadConflicts(); setPreviewData(null); } }, [open]); // eslint-disable-line react-hooks/exhaustive-deps

  const handleResolve = async (conflictId, strategy) => {
    setResolving(conflictId);
    try {
      await resolveSingleConflict(axiosRef, orgId, projectId, modelName, csrfToken, conflictId, strategy);
      setConflicts((prev) => prev.map((c) => c.conflict_id === conflictId ? { ...c, status: "resolved", resolution_strategy: strategy } : c));
      const preview = await previewResolution(axiosRef, orgId, projectId, modelName);
      setPreviewData(preview);
    } catch (error) { notify({ error }); }
    finally { setResolving(null); }
  };

  const allResolved = conflicts.length > 0 && conflicts.every((c) => c.status === "resolved");

  const handleFinalize = async () => {
    setFinalizing(true);
    try {
      await finalizeConflictResolutions(axiosRef, orgId, projectId, modelName, csrfToken, "Resolved conflicts and merged changes");
      notify({ type: "success", message: "Conflicts resolved and merged" });
      onFinalize?.();
      onClose();
    } catch (error) { notify({ error }); }
    finally { setFinalizing(false); }
  };

  const getStatusTag = (conflict) => {
    if (conflict.status === "resolved") return <Tag color="green" icon={<CheckCircleOutlined />}>{conflict.resolution_strategy}</Tag>;
    return <Tag color="orange" icon={<WarningOutlined />}>Pending</Tag>;
  };

  return (
    <Modal title="Resolve Conflicts" open={open} onCancel={onClose} width={MODAL_WIDTH} centered maskClosable={false} destroyOnClose footer={[<Button key="cancel" onClick={onClose}>Cancel</Button>, <Button key="finalize" type="primary" onClick={handleFinalize} disabled={!allResolved} loading={finalizing}>Finalize</Button>]}>
      {loading ? <SpinnerLoader /> : conflicts.length === 0 ? (
        <Alert type="success" message="No conflicts detected" description="You can proceed with your commit." />
      ) : (
        <>
          <Alert type="warning" message={`${conflicts.length} conflict(s) detected`} description="Resolve each conflict before finalizing." style={{ marginBottom: 12 }} />
          {previewData && <Alert type="info" message="Merge Preview" description={<pre style={{ fontSize: 11, maxHeight: 120, overflow: "auto", margin: "4px 0 0", whiteSpace: "pre-wrap" }}>{typeof previewData === "string" ? previewData : JSON.stringify(previewData, null, 2)}</pre>} style={{ marginBottom: 12 }} />}
          <div className="conflict-panels">
            <div className="conflict-panel">
              <div className="conflict-panel-title">Current (Published)</div>
              {conflicts.map((conflict) => (
                <div key={conflict.conflict_id} className="conflict-item">
                  <Typography.Text strong>{conflict.transformation_path || conflict.path}</Typography.Text>
                  <pre style={{ fontSize: 11, margin: "4px 0", maxHeight: 100, overflow: "auto" }}>{typeof conflict.published_transformation === "string" ? conflict.published_transformation : JSON.stringify(conflict.published_transformation, null, 2)}</pre>
                </div>
              ))}
            </div>
            <div className="conflict-panel">
              <div className="conflict-panel-title">Resolution</div>
              {conflicts.map((conflict) => (
                <div key={conflict.conflict_id} className="conflict-item">
                  <div style={{ marginBottom: 4 }}>{getStatusTag(conflict)}</div>
                  {conflict.status !== "resolved" && (
                    <Space className="conflict-actions">
                      <Button size="small" onClick={() => handleResolve(conflict.conflict_id, "accepted")} loading={resolving === conflict.conflict_id} icon={<CheckCircleOutlined />}>Accept Current</Button>
                      <Button size="small" onClick={() => handleResolve(conflict.conflict_id, "rejected")} loading={resolving === conflict.conflict_id} icon={<CloseCircleOutlined />}>Accept Incoming</Button>
                    </Space>
                  )}
                </div>
              ))}
            </div>
            <div className="conflict-panel">
              <div className="conflict-panel-title">Incoming (Draft)</div>
              {conflicts.map((conflict) => (
                <div key={conflict.conflict_id} className="conflict-item">
                  <Typography.Text strong>{conflict.transformation_path || conflict.path}</Typography.Text>
                  <pre style={{ fontSize: 11, margin: "4px 0", maxHeight: 100, overflow: "auto" }}>{typeof conflict.draft_transformation === "string" ? conflict.draft_transformation : JSON.stringify(conflict.draft_transformation, null, 2)}</pre>
                </div>
              ))}
            </div>
          </div>
        </>
      )}
    </Modal>
  );
}

ConflictResolutionModal.propTypes = { open: PropTypes.bool.isRequired, onClose: PropTypes.func.isRequired, modelName: PropTypes.string, onFinalize: PropTypes.func };

export { ConflictResolutionModal };
