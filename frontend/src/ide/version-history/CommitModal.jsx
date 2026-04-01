import { useEffect, useState } from "react";
import PropTypes from "prop-types";
import { Collapse, Input, Modal, Spin, Tag, Typography } from "antd";
import Cookies from "js-cookie";

import { useAxiosPrivate } from "../../service/axios-service";
import { useNotificationService } from "../../service/notification-service";
import { useProjectStore } from "../../store/project-store";
import { orgStore } from "../../store/org-store";
import { useVersionHistoryStore } from "../../store/version-history-store";
import { commitProjectVersion, fetchPendingChanges } from "./services";
import { DiffViewer } from "./DiffViewer";

const MODAL_WIDTH = 900;
const MAX_MESSAGE_LENGTH = 500;
const CHANGE_TYPE_COLORS = { added: "green", modified: "blue", removed: "red" };

function CommitModal({ onCommitSuccess }) {
  const [commitMessage, setCommitMessage] = useState("");
  const [loading, setLoading] = useState(false);
  const [previewData, setPreviewData] = useState(null);
  const [previewLoading, setPreviewLoading] = useState(true);

  const axiosRef = useAxiosPrivate();
  const { notify } = useNotificationService();
  const projectId = useProjectStore((state) => state.projectId);
  const orgId = orgStore.getState().selectedOrgId;
  const csrfToken = Cookies.get("csrftoken");
  const isOpen = useVersionHistoryStore((state) => state.isCommitModalOpen);
  const closeCommitModal = useVersionHistoryStore((state) => state.closeCommitModal);

  useEffect(() => {
    if (!isOpen) return;
    let cancelled = false;
    setPreviewLoading(true);
    setPreviewData(null);
    fetchPendingChanges(axiosRef, orgId, projectId)
      .then((data) => { if (!cancelled) setPreviewData(data); })
      .catch(() => { if (!cancelled) setPreviewData(null); })
      .finally(() => { if (!cancelled) setPreviewLoading(false); });
    return () => { cancelled = true; };
  }, [isOpen]); // eslint-disable-line

  const handleOk = async () => {
    if (!commitMessage.trim()) return;
    setLoading(true);
    try {
      await commitProjectVersion(axiosRef, orgId, projectId, csrfToken, commitMessage.trim());
      notify({ type: "success", message: "Version committed successfully" });
      setCommitMessage("");
      closeCommitModal();
      onCommitSuccess?.();
    } catch (error) {
      notify({ error });
    } finally {
      setLoading(false);
    }
  };

  const handleCancel = () => { setCommitMessage(""); setPreviewData(null); closeCommitModal(); };

  const collapseItems = previewData?.changes?.map((change) => ({
    key: change.model_name,
    label: (<span>{change.model_name} <Tag color={CHANGE_TYPE_COLORS[change.change_type]}>{change.change_type}</Tag></span>),
    children: (
      <div className="commit-preview-diff">
        <DiffViewer originalTitle="Last Committed" modifiedTitle="Current" originalContent={change.old_yaml} modifiedContent={change.new_yaml} forceInline />
      </div>
    ),
  })) || [];

  return (
    <Modal title="Commit Version" open={isOpen} onOk={handleOk} onCancel={handleCancel} okText="Commit" okButtonProps={{ disabled: !commitMessage.trim() || loading, loading }} width={MODAL_WIDTH} centered maskClosable={false} destroyOnClose>
      <Typography.Paragraph type="secondary" style={{ marginBottom: 12 }}>Create a new version snapshot for <strong>all models</strong> in this project.</Typography.Paragraph>
      <Input.TextArea value={commitMessage} onChange={(e) => setCommitMessage(e.target.value)} placeholder="Describe what changed..." maxLength={MAX_MESSAGE_LENGTH} showCount rows={4} autoFocus />
      <div className="commit-preview-section">
        {previewLoading && <div className="commit-preview-empty"><Spin size="small" /></div>}
        {!previewLoading && previewData && !previewData.has_changes && <div className="commit-preview-empty"><Typography.Text type="secondary">No changes since last commit</Typography.Text></div>}
        {!previewLoading && previewData?.has_changes && (
          <>
            <div className="commit-preview-summary"><Typography.Text type="secondary">{previewData.total_models_changed} model{previewData.total_models_changed !== 1 ? "s" : ""} changed</Typography.Text></div>
            <Collapse items={collapseItems} size="small" />
          </>
        )}
      </div>
    </Modal>
  );
}

CommitModal.propTypes = { onCommitSuccess: PropTypes.func };

export { CommitModal };
