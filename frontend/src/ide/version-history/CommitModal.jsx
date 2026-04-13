import { useState } from "react";
import PropTypes from "prop-types";
import { Input, message, Modal, Typography } from "antd";
import Cookies from "js-cookie";

import { useAxiosPrivate } from "../../service/axios-service";
import { useProjectStore } from "../../store/project-store";
import { orgStore } from "../../store/org-store";
import { useVersionHistoryStore } from "../../store/version-history-store";
import { commitProjectVersion } from "./services";

const MODAL_WIDTH = 520;

function CommitModal({ onCommitSuccess }) {
  const [title, setTitle] = useState("");
  const [description, setDescription] = useState("");
  const [submitting, setSubmitting] = useState(false);

  const axiosRef = useAxiosPrivate();
  const projectId = useProjectStore((state) => state.projectId);
  const orgId = orgStore.getState().selectedOrgId;
  const csrfToken = Cookies.get("csrftoken");
  const isOpen = useVersionHistoryStore((state) => state.isCommitModalOpen);
  const closeCommitModal = useVersionHistoryStore(
    (state) => state.closeCommitModal
  );

  const handleOk = async () => {
    if (!title.trim()) return;
    setSubmitting(true);
    try {
      await commitProjectVersion(
        axiosRef,
        orgId,
        projectId,
        csrfToken,
        title.trim(),
        description.trim()
      );
      message.success("Commit queued — version history will update shortly");
      setTitle("");
      setDescription("");
      closeCommitModal();
      onCommitSuccess?.();
    } catch (error) {
      message.error(error?.response?.data?.error_message || "Commit failed");
    } finally {
      setSubmitting(false);
    }
  };

  const handleCancel = () => {
    setTitle("");
    setDescription("");
    closeCommitModal();
  };

  return (
    <Modal
      title="Commit to Git"
      open={isOpen}
      onOk={handleOk}
      onCancel={handleCancel}
      okText="Commit"
      okButtonProps={{
        disabled: !title.trim() || submitting,
        loading: submitting,
      }}
      width={MODAL_WIDTH}
      centered
      maskClosable={false}
      destroyOnClose
    >
      <div style={{ marginBottom: 12 }}>
        <Input
          placeholder="Brief description of changes (e.g. add revenue model)"
          value={title}
          onChange={(e) => setTitle(e.target.value)}
          maxLength={72}
          showCount
          autoFocus
        />
        <Typography.Text type="secondary" style={{ fontSize: 11 }}>
          This becomes the git commit title
        </Typography.Text>
      </div>
      <div>
        <Input.TextArea
          placeholder="Additional context (optional)"
          value={description}
          onChange={(e) => setDescription(e.target.value)}
          rows={3}
          maxLength={500}
          showCount
        />
        <Typography.Text type="secondary" style={{ fontSize: 11 }}>
          Appears in the git commit body
        </Typography.Text>
      </div>
    </Modal>
  );
}

CommitModal.propTypes = { onCommitSuccess: PropTypes.func };

export { CommitModal };
