import { useState, useEffect } from "react";
import PropTypes from "prop-types";
import { Modal, Descriptions, Tag, Typography } from "antd";
import Editor from "@monaco-editor/react";

import { useAxiosPrivate } from "../../service/axios-service";
import { useNotificationService } from "../../service/notification-service";
import { useProjectStore } from "../../store/project-store";
import { useUserStore } from "../../store/user-store";
import { orgStore } from "../../store/org-store";
import { THEME } from "../../common/constants";
import { SpinnerLoader } from "../../widgets/spinner_loader";
import { fetchVersionDetail } from "./services";

const MODAL_WIDTH = 800;

function ViewVersionModal({ open, onClose, versionNumber }) {
  const [versionData, setVersionData] = useState(null);
  const [loading, setLoading] = useState(false);

  const axiosRef = useAxiosPrivate();
  const { notify } = useNotificationService();
  const projectId = useProjectStore((state) => state.projectId);
  const orgId = orgStore.getState().selectedOrgId;
  const { currentTheme } = useUserStore((state) => state.userDetails);

  useEffect(() => {
    if (!open || !versionNumber) return;
    const load = async () => {
      setLoading(true);
      try {
        const data = await fetchVersionDetail(
          axiosRef,
          orgId,
          projectId,
          versionNumber
        );
        setVersionData(data);
      } catch (error) {
        notify({ error });
      } finally {
        setLoading(false);
      }
    };
    load();
  }, [open, versionNumber]); // eslint-disable-line

  const handleClose = () => {
    setVersionData(null);
    onClose();
  };
  const formatDate = (dateStr) =>
    dateStr ? new Date(dateStr).toLocaleString() : "";

  return (
    <Modal
      title={`Version ${versionNumber}`}
      open={open}
      onCancel={handleClose}
      footer={null}
      width={MODAL_WIDTH}
      centered
      maskClosable={false}
      destroyOnClose
    >
      {loading ? (
        <SpinnerLoader />
      ) : versionData ? (
        <>
          <Descriptions size="small" column={2} style={{ marginBottom: 12 }}>
            <Descriptions.Item label="Message">
              {versionData.commit_message || (
                <Typography.Text type="secondary">No message</Typography.Text>
              )}
            </Descriptions.Item>
            <Descriptions.Item label="By">
              {versionData.committed_by?.name || "system"}
            </Descriptions.Item>
            <Descriptions.Item label="Created">
              {formatDate(versionData.created_at)}
            </Descriptions.Item>
            <Descriptions.Item label="Git sync">
              {versionData.git_sync_status === "synced" ? (
                <Tag color="success">synced</Tag>
              ) : versionData.git_sync_status === "failed" ? (
                <Tag color="error">failed</Tag>
              ) : (
                <Tag>{versionData.git_sync_status || "n/a"}</Tag>
              )}
            </Descriptions.Item>
          </Descriptions>
          <div
            style={{
              height: 450,
              border: "1px solid var(--border-color-1, #303030)",
              borderRadius: 6,
              overflow: "hidden",
            }}
          >
            <Editor
              value={versionData?.yaml_content || ""}
              language="yaml"
              height="100%"
              theme={currentTheme === THEME.DARK ? "vs-dark" : "light"}
              options={{
                readOnly: true,
                scrollBeyondLastLine: false,
                minimap: { enabled: false },
                lineNumbers: "on",
                wordWrap: "on",
              }}
              loading={<SpinnerLoader />}
            />
          </div>
        </>
      ) : null}
    </Modal>
  );
}

ViewVersionModal.propTypes = {
  open: PropTypes.bool.isRequired,
  onClose: PropTypes.func.isRequired,
  versionNumber: PropTypes.number,
};

export { ViewVersionModal };
