import { useState, useCallback } from "react";
import PropTypes from "prop-types";
import { Modal, Typography, Alert, Steps, Button, Collapse, Result } from "antd";
import { PlayCircleOutlined, CheckCircleOutlined, CloseCircleOutlined } from "@ant-design/icons";
import Cookies from "js-cookie";

import { SpinnerLoader } from "../../widgets/spinner_loader";
import { useAxiosPrivate } from "../../service/axios-service";
import { orgStore } from "../../store/org-store";
import { useProjectStore } from "../../store/project-store";
import { executeVersion } from "./services";

const MODAL_WIDTH = 600;

function ExecuteVersionModal({ open, onClose, targetVersion, onExecuteSuccess, onRollbackToVersion }) {
  const [executing, setExecuting] = useState(false);
  const [result, setResult] = useState(null);
  const axiosRef = useAxiosPrivate();
  const { projectId } = useProjectStore();
  const orgId = orgStore.getState().selectedOrgId;
  const csrfToken = Cookies.get("csrftoken");

  const currentStep = result ? 2 : executing ? 1 : 0;

  const handleExecute = useCallback(async () => {
    setExecuting(true);
    setResult(null);
    try {
      const res = await executeVersion(axiosRef, orgId, projectId, csrfToken, targetVersion);
      if (res?.status === "success") {
        setResult({ success: true, message: res.data?.message || "All transformations completed." });
        onExecuteSuccess?.();
      } else {
        setResult({ success: false, error_message: res?.error_message || "Execution failed." });
      }
    } catch (err) {
      const errMsg = err?.response?.data?.error_message || err?.message || "Execution failed.";
      setResult({ success: false, error_message: errMsg });
    } finally {
      setExecuting(false);
    }
  }, [axiosRef, orgId, projectId, csrfToken, targetVersion, onExecuteSuccess]);

  const handleClose = () => { setResult(null); setExecuting(false); onClose(); };
  const handleMakeCurrent = () => { handleClose(); onRollbackToVersion?.(targetVersion); };

  const renderConfirmation = () => (
    <>
      <Alert type="warning" showIcon message={`This will execute version ${targetVersion}'s transformations against your current data`} description="All models will be temporarily updated during execution and restored to their current state afterward." style={{ marginBottom: 16 }} />
      <Typography.Paragraph><strong>Version:</strong> v{targetVersion}</Typography.Paragraph>
    </>
  );

  const renderExecuting = () => (
    <div style={{ textAlign: "center", padding: "24px 0" }}>
      <SpinnerLoader />
      <Typography.Paragraph type="secondary" style={{ marginTop: 16 }}>Running transformations for v{targetVersion}...</Typography.Paragraph>
    </div>
  );

  const renderResult = () => {
    if (!result) return null;
    if (result.success) {
      return <Result status="success" icon={<CheckCircleOutlined />} title="Execution Successful" subTitle={`Version ${targetVersion} executed successfully and is now the current version.`} />;
    }
    return <Result status="error" icon={<CloseCircleOutlined />} title="Execution Failed" subTitle="One or more transformations failed." extra={<Collapse ghost items={[{ key: "error", label: "Error details", children: <Typography.Paragraph code style={{ whiteSpace: "pre-wrap", fontSize: 12, maxHeight: 200, overflow: "auto" }}>{result.error_message || "Unknown error"}</Typography.Paragraph> }]} />} />;
  };

  const getFooter = () => {
    if (executing) return null;
    if (result) return [<Button key="close" onClick={handleClose}>Close</Button>];
    return [<Button key="cancel" onClick={handleClose}>Cancel</Button>, <Button key="execute" type="primary" icon={<PlayCircleOutlined />} onClick={handleExecute}>Execute</Button>];
  };

  return (
    <Modal title={`Execute Version ${targetVersion}`} open={open} onCancel={handleClose} footer={getFooter()} width={MODAL_WIDTH} centered maskClosable={false} destroyOnClose>
      <Steps size="small" current={currentStep} style={{ marginBottom: 16 }} items={[{ title: "Confirm" }, { title: "Execute" }, { title: "Result" }]} />
      {currentStep === 0 && renderConfirmation()}
      {currentStep === 1 && renderExecuting()}
      {currentStep === 2 && renderResult()}
    </Modal>
  );
}

ExecuteVersionModal.propTypes = { open: PropTypes.bool.isRequired, onClose: PropTypes.func.isRequired, targetVersion: PropTypes.number, onExecuteSuccess: PropTypes.func, onRollbackToVersion: PropTypes.func };

export { ExecuteVersionModal };
