import { useCallback, useMemo } from "react";
import { Modal, Button, Typography } from "antd";
import { Navigate, Outlet, useNavigate, useLocation } from "react-router-dom";

import { useSessionStore } from "../store/session-store";

const RequireAuth = () => {
  const {
    showSessionExpiredModal,
    setShowSessionExpiredModal,
    sessionDetails,
  } = useSessionStore();
  const navigate = useNavigate();
  const location = useLocation();

  const isLoggedIn = !!sessionDetails?.user?.id;

  const handleLoginRedirect = useCallback(() => {
    setShowSessionExpiredModal(false);
    navigate("/login");
  }, [navigate, setShowSessionExpiredModal]);

  const modalFooter = useMemo(
    () => [
      <Button key="ok" onClick={handleLoginRedirect} type="primary">
        Login
      </Button>,
    ],
    [handleLoginRedirect]
  );

  // Show session expired modal instead of silently redirecting
  if (!isLoggedIn && showSessionExpiredModal) {
    return (
      <Modal
        title="Session Expired"
        open
        centered
        footer={modalFooter}
        closable={false}
        maskClosable={false}
      >
        <Typography>
          Your session has expired. Please log in again to continue.
        </Typography>
      </Modal>
    );
  }

  if (!isLoggedIn) {
    return <Navigate to="/login" />;
  }

  if (location.pathname === "/") {
    return <Navigate to="/project/list" />;
  }

  return <Outlet />;
};

export { RequireAuth };
