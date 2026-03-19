import { useCallback, useMemo } from "react";
import { Modal, Button, Typography } from "antd";
import { Navigate, Outlet, useNavigate, useLocation } from "react-router-dom";

import { useSessionStore } from "../store/session-store";

const RequireAuth = () => {
  const { showSessionExpiredModal, sessionDetails } = useSessionStore();
  const navigate = useNavigate();
  const location = useLocation();

  const isLoggedIn = !!sessionDetails?.user?.id;

  const handleLoginRedirect = useCallback(() => {
    navigate("/login");
  }, [navigate]);

  const modalFooter = useMemo(
    () => [
      <Button key="ok" onClick={handleLoginRedirect} type="primary">
        Login
      </Button>,
    ],
    [handleLoginRedirect]
  );

  if (!isLoggedIn) {
    return <Navigate to="/login" />;
  }

  if (location.pathname === "/") {
    return <Navigate to="/project/list" />;
  }

  return (
    <>
      <Outlet />
      <Modal
        title="Session Expired"
        open={showSessionExpiredModal}
        centered
        footer={modalFooter}
        maskClosable={false}
      >
        <Typography>
          Your session has expired. Please log in again to continue your
          transformation with Visitran Ai.
        </Typography>
      </Modal>
    </>
  );
};

export { RequireAuth };
