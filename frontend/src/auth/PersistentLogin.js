import { useState, useEffect, useCallback } from "react";
import { Outlet, useNavigate, useLocation } from "react-router-dom";

import { useUserSession } from "../widgets/hooks/useUserSession";
import { orgStore } from "../store/org-store";
import { SpinnerLoader } from "../widgets/spinner_loader";
import { isGuestPath } from "./RequireGuest";

const PersistentLogin = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const [isLoading, setIsLoading] = useState(true);
  const { setOrgId } = orgStore();
  const userSession = useUserSession();

  const isGuestRoute = isGuestPath(location.pathname);

  const fetchSession = useCallback(async () => {
    try {
      const res = await userSession();

      if (res?.data) {
        setOrgId(res.data.organization_id || null);
      } else {
        setOrgId(null);
        if (!isGuestRoute) {
          navigate("/login", { replace: true });
        }
      }
    } catch (error) {
      if (!isGuestRoute) {
        navigate("/login", { replace: true });
      }
    } finally {
      setIsLoading(false);
    }
  }, [isGuestRoute]);

  useEffect(() => {
    fetchSession();
  }, []);

  if (isLoading) {
    return <SpinnerLoader />;
  }

  return <Outlet />;
};

export { PersistentLogin };
