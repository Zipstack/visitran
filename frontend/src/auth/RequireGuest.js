import { useMemo } from "react";
import { Navigate, Outlet, useLocation } from "react-router-dom";

import { useSessionStore } from "../store/session-store";

const publicRoutes = ["/login", "/signup", "/forgot-password"];

const isGuestPath = (pathname) =>
  publicRoutes.includes(pathname) || pathname.startsWith("/reset-password/");

const RequireGuest = () => {
  const { sessionDetails } = useSessionStore();
  const location = useLocation();

  const isLoggedIn = useMemo(
    () => !!sessionDetails?.user?.id,
    [sessionDetails?.user?.id]
  );

  const isPublicRoute = useMemo(
    () => isGuestPath(location.pathname),
    [location.pathname]
  );

  if (isLoggedIn) {
    return <Navigate to="/project/list" />;
  }

  if (!isPublicRoute) {
    return <Navigate to="/login" />;
  }

  return <Outlet />;
};

export { RequireGuest, publicRoutes, isGuestPath };
