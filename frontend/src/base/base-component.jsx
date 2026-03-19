import { useEffect, useMemo } from "react";
import { BrowserRouter } from "react-router-dom";
import { ConfigProvider, theme } from "antd";

import { RouteComponent } from "./route-component.jsx";
import { useUserStore } from "../store/user-store.js";
import { THEME } from "../common/constants.js";
import { NotificationProvider } from "../service/notification-service.js";

function BaseComponent() {
  const { defaultAlgorithm, darkAlgorithm } = theme;
  const userDetails = useUserStore((state) => state.userDetails);

  const isDark = useMemo(
    () =>
      userDetails?.currentTheme === THEME.DARK ||
      (!userDetails?.currentTheme &&
        window.matchMedia("(prefers-color-scheme: dark)").matches),
    [userDetails?.currentTheme]
  );

  // Apply dark/light class on document.body so CSS variables work on all pages
  // including auth pages (login, signup, etc.) that don't render Topbar
  useEffect(() => {
    if (isDark) {
      document.body.classList.add(THEME.DARK);
    } else {
      document.body.classList.remove(THEME.DARK);
    }
  }, [isDark]);

  return (
    <ConfigProvider
      direction={window.direction || "ltr"}
      theme={{
        algorithm: isDark ? darkAlgorithm : defaultAlgorithm,
      }}
    >
      <NotificationProvider>
        <BrowserRouter>
          <RouteComponent />
        </BrowserRouter>
      </NotificationProvider>
    </ConfigProvider>
  );
}

export { BaseComponent };
