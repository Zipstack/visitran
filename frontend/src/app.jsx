import { useEffect } from "react";

import { BaseComponent } from "./base/base-component.jsx";
import { ErrorBoundary } from "./widgets/error_boundary";
import { GlobalErrorFallback } from "./widgets/global-error-fallback";

function App() {
  const handleGlobalError = (errorInfo) => {
    console.error("Global Error Boundary caught an error:", errorInfo);
  };

  useEffect(() => {
    const handleUnhandledRejection = (event) => {
      console.error("Unhandled Promise Rejection:", event.reason);
      event.preventDefault();
    };

    window.addEventListener("unhandledrejection", handleUnhandledRejection);

    return () => {
      window.removeEventListener(
        "unhandledrejection",
        handleUnhandledRejection
      );
    };
  }, []);

  return (
    <ErrorBoundary
      fallbackComponent={<GlobalErrorFallback />}
      onError={handleGlobalError}
    >
      <BaseComponent />
    </ErrorBoundary>
  );
}

export { App };
