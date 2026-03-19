import { createRoot } from "react-dom/client";
import { PostHogProvider } from "posthog-js/react";
import posthog from "posthog-js";

import { LazyLoadComponent } from "./widgets/lazy_loader";
import "./index.css";
import { SpinnerLoader } from "./widgets/spinner_loader/index.js";

const POSTHOG_ENABLED = process.env.REACT_APP_POSTHOG_ENABLED;
if (POSTHOG_ENABLED !== "false") {
  // Define the PostHog API key and host URL
  const API_KEY = "phc_yaJfjjRNRSussXXiVQvNN2AXcrVkYvunI3YpDUdGnnS"; // gitleaks:allow
  const API_HOST = "https://us.i.posthog.com";

  // Initialize PostHog with the specified API key and host
  posthog.init(API_KEY, {
    api_host: API_HOST,
    autocapture: true,
  });
}

const root = createRoot(document.getElementById("root"));
root.render(
  <PostHogProvider client={posthog}>
    <LazyLoadComponent
      loader={
        <div className="height-100vh width-100">
          <SpinnerLoader />
        </div>
      }
      component={() => import("./app.jsx")}
      componentName="App"
    />
  </PostHogProvider>
);
