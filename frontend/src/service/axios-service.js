import axios from "axios";
import { useEffect, useMemo } from "react";

import { useSessionStore } from "../store/session-store";

let useSubscriptionDetailsStoreSafe;
try {
  // `require` keeps tree-shaking intact; will fail gracefully in builds
  // where the file doesn’t exist.
  useSubscriptionDetailsStoreSafe =
    require("../plugins/store/subscription-details-store").useSubscriptionDetailsStore;
} catch {
  useSubscriptionDetailsStoreSafe = null;
}

function useAxiosPrivate() {
  const axiosPrivate = useMemo(() => axios.create(), []);
  const { setSessionDetails } = useSessionStore();

  const updateSubscriptionDetails =
    typeof useSubscriptionDetailsStoreSafe === "function"
      ? useSubscriptionDetailsStoreSafe().updateSubscriptionDetails
      : undefined;

  useEffect(() => {
    const responseInterceptor = axiosPrivate.interceptors.response.use(
      (response) => {
        return response;
      },
      async (error) => {
        if (error?.response?.status === 401) {
          localStorage.removeItem("orgid");
          localStorage.removeItem("session-storage");
          setSessionDetails({});
          return Promise.resolve({ suppressed: true });
        }
        if (error?.response?.status === 402) {
          const data = error?.response?.data;
          // Backend 402 responses (from subscription_helper.py):
          //  - TrialExpiredResponse:  { status: "failed", type: "subscription_error" }
          //  - ProjectLimitReached:   { status: "Project Limit Reached", ... }
          //  - OrgUserLimitReached:   { status: "Team Size Limit Reached", ... }
          //  - JobLimitReached:       { status: "Job Limit Reached", ... }
          const isTrialExpired =
            data?.status === "failed" && data?.type === "subscription_error";

          if (
            isTrialExpired &&
            typeof updateSubscriptionDetails === "function"
          ) {
            updateSubscriptionDetails({
              remainingDaysInSubscription: 0,
              isSubscriptionExpired: true,
            });
            return Promise.resolve({ suppressed: true });
          }
          // Limit-reached 402s propagate so the caller shows the real message.
          return Promise.reject(error);
        }
        return Promise.reject(error);
      }
    );
    return () => {
      axiosPrivate.interceptors.response.eject(responseInterceptor);
    };
  }, []);
  return axiosPrivate;
}
export { useAxiosPrivate };
