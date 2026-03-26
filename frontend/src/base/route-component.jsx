import { useEffect } from "react";
import { Routes, Route, Navigate, Outlet } from "react-router-dom";

import { orgStore } from "../store/org-store";
import { useSessionStore } from "../store/session-store";
import { usePermissionStore } from "../store/permission-store";
import { useAxiosPrivate } from "../service/axios-service";
import { LazyLoadComponent } from "../widgets/lazy_loader";
import { SpinnerLoader } from "../widgets/spinner_loader";
import { Topbar } from "./components/topbar/Topbar.jsx";
import NotFound from "./not-found/NotFound.jsx";
import { RequireAuth } from "../auth/RequireAuth.js";
import EnvList from "./components/environment/EnvList.jsx";
import ConnectionList from "./components/connection/ConnectionList.jsx";
import Settings from "./components/settings/Settings";
import ErrorPage from "./error/Error";
import { useNotificationService } from "../service/notification-service.js";
// OSS login components - used when cloud plugins not available
import { Login } from "./components/login/Login.jsx";
import { Signup } from "./components/login/Signup.jsx";
import { ForgotPassword } from "./components/login/ForgotPassword.jsx";
import { ResetPassword } from "./components/login/ResetPassword.jsx";
import { Profile as ProfilePage } from "./components/settings/contents/Profile.jsx";
import { PersistentLogin } from "../auth/PersistentLogin.js";
import { RequireGuest } from "../auth/RequireGuest.js";

// Core component — always available
let ProjectListPage;
try {
  ProjectListPage =
    require("../base/project-listing/ProjectListing.jsx").ProjectListing;
} catch {
  // fallback handled below
}

// Job scheduler is now core functionality (not a plugin)
const JobListPage = require("../ide/scheduler/JobList.jsx").JobList;

// Run history is now core functionality (not a plugin)
const RunHistoryPage = require("../ide/run-history/Runhistory.jsx").Runhistory;

// Cloud-only components — absent in OSS, each loaded independently
let LandingPage;
let OrganizationPage;
let SubscriptionCheck;
let SlackNotification;
let SubscriptionAdminPage;
try {
  LandingPage = require("../plugins/landing/Landing.jsx").Landing;
} catch {
  /* plugin not available */
}
try {
  OrganizationPage =
    require("../plugins/organization/Organization.jsx").Organization;
} catch {
  /* plugin not available */
}
try {
  SubscriptionCheck =
    require("../plugins/subscription/SubscriptionCheck.jsx").SubscriptionCheck;
} catch {
  /* plugin not available */
}
try {
  SlackNotification =
    require("../plugins/slack-integration/SlackNotification.jsx").SlackNotification;
} catch {
  /* plugin not available */
}
try {
  SubscriptionAdminPage =
    require("../plugins/subscription/admin/SubscriptionAdminPage.jsx").SubscriptionAdminPage;
} catch {
  /* plugin not available */
}

// Cloud-only settings components — loaded from plugins/settings/ when available
let UserManagement;
let Roles;
let Resources;
let Permissions;
let Subscriptions;
let KeyManagement;
try {
  UserManagement = require("../plugins/settings/UserManagement").default;
} catch {
  /* plugin not available */
}
try {
  Roles = require("../plugins/settings/Roles").default;
} catch {
  /* plugin not available */
}
try {
  Resources = require("../plugins/settings/Resources").default;
} catch {
  /* plugin not available */
}
try {
  Permissions = require("../plugins/settings/Permissions").default;
} catch {
  /* plugin not available */
}
try {
  Subscriptions = require("../plugins/settings/Subscriptions").default;
} catch {
  /* plugin not available */
}
try {
  KeyManagement = require("../plugins/settings/keyManagement").default;
} catch {
  /* plugin not available */
}

function RouteComponent() {
  const axios = useAxiosPrivate();
  const { setPermissionDetails } = usePermissionStore();
  const { sessionDetails } = useSessionStore();
  const { selectedOrgId } = orgStore();
  const { notify } = useNotificationService();

  const isCloud = sessionDetails?.is_cloud;

  const getPermissions = async () => {
    try {
      const requestOptions = {
        method: "GET",
        url: `/api/v1/visitran/${
          selectedOrgId || "default_org"
        }/permissions_metrics`,
      };
      const res = await axios(requestOptions);
      setPermissionDetails(res.data);
    } catch (error) {
      console.error(error);
      notify({ error });
    }
  };

  useEffect(() => {
    if (selectedOrgId && isCloud) {
      getPermissions();
    }
  }, [selectedOrgId, sessionDetails?.user_role]);

  return (
    <Routes>
      <Route path="" element={<PersistentLogin />}>
        <Route path="" element={<RequireGuest />}>
          <Route
            path="/login"
            element={LandingPage ? <LandingPage /> : <Login />}
          />
          <Route path="/signup" element={<Signup />} />
          <Route path="/forgot-password" element={<ForgotPassword />} />
          <Route
            path="/reset-password/:uid/:token"
            element={<ResetPassword />}
          />
        </Route>

        <Route path="" element={<RequireAuth />}>
          {OrganizationPage && (
            <Route path="/org" element={<OrganizationPage />} />
          )}
          <Route path="" element={<Topbar />}>
            <Route
              path=""
              element={SubscriptionCheck ? <SubscriptionCheck /> : <Outlet />}
            >
              <Route
                path="/ide/project/:id"
                element={
                  <LazyLoadComponent
                    loader={<SpinnerLoader />}
                    component={() => import("../ide/ide-component.jsx")}
                    componentName="IdeComponent"
                  />
                }
              />
              <Route
                path="/ide/project"
                element={<Navigate to="/project/list" />}
              />
              <Route path="/ide" element={<Navigate to="/project/list" />} />

              <Route path="/project">
                <Route path="env/list" element={<EnvList />}></Route>
                <Route
                  path="connection/list"
                  element={<ConnectionList />}
                ></Route>

                <Route path="job/list" element={<JobListPage />} />
                <Route path="job/history" element={<RunHistoryPage />} />
                <Route path="list" element={<ProjectListPage />} />
                <Route path="setting" element={<Settings />}>
                  <Route index element={<Navigate to="profile" replace />} />
                  <Route path="profile" element={<ProfilePage />} />
                  {UserManagement && (
                    <Route path="usermanagement" element={<UserManagement />} />
                  )}
                  {Roles && <Route path="roles" element={<Roles />} />}
                  {Resources && (
                    <Route path="resources" element={<Resources />} />
                  )}
                  {Permissions && (
                    <Route path="permissions" element={<Permissions />} />
                  )}
                  {SubscriptionAdminPage && (
                    <Route
                      path="subscription-admin"
                      element={<SubscriptionAdminPage />}
                    />
                  )}
                  {Subscriptions && (
                    <Route path="subscriptions" element={<Subscriptions />} />
                  )}
                  {SlackNotification && sessionDetails?.is_org_admin && (
                    <Route
                      path="notification/slack"
                      element={<SlackNotification />}
                    />
                  )}
                  {KeyManagement && (
                    <Route path="keymanagement" element={<KeyManagement />} />
                  )}
                </Route>
              </Route>
            </Route>
          </Route>
        </Route>
      </Route>
      <Route path="/error" element={<ErrorPage />} />
      <Route path="*" element={<NotFound />} />
    </Routes>
  );
}
export { RouteComponent };
