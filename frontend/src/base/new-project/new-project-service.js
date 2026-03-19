// All API calls for NewProject

import Cookies from "js-cookie";

// Get all connections
export async function getAllConnectionsApi(axiosRef, orgId) {
  const req = {
    method: "GET",
    url: `/api/v1/visitran/${orgId || "default_org"}/connections`,
  };
  const res = await axiosRef(req);
  return res.data.data.page_items;
}

// Get all environments
export async function getAllEnvironmentsApi(axiosRef, orgId) {
  const req = {
    method: "GET",
    url: `/api/v1/visitran/${orgId || "default_org"}/environments`,
  };
  const res = await axiosRef(req);
  return res.data.data.page_items;
}

// Get single project details
export async function getSingleProjectDetailsApi(axiosRef, orgId, projId) {
  const req = {
    method: "GET",
    url: `/api/v1/visitran/${orgId || "default_org"}/project/${projId}`,
  };
  const res = await axiosRef(req);
  return res.data.data;
}

// Create project
export async function createProjectApi(axiosRef, orgId, formValues) {
  const csrfToken = Cookies.get("csrftoken");
  const req = {
    method: "POST",
    url: `/api/v1/visitran/${orgId || "default_org"}/project/create`,
    headers: { "X-CSRFToken": csrfToken },
    data: {
      ...formValues,
      connection: { id: formValues.connection },
      environment: formValues.environment
        ? { id: formValues.environment }
        : undefined,
    },
  };
  return axiosRef(req);
}

// Update project
export async function updateProjectApi(axiosRef, orgId, projId, formValues) {
  const csrfToken = Cookies.get("csrftoken");
  const req = {
    method: "PUT",
    url: `/api/v1/visitran/${orgId || "default_org"}/project/${projId}/update`,
    headers: { "X-CSRFToken": csrfToken },
    data: {
      ...formValues,
      connection: { id: formValues.connection },
      environment: formValues.environment
        ? { id: formValues.environment }
        : undefined,
    },
  };
  return axiosRef(req);
}
