export async function fetchDataSources(axiosRef, orgId) {
  const requestOptions = {
    method: "GET",
    url: `/api/v1/visitran/${orgId || "default_org"}/datasource`,
  };
  const res = await axiosRef(requestOptions);
  return res.data.datasource;
}

export async function fetchDataSourceFields(axiosRef, orgId, datasourceName) {
  const requestOptions = {
    method: "GET",
    url: `/api/v1/visitran/${
      orgId || "default_org"
    }/datasource/${datasourceName}/fields`,
  };
  const res = await axiosRef(requestOptions);
  return res.data.datasource_field_details || {};
}

export async function createConnectionApi(axiosRef, orgId, csrfToken, payload) {
  const requestOptions = {
    url: `/api/v1/visitran/${orgId || "default_org"}/connections/create`,
    method: "POST",
    data: payload,
    headers: { "X-CSRFToken": csrfToken },
  };
  const res = await axiosRef(requestOptions);
  return res;
}

export async function updateConnectionApi(
  axiosRef,
  orgId,
  csrfToken,
  connectionId,
  payload
) {
  const requestOptions = {
    url: `/api/v1/visitran/${
      orgId || "default_org"
    }/connection/${connectionId}/update`,
    method: "PUT",
    data: payload,
    headers: { "X-CSRFToken": csrfToken },
  };
  const res = await axiosRef(requestOptions);
  return res;
}

export async function fetchSingleConnection(axiosRef, orgId, connectionId) {
  const requestOptions = {
    method: "GET",
    url: `/api/v1/visitran/${
      orgId || "default_org"
    }/connection/${connectionId}`,
  };
  const res = await axiosRef(requestOptions);
  return res.data.data;
}

export async function revealConnectionCredentials(
  axiosRef,
  orgId,
  connectionId
) {
  const requestOptions = {
    method: "GET",
    url: `/api/v1/visitran/${
      orgId || "default_org"
    }/connection/${connectionId}/reveal`,
  };
  const res = await axiosRef(requestOptions);
  return res.data.data;
}

export async function fetchConnectionUsage(axiosRef, orgId, connectionId) {
  const requestOptions = {
    method: "GET",
    url: `/api/v1/visitran/${
      orgId || "default_org"
    }/connection/${connectionId}/usage`,
  };
  const res = await axiosRef(requestOptions);
  return res.data.data;
}

export async function testConnectionApi(
  axiosRef,
  orgId,
  csrfToken,
  datasource,
  connectionDetails,
  connectionId = null
) {
  const data = {
    datasource,
    connection_details: connectionDetails,
  };
  if (connectionId) {
    data.connection_id = connectionId;
  }
  const requestOptions = {
    url: `/api/v1/visitran/${orgId || "default_org"}/connection/test`,
    method: "POST",
    data,
    headers: { "X-CSRFToken": csrfToken },
  };
  const res = await axiosRef(requestOptions);
  return res;
}

// Environment
export async function fetchAllConnections(axiosRef, orgId) {
  const requestOptions = {
    method: "GET",
    url: `/api/v1/visitran/${orgId || "default_org"}/connections`,
  };
  const res = await axiosRef(requestOptions);
  return res.data.data.page_items;
}

export async function fetchProjectByConnection(axiosRef, orgId, envId) {
  const requestOptions = {
    method: "GET",
    url: `/api/v1/visitran/${
      orgId || "default_org"
    }/environment/${envId}/usage`,
  };
  const res = await axiosRef(requestOptions);
  return res.data.data;
}

export async function fetchSingleEnvironment(axiosRef, orgId, envId) {
  const requestOptions = {
    method: "GET",
    url: `/api/v1/visitran/${orgId || "default_org"}/environment/${envId}`,
  };
  const res = await axiosRef(requestOptions);
  return res.data.data;
}

export async function revealEnvironmentCredentials(axiosRef, orgId, envId) {
  const requestOptions = {
    method: "GET",
    url: `/api/v1/visitran/${
      orgId || "default_org"
    }/environment/${envId}/reveal`,
  };
  const res = await axiosRef(requestOptions);
  return res.data.data;
}

export async function updateEnvironmentApi(
  axiosRef,
  orgId,
  csrfToken,
  envId,
  payload
) {
  const requestOptions = {
    method: "PUT",
    url: `/api/v1/visitran/${
      orgId || "default_org"
    }/environment/${envId}/update`,
    data: payload,
    headers: { "X-CSRFToken": csrfToken },
  };
  const res = await axiosRef(requestOptions);
  return res.data;
}

export async function createEnvironmentApi(
  axiosRef,
  orgId,
  csrfToken,
  payload
) {
  const requestOptions = {
    method: "POST",
    url: `/api/v1/visitran/${orgId || "default_org"}/environments/create`,
    data: payload,
    headers: { "X-CSRFToken": csrfToken },
  };
  const res = await axiosRef(requestOptions);
  return res.data;
}

export async function fetchAllEnvironments(axiosRef, orgId, page, limit) {
  const requestOptions = {
    method: "GET",
    url: `/api/v1/visitran/${orgId || "default_org"}/environments`,
    params: {
      page,
      limit,
    },
  };
  const res = await axiosRef(requestOptions);
  return res.data.data;
}

export async function deleteEnvironmentApi(axiosRef, orgId, csrfToken, envId) {
  const requestOptions = {
    method: "DELETE",
    url: `/api/v1/visitran/${
      orgId || "default_org"
    }/environment/${envId}/delete`,
    headers: { "X-CSRFToken": csrfToken },
  };
  const res = await axiosRef(requestOptions);
  return res.data;
}

export async function deleteConnection(
  axiosRef,
  orgId,
  csrfToken,
  connectionId
) {
  const requestOptions = {
    method: "DELETE",
    url: `/api/v1/visitran/${
      orgId || "default_org"
    }/connection/${connectionId}/delete`,
    headers: { "X-CSRFToken": csrfToken },
  };
  const res = await axiosRef(requestOptions);
  return res.data;
}

export async function deleteAllConnectionsApi(axiosRef, orgId, csrfToken) {
  const requestOptions = {
    method: "DELETE",
    url: `/api/v1/visitran/${orgId || "default_org"}/connections/delete-all`,
    headers: { "X-CSRFToken": csrfToken },
  };
  const res = await axiosRef(requestOptions);
  return res.data;
}
