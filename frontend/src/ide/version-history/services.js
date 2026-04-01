const getVersionUrl = (orgId, projectId) =>
  `/api/v1/visitran/${orgId || "default_org"}/project/${projectId}/version`;

const getGitConfigUrl = (orgId, projectId) =>
  `/api/v1/visitran/${orgId || "default_org"}/project/${projectId}/git-config`;

// ── Version History ──

export async function fetchVersionHistory(axiosRef, orgId, projectId, page, limit) {
  const res = await axiosRef({
    method: "GET",
    url: `${getVersionUrl(orgId, projectId)}/versions`,
    params: { page, limit },
  });
  return res.data.data;
}

export async function fetchPendingChanges(axiosRef, orgId, projectId) {
  const res = await axiosRef({
    method: "GET",
    url: `${getVersionUrl(orgId, projectId)}/pending-changes`,
  });
  return res.data.data;
}

export async function fetchVersionDetail(axiosRef, orgId, projectId, versionNumber) {
  const res = await axiosRef({
    method: "GET",
    url: `${getVersionUrl(orgId, projectId)}/version/${versionNumber}`,
  });
  return res.data.data;
}

export async function commitProjectVersion(axiosRef, orgId, projectId, csrfToken, commitMessage) {
  const res = await axiosRef({
    method: "POST",
    url: `${getVersionUrl(orgId, projectId)}/commit`,
    data: { commit_message: commitMessage },
    headers: { "X-CSRFToken": csrfToken },
  });
  return res.data.data;
}

export async function compareVersions(axiosRef, orgId, projectId, fromVersion, toVersion) {
  const res = await axiosRef({
    method: "GET",
    url: `${getVersionUrl(orgId, projectId)}/compare`,
    params: { version_a: fromVersion, version_b: toVersion },
  });
  return res.data.data;
}

export async function executeRollback(axiosRef, orgId, projectId, csrfToken, targetVersion, reason) {
  const res = await axiosRef({
    method: "POST",
    url: `${getVersionUrl(orgId, projectId)}/rollback`,
    data: { version_number: targetVersion, reason },
    headers: { "X-CSRFToken": csrfToken },
  });
  return res.data.data;
}

export async function fetchAuditEvents(axiosRef, orgId, projectId, page, limit, filters) {
  const res = await axiosRef({
    method: "GET",
    url: `${getVersionUrl(orgId, projectId)}/audit`,
    params: { page, limit, ...filters },
  });
  return res.data.data;
}

export async function exportAuditCsv(axiosRef, orgId, projectId, filters) {
  const res = await axiosRef({
    method: "GET",
    url: `${getVersionUrl(orgId, projectId)}/audit`,
    params: { ...filters, format: "csv" },
    responseType: "blob",
  });
  return res.data;
}

export async function checkConflicts(axiosRef, orgId, projectId, modelName, csrfToken) {
  const res = await axiosRef({
    method: "POST",
    url: `${getVersionUrl(orgId, projectId)}/${modelName}/conflicts/check`,
    headers: { "X-CSRFToken": csrfToken },
  });
  return res.data.data;
}

export async function getConflicts(axiosRef, orgId, projectId, modelName) {
  const res = await axiosRef({
    method: "GET",
    url: `${getVersionUrl(orgId, projectId)}/${modelName}/conflicts`,
  });
  return res.data.data;
}

export async function resolveSingleConflict(axiosRef, orgId, projectId, modelName, csrfToken, conflictId, strategy, resolvedData) {
  const res = await axiosRef({
    method: "POST",
    url: `${getVersionUrl(orgId, projectId)}/${modelName}/conflicts/resolve`,
    data: {
      conflict_id: conflictId,
      strategy,
      ...(resolvedData && { resolved_data: resolvedData }),
    },
    headers: { "X-CSRFToken": csrfToken },
  });
  return res.data.data;
}

export async function finalizeConflictResolutions(axiosRef, orgId, projectId, modelName, csrfToken, commitMessage) {
  const res = await axiosRef({
    method: "POST",
    url: `${getVersionUrl(orgId, projectId)}/${modelName}/conflicts/finalize`,
    data: { commit_message: commitMessage },
    headers: { "X-CSRFToken": csrfToken },
  });
  return res.data.data;
}

export async function previewResolution(axiosRef, orgId, projectId, modelName) {
  const res = await axiosRef({
    method: "GET",
    url: `${getVersionUrl(orgId, projectId)}/${modelName}/conflicts/preview`,
  });
  return res.data.data;
}

export async function executeVersion(axiosRef, orgId, projectId, csrfToken, versionNumber, environment) {
  const res = await axiosRef({
    method: "POST",
    url: `${getVersionUrl(orgId, projectId)}/execute-version`,
    data: { version_number: versionNumber, environment: environment || {} },
    headers: { "X-CSRFToken": csrfToken },
  });
  return res.data;
}

export async function fetchDraftStatus(axiosRef, orgId, projectId) {
  const res = await axiosRef({
    method: "GET",
    url: `${getVersionUrl(orgId, projectId)}/draft-status`,
  });
  return res.data.data;
}

export async function retryGitSync(axiosRef, orgId, projectId, csrfToken, versionId) {
  const res = await axiosRef({
    method: "POST",
    url: `${getVersionUrl(orgId, projectId)}/retry-git-sync`,
    data: { version_id: versionId },
    headers: { "X-CSRFToken": csrfToken },
  });
  return res.data.data;
}

// ── Git Config ──

export async function fetchGitConfig(axiosRef, orgId, projectId) {
  const res = await axiosRef({
    method: "GET",
    url: getGitConfigUrl(orgId, projectId),
  });
  return res.data.data;
}

export async function saveGitConfig(axiosRef, orgId, projectId, csrfToken, payload) {
  const res = await axiosRef({
    method: "POST",
    url: `${getGitConfigUrl(orgId, projectId)}/save`,
    data: payload,
    headers: { "X-CSRFToken": csrfToken },
  });
  return res.data.data;
}

export async function deleteGitConfig(axiosRef, orgId, projectId, csrfToken) {
  const res = await axiosRef({
    method: "DELETE",
    url: `${getGitConfigUrl(orgId, projectId)}/delete`,
    headers: { "X-CSRFToken": csrfToken },
  });
  return res.data.data;
}

export async function testGitConnection(axiosRef, orgId, projectId, csrfToken, payload) {
  const res = await axiosRef({
    method: "POST",
    url: `${getGitConfigUrl(orgId, projectId)}/test`,
    data: payload,
    headers: { "X-CSRFToken": csrfToken },
  });
  return res.data.data;
}

export async function fetchAvailableRepos(axiosRef, orgId, projectId) {
  const res = await axiosRef({
    method: "GET",
    url: `${getGitConfigUrl(orgId, projectId)}/available-repos`,
  });
  return res.data.data;
}
