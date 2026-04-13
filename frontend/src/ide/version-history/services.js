const getVersionUrl = (orgId, projectId) =>
  `/api/v1/visitran/${orgId || "default_org"}/project/${projectId}/version`;

const getGitConfigUrl = (orgId, projectId) =>
  `/api/v1/visitran/${orgId || "default_org"}/project/${projectId}/git-config`;

// ── Version History ──

export async function fetchVersionHistory(
  axiosRef,
  orgId,
  projectId,
  page,
  limit
) {
  const res = await axiosRef({
    method: "GET",
    url: `${getVersionUrl(orgId, projectId)}/versions`,
    params: { page, limit },
  });
  return res.data.data;
}

export async function fetchVersionDetail(
  axiosRef,
  orgId,
  projectId,
  versionNumber
) {
  const res = await axiosRef({
    method: "GET",
    url: `${getVersionUrl(orgId, projectId)}/version/${versionNumber}`,
  });
  return res.data.data;
}

export async function commitProjectVersion(
  axiosRef,
  orgId,
  projectId,
  csrfToken,
  title,
  description
) {
  const res = await axiosRef({
    method: "POST",
    url: `${getVersionUrl(orgId, projectId)}/commit`,
    data: { title, description: description || "" },
    headers: { "X-CSRFToken": csrfToken },
  });
  return res.data.data;
}

export async function compareVersions(
  axiosRef,
  orgId,
  projectId,
  fromVersion,
  toVersion
) {
  const res = await axiosRef({
    method: "GET",
    url: `${getVersionUrl(orgId, projectId)}/compare`,
    params: { version_a: fromVersion, version_b: toVersion },
  });
  return res.data.data;
}

export async function executeRollback(
  axiosRef,
  orgId,
  projectId,
  csrfToken,
  targetVersion,
  reason
) {
  const res = await axiosRef({
    method: "POST",
    url: `${getVersionUrl(orgId, projectId)}/rollback`,
    data: { version_number: targetVersion, reason },
    headers: { "X-CSRFToken": csrfToken },
  });
  return res.data.data;
}

export async function executeVersion(
  axiosRef,
  orgId,
  projectId,
  csrfToken,
  versionNumber,
  environment,
  commitSha
) {
  const data = { version_number: versionNumber, environment: environment || {} };
  if (commitSha) data.commit_sha = commitSha;
  const res = await axiosRef({
    method: "POST",
    url: `${getVersionUrl(orgId, projectId)}/execute-version`,
    data,
    headers: { "X-CSRFToken": csrfToken },
  });
  return res.data;
}

export async function retryGitSync(
  axiosRef,
  orgId,
  projectId,
  csrfToken,
  versionId
) {
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

export async function saveGitConfig(
  axiosRef,
  orgId,
  projectId,
  csrfToken,
  payload
) {
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

export async function testGitConnection(
  axiosRef,
  orgId,
  projectId,
  csrfToken,
  payload
) {
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

export async function fetchBranches(axiosRef, orgId, projectId) {
  const res = await axiosRef({
    method: "GET",
    url: `${getGitConfigUrl(orgId, projectId)}/branches`,
  });
  return res.data.data?.branches || [];
}

export async function updatePRMode(
  axiosRef,
  orgId,
  projectId,
  csrfToken,
  prMode,
  prBaseBranch
) {
  const res = await axiosRef({
    method: "POST",
    url: `${getGitConfigUrl(orgId, projectId)}/enable-pr-workflow`,
    data: {
      pr_mode: prMode,
      pr_base_branch: prBaseBranch,
    },
    headers: { "X-CSRFToken": csrfToken },
  });
  return res.data.data;
}

export async function createBranch(
  axiosRef,
  orgId,
  projectId,
  csrfToken,
  payload
) {
  const res = await axiosRef({
    method: "POST",
    url: `${getGitConfigUrl(orgId, projectId)}/create-branch`,
    data: payload,
    headers: { "X-CSRFToken": csrfToken },
  });
  return res.data.data;
}

export async function createVersionPR(
  axiosRef,
  orgId,
  projectId,
  csrfToken,
  versionNumber
) {
  const res = await axiosRef({
    method: "POST",
    url: `${getVersionUrl(
      orgId,
      projectId
    )}/version/${versionNumber}/create-pr`,
    headers: { "X-CSRFToken": csrfToken },
  });
  return res.data.data;
}

export async function fetchVersionPR(
  axiosRef,
  orgId,
  projectId,
  versionNumber
) {
  const res = await axiosRef({
    method: "GET",
    url: `${getVersionUrl(orgId, projectId)}/version/${versionNumber}/pr`,
  });
  return res.data.data;
}

// ── Import from Branch ──

export async function listProjectFolders(
  axiosRef,
  orgId,
  projectId,
  csrfToken,
  payload
) {
  const res = await axiosRef({
    method: "POST",
    url: `${getGitConfigUrl(orgId, projectId)}/project-folders`,
    data: payload,
    headers: { "X-CSRFToken": csrfToken },
  });
  return res.data.data;
}

export async function importFromBranch(
  axiosRef,
  orgId,
  projectId,
  csrfToken,
  payload
) {
  const res = await axiosRef({
    method: "POST",
    url: `${getVersionUrl(orgId, projectId)}/import`,
    data: payload,
    headers: { "X-CSRFToken": csrfToken },
  });
  return res.data.data;
}
