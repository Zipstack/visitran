import Cookies from "js-cookie";

import { useAxiosPrivate } from "../../service/axios-service";
import { orgStore } from "../../store/org-store";
import { useProjectStore } from "../../store/project-store";

export function useJobService() {
  const axiosPrivate = useAxiosPrivate();
  const csrfToken = Cookies.get("csrftoken");
  const { selectedOrgId } = orgStore();
  const { projectId: storeProjectId } = useProjectStore();
  const orgId = selectedOrgId || "default_org";

  const headers = {
    "Content-Type": "application/json",
    "X-CSRFToken": csrfToken,
  };

  // Build jobs base URL — projId param takes priority over store
  const jobsUrl = (projId) =>
    `/api/v1/visitran/${orgId}/project/${projId || storeProjectId}/jobs`;

  const listPeriodicTasks = async (page, limit) => {
    // Job list page may not have a project selected.
    // Backend filters by org anyway, so use placeholder when no project.
    const url = `${jobsUrl(storeProjectId || "_all")}/list-periodic-tasks`;
    const response = await axiosPrivate.get(url, {
      params: { page, limit },
    });
    return response.data;
  };

  const getPeriodicTask = async (userTaskId, projId) => {
    // Use "_all" placeholder when no project selected (backend filters by org)
    const url = `${jobsUrl(
      projId || storeProjectId || "_all"
    )}/list-periodic-task/${userTaskId}`;
    const response = await axiosPrivate.get(url);
    return response.data;
  };

  const createTask = async (data, projId) => {
    const url = `${jobsUrl(projId)}/create-periodic-task`;
    const response = await axiosPrivate.post(url, data, { headers });
    return response.data;
  };

  const updateTask = async (projId, taskId, data) => {
    const url = `${jobsUrl(projId)}/update/${taskId}`;
    const response = await axiosPrivate.post(url, data, { headers });
    return response.data;
  };

  const deleteTask = async (projId, taskId) => {
    const url = `${jobsUrl(projId)}/delete-periodic-task/${taskId}`;
    const response = await axiosPrivate.delete(url, { headers });
    return response.data;
  };

  const runTask = async (projId, taskId) => {
    const url = `${jobsUrl(projId)}/trigger-periodic-task/${taskId}`;
    const response = await axiosPrivate.post(url, {}, { headers });
    return response.data;
  };

  const runTaskForModel = async (projId, taskId, modelName) => {
    const url = `${jobsUrl(
      projId
    )}/trigger-periodic-task/${taskId}/model/${encodeURIComponent(modelName)}`;
    const response = await axiosPrivate.post(url, {}, { headers });
    return response.data;
  };

  const listDeployCandidates = async (projId, modelName) => {
    const url = `${jobsUrl(
      projId
    )}/quick-deploy/candidates/${encodeURIComponent(modelName)}`;
    const response = await axiosPrivate.get(url);
    return response.data?.data || [];
  };

  const listRecentRunsForModel = async (projId, modelName, limit = 5) => {
    const url = `${jobsUrl(
      projId
    )}/quick-deploy/recent-runs/${encodeURIComponent(modelName)}`;
    const response = await axiosPrivate.get(url, { params: { limit } });
    return response.data?.data || [];
  };

  const getProjects = async () => {
    const url = `/api/v1/visitran/${orgId}/projects`;
    const response = await axiosPrivate.get(url);
    // Projects API returns {page_items: [...], total, page, page_size}
    return response.data?.page_items || [];
  };

  const getEnvironments = async () => {
    const url = `/api/v1/visitran/${orgId}/environments`;
    const response = await axiosPrivate.get(url);
    return response.data?.data?.page_items || response.data?.data || [];
  };

  const getProjectModels = async (projId) => {
    const url = `/api/v1/visitran/${orgId}/project/${
      projId || storeProjectId
    }/explorer`;
    const response = await axiosPrivate.get(url);
    return response.data;
  };

  const detectWatermarkColumns = async (projId, environmentId, tableName) => {
    const url = `${jobsUrl(projId)}/watermark/detect`;
    const response = await axiosPrivate.post(
      url,
      { environment_id: environmentId, table_name: tableName },
      { headers }
    );
    return response.data;
  };

  // Get model columns for incremental config
  // Returns: { destination_columns, source_columns }
  const getModelColumns = async (projId, modelName) => {
    const url = `/api/v1/visitran/${orgId}/project/${
      projId || storeProjectId
    }/jobs/model/${modelName}/columns`;
    const response = await axiosPrivate.get(url);
    // Response: { destination_columns: [...], source_columns: [...], columns: [...] }
    return {
      destinationColumns:
        response.data?.destination_columns || response.data?.columns || [],
      sourceColumns:
        response.data?.source_columns || response.data?.columns || [],
    };
  };

  return {
    listPeriodicTasks,
    getPeriodicTask,
    createTask,
    updateTask,
    deleteTask,
    runTask,
    runTaskForModel,
    listDeployCandidates,
    listRecentRunsForModel,
    getProjects,
    getEnvironments,
    getProjectModels,
    detectWatermarkColumns,
    getModelColumns,
  };
}
