import axios from "axios";

import { useProjectStore } from "../../store/project-store";
import { orgStore } from "../../store/org-store";

export const fetchSQLQuery = async (modelName) => {
  const { projectId } = useProjectStore.getState();
  // Get organization ID from the org store
  const orgId = orgStore.getState().selectedOrgId || "default_org";
  if (!projectId || !modelName) {
    console.warn("Missing projectId or modelName for SQL query.");
    return "No SQL available";
  }

  const url = `/api/v1/visitran/${orgId}/project/${projectId}/lineage/${modelName}/info`;

  try {
    const response = await axios.get(url);
    return response.data?.data?.sql || "No SQL available";
  } catch (error) {
    console.error("Error fetching SQL query:", error);
    return "Error loading SQL";
  }
};
