import axios from "axios";

import { useProjectStore } from "../../store/project-store";
import { orgStore } from "../../store/org-store";

export const fetchPythonContent = async (modelName, projectId) => {
  // Use provided projectId or fall back to store if not provided
  const storeProjectId = useProjectStore.getState().projectId;
  const effectiveProjectId = projectId || storeProjectId;
  // Get organization ID from the org store
  const orgId = orgStore.getState().selectedOrgId || "default_org";

  if (!effectiveProjectId || !modelName) {
    console.warn("Missing projectId or modelName for Python content.");
    return "# No Python content available";
  }

  // Use the same API endpoint as SQL query but with an additional parameter
  // to request Python content instead of SQL
  const url = `/api/v1/visitran/${orgId}/project/${effectiveProjectId}/lineage/${modelName}/info?type=python`;

  try {
    const response = await axios.get(url);

    // Extract the Python content from the response
    // The backend now includes a python_content field when type=python is specified
    return (
      response.data?.data?.python_content ||
      "# Python content will be available soon"
    );
  } catch (error) {
    console.error("Error fetching Python content:", error);
    return "# Error loading Python content";
  }
};
