/**
 * Organization related service functions
 */

/**
 * Get the current organization ID from local storage or URL
 * @return {string|null} The organization ID or null if not found
 */
export const getOrgId = () => {
  try {
    // Try to get organization ID from local storage
    let orgId = localStorage.getItem("currentOrganizationId");

    // If not in localStorage, try sessionStorage
    if (!orgId) {
      orgId = sessionStorage.getItem("currentOrganizationId");
    }

    // If still not found, try to extract from URL
    if (!orgId) {
      const urlParams = new URLSearchParams(window.location.search);
      orgId = urlParams.get("org_id");
    }

    // If still not found but we're in a path that includes organization ID
    if (!orgId && window.location.pathname.includes("/organization/")) {
      const pathParts = window.location.pathname.split("/");
      const orgIndex = pathParts.indexOf("organization");
      if (orgIndex !== -1 && pathParts.length > orgIndex + 1) {
        orgId = pathParts[orgIndex + 1];
      }
    }

    return orgId;
  } catch (error) {
    console.error("Error retrieving organization ID:", error);
    return null;
  }
};

/**
 * Set the current organization ID in local storage
 * @param {string} orgId - The organization ID to set
 */
export const setOrgId = (orgId) => {
  try {
    if (orgId) {
      localStorage.setItem("currentOrganizationId", orgId);
    }
  } catch (error) {
    console.error("Error setting organization ID:", error);
  }
};
