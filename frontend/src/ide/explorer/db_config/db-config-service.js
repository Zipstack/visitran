import Cookies from "js-cookie";

import { useAxiosPrivate } from "../../../service/axios-service";
import { orgStore } from "../../../store/org-store";
let options = {};

function dbConfigService() {
  const { selectedOrgId } = orgStore();
  const axiosPrivate = useAxiosPrivate();
  const csrfToken = Cookies.get("csrftoken");

  return {
    getDsList: () => {
      options = {
        url: `/api/v1/visitran/${selectedOrgId || "default_org"}/datasource`,
        method: "GET",
      };
      return axiosPrivate(options);
    },
    getDbConfig: (projectId) => {
      options = {
        url: `/api/v1/visitran/${
          selectedOrgId || "default_org"
        }/project/${projectId}/connection`,
        method: "GET",
      };
      return axiosPrivate(options);
    },
    getDsFields: (dsId) => {
      options = {
        url: `/api/v1/visitran/${
          selectedOrgId || "default_org"
        }/datasource/${dsId}/fields`,
        method: "GET",
      };
      return axiosPrivate(options);
    },
    setDbConfig: (projectId, dsId, data, update = false) => {
      options = {
        url: `/api/v1/visitran/${
          selectedOrgId || "default_org"
        }/project/${projectId}/connection/${dsId}/update`,
        method: update ? "PUT" : "POST",
        data,
        headers: {
          "X-CSRFToken": csrfToken,
        },
      };
      return axiosPrivate(options);
    },
    testConnection: (projectId, dsId, data) => {
      options = {
        url: `/api/v1/visitran/${
          selectedOrgId || "default_org"
        }/project/${projectId}/connection/${dsId}/test`,
        method: data ? "PUT" : "GET",
        ...(data && { data }),
        headers: {
          "X-CSRFToken": csrfToken,
        },
      };
      return axiosPrivate(options);
    },
  };
}

export { dbConfigService };
