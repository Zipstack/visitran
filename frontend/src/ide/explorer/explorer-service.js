import Cookies from "js-cookie";

import { useAxiosPrivate } from "../../service/axios-service";
import { orgStore } from "../../store/org-store";
let options = {};

function explorerService() {
  const axiosPrivate = useAxiosPrivate();
  const csrfToken = Cookies.get("csrftoken");

  const { selectedOrgId } = orgStore();
  const removeProjectName = (path) => {
    let newPath = path.split("/");
    // removing first value which is project name
    newPath.shift();
    newPath = newPath.join("/");
    return newPath;
  };
  return {
    getExplorer: (projectId) => {
      options = {
        url: `/api/v1/visitran/${
          selectedOrgId || "default_org"
        }/project/${projectId}/explorer`,
        method: "GET",
      };
      return axiosPrivate(options);
    },
    getDbExplorer: (projectId, hardReload = false) => {
      options = {
        url: `/api/v1/visitran/${
          selectedOrgId || "default_org"
        }/project/${projectId}/explorer/database?reload=${hardReload}`,
        method: "GET",
      };
      return axiosPrivate(options);
    },
    getFileContent: (projectId, filePath) => {
      options = {
        url: `/api/v1/visitran/${
          selectedOrgId || "default_org"
        }/project/${projectId}/explorer/get`,
        method: "GET",
        params: { file_path: removeProjectName(filePath) },
      };
      return axiosPrivate(options);
    },
    createModel: (name, path, projectId) => {
      options = {
        url: `/api/v1/visitran/${
          selectedOrgId || "default_org"
        }/project/${projectId}/explorer/model/create`,
        method: "POST",
        data: {
          model_name: name,
          parent_path: removeProjectName(path),
        },
        headers: {
          "X-CSRFToken": csrfToken,
        },
      };
      return axiosPrivate(options);
    },
    createFolder: (name, path, projectId) => {
      options = {
        url: `/api/v1/visitran/${
          selectedOrgId || "default_org"
        }/project/${projectId}/explorer/folder/create`,
        method: "POST",
        data: {
          folder_name: name,
          parent_path: removeProjectName(path),
        },
        headers: {
          "X-CSRFToken": csrfToken,
        },
      };
      return axiosPrivate(options);
    },
    rename: (name, path, projectId) => {
      options = {
        url: `/api/v1/visitran/${
          selectedOrgId || "default_org"
        }/project/${projectId}/explorer/file/rename`,
        method: "POST",
        data: {
          file_name: removeProjectName(path),
          rename: removeProjectName(name),
        },
        headers: {
          "X-CSRFToken": csrfToken,
        },
      };
      return axiosPrivate(options);
    },
    deleteFolder: (projectId, path, obj) => {
      const keys = path.map((el) => {
        return removeProjectName(el);
      });
      const data = {
        file_name: keys,
        ...(obj?.type && { delete_table: obj.checked }),
        ...(obj?.type === "all" && { delete_all_models: true }),
      };
      options = {
        url: `/api/v1/visitran/${
          selectedOrgId || "default_org"
        }/project/${projectId}/explorer/file/delete`,
        method: "DELETE",
        data: data,
        headers: {
          "X-CSRFToken": csrfToken,
        },
      };
      return axiosPrivate(options);
    },
    runSeed: (projectId, fileName, schema) => {
      const isfileName = fileName ? false : true;
      options = {
        url: `/api/v1/visitran/${
          selectedOrgId || "default_org"
        }/project/${projectId}/execute/seed`,
        method: "POST",
        headers: {
          "X-CSRFToken": csrfToken,
        },
        data: {
          runAll: isfileName,
          fileName: fileName,
          schema_name: schema,
        },
      };
      return axiosPrivate(options);
    },

    runModel: (projectId, fileName) => {
      options = {
        url: `/api/v1/visitran/${
          selectedOrgId || "default_org"
        }/project/${projectId}/execute/run`,
        method: "POST",
        headers: {
          "X-CSRFToken": csrfToken,
        },
        data: {
          file_name: fileName,
        },
      };
      return axiosPrivate(options);
    },

    getAllSchema: (projectId) => {
      options = {
        url: `/api/v1/visitran/${
          selectedOrgId || "default_org"
        }/project/${projectId}/schemas`,
        method: "GET",
      };
      return axiosPrivate(options);
    },

    setProjectSchema: (projectId, schemaName) => {
      options = {
        url: `/api/v1/visitran/${
          selectedOrgId || "default_org"
        }/project/${projectId}/set_schema`,
        method: "POST",
        data: {
          schema_name: schemaName,
        },
        headers: {
          "X-CSRFToken": csrfToken,
        },
      };
      return axiosPrivate(options);
    },
  };
}

export { explorerService };
