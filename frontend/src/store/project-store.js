import { create } from "zustand";
import { persist } from "zustand/middleware";

const STORE_VARIABLES = {
  projectName: "",
  projectDetails: {},
  dbConfigDetails: {},
  previewTimeTravel: false,
  projectId: "",
  renamedModel: {},
};

const useProjectStore = create(
  persist(
    (setState, getState) => ({
      ...STORE_VARIABLES,
      setProjectName: (name) => {
        setState(() => {
          return { projectName: name };
        });
      },
      updateProjectDetails: (details, projectid = null) => {
        let projectId = projectid;
        const { projectDetails } = getState();
        if (!projectid) {
          projectId = getState().projectId;
        }
        if (projectDetails[projectId]) {
          projectDetails[projectId] = {
            ...projectDetails[projectId],
            ...details,
          };
        } else {
          projectDetails[projectId] = { ...details };
        }
        setState(() => {
          return {
            projectDetails,
          };
        });
      },
      setOpenedTabs: (details, projectId = null) => {
        getState().updateProjectDetails({ openedTabs: details }, projectId);
      },
      makeActiveTab: (details, projectId = null) => {
        getState().updateProjectDetails({ focussedTab: details }, projectId);
      },
      setPreview: (flag) => {
        setState(() => {
          return { previewTimeTravel: flag };
        });
      },
      setDbConfigDetails: (details) => {
        setState(() => {
          return { dbConfigDetails: details };
        });
      },
      setProjectId: (id) => {
        setState(() => {
          return { projectId: id };
        });
      },
      setRenamedModel: (modelNames) => {
        setState(() => {
          return { renamedModel: modelNames };
        });
      },
    }),
    {
      name: "project-tab-storage",
      partialize: (state) => ({
        projectDetails: state.projectDetails || {},
        projectName: state.projectName,
      }),
    }
  )
);
export { useProjectStore };
