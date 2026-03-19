import { useEffect, memo } from "react";
import PropTypes from "prop-types";

import { Header } from "./Header";
import { Body } from "./Body";
import { useProjectStore } from "../../store/project-store";
import { getActiveModelName } from "../../common/helpers";

const PythonDrawer = memo(function PythonDrawer({
  isPythonDrawerOpen,
  closePythonDrawer,
}) {
  const { projectId, projectDetails } = useProjectStore.getState();
  const modelName = getActiveModelName(projectId, projectDetails);
  useEffect(() => {
    if (!modelName && isPythonDrawerOpen) {
      closePythonDrawer();
    }
  }, [modelName, isPythonDrawerOpen, closePythonDrawer]);

  return (
    <div className="chat-ai-container">
      <Header closePythonDrawer={closePythonDrawer} modelName={modelName} />
      <div className="flex-1 overflow-hidden">
        <Body
          isPythonDrawerOpen={isPythonDrawerOpen}
          projectId={projectId}
          modelName={modelName}
        />
      </div>
    </div>
  );
});

PythonDrawer.propTypes = {
  isPythonDrawerOpen: PropTypes.bool.isRequired,
  closePythonDrawer: PropTypes.func.isRequired,
};

PythonDrawer.displayName = "PythonDrawer";

export { PythonDrawer };
