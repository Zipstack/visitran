import { useEffect, memo } from "react";
import PropTypes from "prop-types";

import { Header } from "./Header";
import { Body } from "./Body";
import { useProjectStore } from "../../store/project-store";
import { getActiveModelName } from "../../common/helpers";

const SQLDrawer = memo(function SQLDrawer({ isSQLDrawerOpen, closeSQLDrawer }) {
  const { projectId, projectDetails } = useProjectStore.getState();

  const modelName = getActiveModelName(projectId, projectDetails);
  useEffect(() => {
    if (!modelName && isSQLDrawerOpen) {
      closeSQLDrawer();
    }
  }, [modelName, isSQLDrawerOpen, closeSQLDrawer]);

  return (
    <div className="chat-ai-container">
      <Header closeSQLDrawer={closeSQLDrawer} />
      <div className="flex-1 overflow-hidden">
        <Body isSQLDrawerOpen={isSQLDrawerOpen} activeModel={modelName} />
      </div>
    </div>
  );
});

SQLDrawer.propTypes = {
  isSQLDrawerOpen: PropTypes.bool.isRequired,
  closeSQLDrawer: PropTypes.func.isRequired,
};

SQLDrawer.displayName = "SQLDrawer";

export { SQLDrawer };
