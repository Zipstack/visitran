import { useEffect, useState } from "react";
import PropTypes from "prop-types";
import Editor from "@monaco-editor/react";

import { useUserStore } from "../../store/user-store.js";
import { useProjectStore } from "../../store/project-store.js";
import { THEME } from "../../common/constants";
import { SpinnerLoader } from "../../widgets/spinner_loader";
import { explorerService } from "../explorer/explorer-service.js";
import { useNotificationService } from "../../service/notification-service.js";

function MonacoEditor({ language = "python", nodeData = {} }) {
  const userDetails = useUserStore((state) => state.userDetails);
  const { projectId } = useProjectStore();
  const expService = explorerService();
  const { notify } = useNotificationService();

  const [fileContent, setFileContent] = useState();

  useEffect(function fetchFile() {
    expService
      .getFileContent(projectId, nodeData.key)
      .then((res) => {
        setFileContent(res.data);
      })
      .catch((error) => {
        console.error(error);
        notify({ error });
      });
  }, []);

  return (
    <Editor
      value={fileContent}
      language={language}
      height="100%"
      options={{ readOnly: true, scrollBeyondLastLine: false }}
      theme={userDetails.currentTheme === THEME.DARK ? "vs-dark" : "light"}
      loading={<SpinnerLoader />}
    />
  );
}

MonacoEditor.propTypes = {
  language: PropTypes.string,
  nodeData: PropTypes.object,
};

export { MonacoEditor };
