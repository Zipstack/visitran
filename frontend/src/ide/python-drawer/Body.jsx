import { useEffect, useState } from "react";
import PropTypes from "prop-types";
import { Prism as SyntaxHighlighter } from "react-syntax-highlighter";
import { oneDark, vs } from "react-syntax-highlighter/dist/esm/styles/prism";

import { useUserStore } from "../../store/user-store";
import { THEME } from "../../common/constants.js";
import { fetchPythonContent } from "./PythonServices";
import { SpinnerLoader } from "../../widgets/spinner_loader/index.js";

export const Body = ({ isPythonDrawerOpen, projectId, modelName }) => {
  const [pythonContent, setPythonContent] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const { currentTheme } = useUserStore((state) => state.userDetails);
  const isDarkMode = currentTheme === THEME.DARK;

  useEffect(() => {
    const loadPythonContent = async () => {
      if (!isPythonDrawerOpen || !modelName) {
        setPythonContent("# No model selected.");
        return;
      }

      try {
        setLoading(true);
        setError(null);

        const content = await fetchPythonContent(modelName, projectId);
        setPythonContent(content);
      } catch (error) {
        console.error("Error fetching Python content:", error);
        setError("Failed to fetch Python content. Please try again.");
      } finally {
        setLoading(false);
      }
    };

    loadPythonContent();
  }, [isPythonDrawerOpen, modelName]);

  if (loading) {
    return <SpinnerLoader />;
  }

  if (error) {
    return (
      <div
        className="flex-center"
        style={{ height: "100%", padding: "20px", color: "red" }}
      >
        {error}
      </div>
    );
  }

  return (
    <div style={{ height: "100%", overflow: "auto" }}>
      <SyntaxHighlighter
        language="python"
        style={isDarkMode ? oneDark : vs}
        showLineNumbers
        wrapLines
        customStyle={{
          margin: 0,
          height: "100%",
          fontSize: "14px",
          borderRadius: 0,
        }}
      >
        {pythonContent || "# No Python content available"}
      </SyntaxHighlighter>
    </div>
  );
};

Body.propTypes = {
  isPythonDrawerOpen: PropTypes.bool.isRequired,
  projectId: PropTypes.string.isRequired,
  modelName: PropTypes.string.isRequired,
};
