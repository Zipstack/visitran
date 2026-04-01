import { useState, useEffect } from "react";
import PropTypes from "prop-types";
import { Segmented } from "antd";
import { DiffEditor } from "@monaco-editor/react";

import { useUserStore } from "../../store/user-store";
import { THEME } from "../../common/constants";
import { SpinnerLoader } from "../../widgets/spinner_loader";

const VIEW_OPTIONS = ["Split View", "Inline View"];

function DiffViewer({
  originalContent,
  modifiedContent,
  originalTitle,
  modifiedTitle,
  forceInline,
}) {
  const { currentTheme } = useUserStore((state) => state.userDetails);
  const [viewMode, setViewMode] = useState(
    window.innerWidth > 900 ? "Split View" : "Inline View"
  );

  useEffect(() => {
    if (forceInline) return;
    const handleResize = () => {
      setViewMode(window.innerWidth > 900 ? "Split View" : "Inline View");
    };
    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, [forceInline]);

  const renderSideBySide = !forceInline && viewMode === "Split View";

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      {!forceInline && (
        <div className="diff-viewer-header">
          <span>{originalTitle || "Original"}</span>
          <Segmented
            value={viewMode}
            onChange={setViewMode}
            options={VIEW_OPTIONS}
            size="small"
          />
          <span>{modifiedTitle || "Modified"}</span>
        </div>
      )}
      <div className="diff-viewer-container">
        <DiffEditor
          original={originalContent || ""}
          modified={modifiedContent || ""}
          language="yaml"
          height="100%"
          width="100%"
          theme={currentTheme === THEME.DARK ? "vs-dark" : "light"}
          options={{
            readOnly: true,
            scrollBeyondLastLine: false,
            renderSideBySide,
            enableSplitViewResizing: !forceInline,
            automaticLayout: true,
          }}
          loading={<SpinnerLoader />}
        />
      </div>
    </div>
  );
}

DiffViewer.propTypes = {
  originalContent: PropTypes.string,
  modifiedContent: PropTypes.string,
  originalTitle: PropTypes.string,
  modifiedTitle: PropTypes.string,
  forceInline: PropTypes.bool,
};

export { DiffViewer };
