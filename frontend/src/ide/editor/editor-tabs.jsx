import { getLanguageByExtension } from "../../common/constants.js";
import { LazyLoadComponent } from "../../widgets/lazy_loader";
import DbSchemaVisualization from "../explorer/db_schema/db-schema-visualization.jsx";
function getTabComponent(tabDetails) {
  const EDITOR_TAB_COMPONENTS = {
    NO_CODE_MODEL: (
      <LazyLoadComponent
        component={() => import("./no-code-model/no-code-model.jsx")}
        componentName="NoCodeModel"
        nodeData={tabDetails}
      />
    ),
    ROOT_DB: (
      <div style={{ height: "100%", width: "100%" }}>
        <DbSchemaVisualization />
      </div>
    ),
    LINEAGE: (
      <LazyLoadComponent
        component={() => import("./lineage-tab/lineage-tab.jsx")}
        componentName="LineageTab"
        nodeData={tabDetails}
      />
    ),
    SQL_FLOW: (
      <LazyLoadComponent
        component={() => import("./sql-flow-tab/sql-flow-tab.jsx")}
        componentName="SQLFlowTab"
        nodeData={tabDetails}
      />
    ),
  };

  let Component = EDITOR_TAB_COMPONENTS[tabDetails.node.type];

  if (!Component) {
    if (tabDetails.node.extension === "csv") {
      Component = (
        <LazyLoadComponent
          component={() => import("./csv-viewer.jsx")}
          componentName={"CsvViewer"}
          key={tabDetails.key}
          nodeData={tabDetails}
        />
      );
    } else {
      Component = (
        <LazyLoadComponent
          component={() => import("./monaco-editor.jsx")}
          componentName="MonacoEditor"
          key={tabDetails.key}
          nodeData={tabDetails}
          language={getLanguageByExtension(tabDetails.node.extension)}
        />
      );
    }
  }
  return Component;
}

export { getTabComponent };
