import { useEffect, useState } from "react";

import { IdeExplorer } from "./explorer/explorer-component.jsx";
import { IdeEditor } from "./editor/editor-component.jsx";
import "./ide-layout.css";
import { useProjectStore } from "../store/project-store.js";
import { ChatLayout } from "./chat-ai/ChatLayout.jsx";
import { useNoCodeModelDrawerStore } from "../store/no-code-model-drawer-store.js";
import { useLineageTabStore } from "../store/lineage-tab-store.js";

function IdeComponent() {
  const [activeExpNode, setActiveExpNode] = useState({});
  const [deletedNode, setDeletedNode] = useState({});
  const [openNewModalPopup, setNewModalPopup] = useState(false);

  const [activeTab, setActiveTab] = useState(1);
  const [activeEditorTab, setActiveEditorTab] = useState();
  const { setProjectId } = useProjectStore();
  const { initializeAIDrawer } = useNoCodeModelDrawerStore();
  const { pendingLineageTab, clearPendingLineageTab } = useLineageTabStore();

  function onSelect(selectedNode) {
    setActiveExpNode(selectedNode);
  }

  function onDelete(deletedNode) {
    setDeletedNode(deletedNode);
  }

  function onTabChange(tabKey) {
    setActiveEditorTab(tabKey);
  }

  useEffect(() => {
    const id = location.pathname.split("/").pop();
    setProjectId(id);
    // Initialize AI drawer to open by default when project loads
    initializeAIDrawer();
  }, [initializeAIDrawer]);

  // Listen for pending lineage tab and open it
  useEffect(() => {
    if (pendingLineageTab) {
      const lineageTabData = {
        key: pendingLineageTab.key,
        node: {
          title: pendingLineageTab.title || "Lineage",
          type: "LINEAGE",
        },
      };
      setActiveExpNode(lineageTabData);
      clearPendingLineageTab();
    }
  }, [pendingLineageTab, clearPendingLineageTab]);

  return (
    <div className={`ideLayout`}>
      <ChatLayout>
        <div className="ideBody">
          <IdeExplorer
            onSelect={onSelect}
            currentNode={activeEditorTab}
            onDelete={onDelete}
            openNewModalPopup={openNewModalPopup}
            setNewModalPopup={setNewModalPopup}
            activeTab={activeTab}
            setActiveTab={setActiveTab}
          />
          <div className="flex-1">
            <IdeEditor
              setNewModalPopup={setNewModalPopup}
              currentTab={activeExpNode}
              deletedTab={deletedNode}
              onChangeTab={onTabChange}
              setActiveExpTab={setActiveTab}
            />
          </div>
        </div>
      </ChatLayout>
    </div>
  );
}

export { IdeComponent };
