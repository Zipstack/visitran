import { memo, useState, useEffect, useRef, useMemo } from "react";
import PropTypes from "prop-types";
import { Tabs, Typography } from "antd";
import isEmpty from "lodash/isEmpty";
import cloneDeep from "lodash/cloneDeep";
import debounce from "lodash/debounce";

import { getTabComponent } from "./editor-tabs.jsx";
import { getIconByType } from "../explorer/explorer-constants.js";
import { useProjectStore } from "../../store/project-store.js";
import "../ide-layout.css";
import { StartBanner } from "./start-banner.jsx";
import { useRefreshModelsStore } from "../../store/refresh-models-store.js";

let VersionControlBar;
try {
  VersionControlBar = require("../../plugins/version-control/components/VersionControlBar.jsx").VersionControlBar;
} catch {}

const { Text } = Typography;

const IdeEditor = memo(function IdeEditor({
  currentTab = {},
  deletedTab = {},
  onChangeTab = () => {},
  setNewModalPopup = () => {},
  setActiveExpTab = () => {},
}) {
  const {
    projectDetails = {},
    setOpenedTabs,
    makeActiveTab,
    previewTimeTravel,
    projectId,
  } = useProjectStore();
  const { refreshModels, setRefreshModels } = useRefreshModelsStore();

  // ensure we clone safely
  const projectDetailForId = cloneDeep(projectDetails[projectId] || {});
  const { openedTabs = [], focussedTab = {} } = projectDetailForId;

  const deriveTitleFromKey = (key) => {
    if (!key) return "";
    const parts = String(key).split("/");
    return parts?.[parts.length - 1] || key;
  };

  function normalizePersistedTab(tab) {
    const normalized = { ...tab };

    // if label missing, derive from key
    if (!normalized?.label || isEmpty(normalized?.label)) {
      normalized.label = deriveTitleFromKey(normalized?.key);
    }

    // ensure extension present if not given
    if (!normalized?.extension) {
      normalized.extension = normalized?.label?.includes(".")
        ? normalized?.label?.split(".")?.pop()
        : normalized?.label;
    }

    // ensure type exists (best-effort)
    if (!normalized?.type) {
      normalized.type = "NO_CODE_MODEL";
    }

    return normalized;
  }

  // deserialize initial tabs with safety
  const [tabItems, setTabItems] = useState(() =>
    deserializePersistedTabs(cloneDeep(openedTabs))
  );

  // activeTab starts from focussedTab.key or first tab or empty string
  const [activeTab, setActiveTab] = useState(
    focussedTab?.key || (tabItems[0] && tabItems[0].key) || ""
  );

  // ref keeps the raw persisted tabs (without JSX label/icon)
  const serializePersistingTabsRef = useRef(
    cloneDeep(projectDetails[projectId]?.openedTabs || [])
  );

  // Memoized lookup for database tab
  const existingDbTab = useMemo(() => {
    return tabItems.find((tab) => tab.type === "ROOT_DB");
  }, [tabItems]);

  useEffect(() => {
    addTab(currentTab);
  }, [currentTab]);

  useEffect(() => {
    removeTab(deletedTab);
  }, [deletedTab]);

  useEffect(() => {
    // Keep ref in sync with store persisted opened tabs
    serializePersistingTabsRef.current = cloneDeep(
      projectDetails[projectId]?.openedTabs || []
    );
    setTabItems(deserializePersistedTabs(cloneDeep(openedTabs)));

    // log current focussedTab for debugging
    // This sets activeTab safely:
    const newActive =
      projectDetails[projectId]?.focussedTab?.key ||
      (tabItems[0] && tabItems[0].key) ||
      "";
    setActiveTab(newActive);
  }, [
    projectId,
    projectDetails[projectId]?.openedTabs,
    projectDetails[projectId]?.focussedTab,
  ]);

  useEffect(() => {
    if (!refreshModels) return;

    const currentTab = tabItems?.find((tab) => tab?.key === activeTab);
    if (!activeTab || currentTab?.type !== "NO_CODE_MODEL") {
      setRefreshModels(false);
    }
  }, [refreshModels, tabItems, activeTab]);

  const onTabChange = debounce((activeKey) => {
    if (previewTimeTravel) {
      return;
    }
    const changedTabItems = tabItems.map((item) => {
      return { ...item, closable: item.key === activeKey };
    });
    updateTab(activeKey);
    setTabItems(changedTabItems);
  }, 20);

  function onEdit(targetKey, action) {
    if (action === "remove") {
      closeTab(targetKey);
    }
  }

  function addTab(newTab) {
    if (isEmpty(newTab) || isEmpty(newTab.node)) {
      return;
    }

    // Special handling for database tabs (ROOT_DB type)
    if (newTab.node?.type === "ROOT_DB" && existingDbTab) {
      // If a database tab is already open, no need to do anything else
      onTabChange(existingDbTab.key);
      return;
    }

    // If tab already exists, just activate it
    if (tabItems.some((tab) => tab.key === newTab.key)) {
      onTabChange(newTab.key);
      return;
    }

    // Prepare safe label/title
    const rawTitle =
      newTab.node.title ||
      newTab.label ||
      deriveTitleFromKey(newTab.key) ||
      newTab.key;

    // Ensure safeTitle is always a string (node.title can be JSX from tree badges)
    const safeTitle =
      typeof rawTitle === "string"
        ? rawTitle
        : newTab.label || deriveTitleFromKey(newTab.key) || newTab.key;

    const newTabItem = {
      label: safeTitle, // will be replaced with icon-wrapped JSX later
      key: newTab.key,
      type: newTab.node.type,
      extension:
        newTab.node.extension ||
        (safeTitle.includes(".") ? safeTitle.split(".").pop() : safeTitle),
    };

    // Add to persisted ref (normalized)
    serializePersistingTabs(newTabItem);

    // Once persisted, add icon-wrapped label and children
    newTabItem["label"] = addIconToTabTitle(newTabItem.label, newTabItem.type);
    newTabItem["children"] = getTabComponent(newTab);
    const newTabItems = [...tabItems, newTabItem];

    // Update close icon for previous active
    if (tabItems.length) {
      const activeTabIndex = tabItems.findIndex((tab) => tab.key === activeTab);
      if (activeTabIndex >= 0 && newTabItems[activeTabIndex]) {
        newTabItems[activeTabIndex]["closable"] = false;
      }
    }

    // Make this new tab active
    updateTab(newTab);
    setTabItems(newTabItems);
  }

  function closeTab(closedTabKey, updatedTabItems = tabItems) {
    const targetIndex = updatedTabItems.findIndex(
      (tab) => tab.key === closedTabKey
    );
    // removing tab
    const newTabItems = updatedTabItems.filter(
      (tab) => tab.key !== closedTabKey
    );

    // remove from persisted ref and update store
    serializePersistingTabsRef.current =
      serializePersistingTabsRef.current.filter(
        (tab) => tab.key !== closedTabKey
      );
    setOpenedTabs(serializePersistingTabsRef.current);

    // update existing opened tab to active tab
    if (newTabItems.length && activeTab === closedTabKey) {
      const newActiveTabIndex =
        targetIndex === newTabItems.length ? targetIndex - 1 : targetIndex;
      if (newTabItems[newActiveTabIndex]) {
        newTabItems[newActiveTabIndex]["closable"] = true;
        // updateTab expects a tab-like shape: pass the item
        updateTab(newTabItems[newActiveTabIndex]);
      }
    } else if (isEmpty(newTabItems)) {
      updateTab("");
    }
    setTabItems(newTabItems);
    return newTabItems;
  }

  function serializePersistingTabs(tabToSerialize) {
    const serializedTab = normalizePersistedTab(tabToSerialize);
    // ensure we don't add exact duplicate keys
    const exists = serializePersistingTabsRef.current.some(
      (t) => t.key === serializedTab.key
    );
    if (!exists) {
      serializePersistingTabsRef.current = [
        ...serializePersistingTabsRef.current,
        serializedTab,
      ];
      setOpenedTabs(serializePersistingTabsRef.current);
    } else {
      // If exists, make sure persisted version has label filled
      serializePersistingTabsRef.current =
        serializePersistingTabsRef.current.map((t) =>
          t.key === serializedTab.key ? { ...t, ...serializedTab } : t
        );
      setOpenedTabs(serializePersistingTabsRef.current);
    }
  }

  function deserializePersistedTabs(serializedTabs) {
    const deserializedTabs = (serializedTabs || []).map((tab) => {
      const safeTab = normalizePersistedTab(tab);
      return safeTab;
    });

    deserializedTabs.forEach((tab) => {
      tab["children"] = getTabComponent({
        node: {
          title: tab.label,
          type: tab.type,
          extension: tab.extension,
        },
        key: tab.key,
      });
      tab["label"] = addIconToTabTitle(tab.label, tab.type);
      tab["closable"] =
        deserializedTabs.length === 1 || tab.key === focussedTab.key;
    });
    return deserializedTabs;
  }

  function updateTab(tab = {}) {
    // Normalize and ensure node.title exists
    const tabDetails = getTabDetails(tab);

    // Ensure a label/title exists at source of truth (node.title can be JSX from tree badges)
    const rawTitle =
      tabDetails.node?.title ||
      tabDetails.label ||
      deriveTitleFromKey(tabDetails.key) ||
      "";
    const safeTitle =
      typeof rawTitle === "string"
        ? rawTitle
        : tabDetails.label || deriveTitleFromKey(tabDetails.key) || "";

    // ensure node and label are consistent
    tabDetails.node = {
      ...(tabDetails.node || {}),
      title: safeTitle,
      extension:
        tabDetails.node?.extension ||
        (safeTitle.includes(".") ? safeTitle.split(".").pop() : safeTitle),
    };
    if (!tabDetails.label || isEmpty(tabDetails.label)) {
      tabDetails.label = safeTitle;
    }

    // Update focussedTab in store with full shape (key, type, label, extension)
    // pass projectId so the store will write into correct projectDetails[projectId]
    try {
      makeActiveTab(
        {
          key: tabDetails.key || "",
          type: tabDetails.node?.type || "",
          label: tabDetails.label,
          extension: tabDetails.node?.extension,
        },
        projectId
      );
    } catch (err) {
      makeActiveTab(
        { key: tabDetails.key || "", type: tabDetails.node?.type || "" },
        projectId
      );
    }

    // update local active state & notify parent
    setActiveTab(tabDetails.key || "");
    onChangeTab(tabDetails.key || "");
  }

  function removeTab(deletedNode) {
    if (!deletedNode || !deletedNode.key) return;

    const updatedTabItems = closeTab(
      deletedNode.key,
      tabItems // pass the full list
    );

    setTabItems(updatedTabItems); // ensure state updates
  }

  function getTabDetails(tab) {
    if (!tab) {
      // empty -> return empty shaped object
      return { key: "", node: { type: "", title: "" }, label: "" };
    }
    if (tab && !tab.node?.type) {
      const key = tab.key || tab;
      const tabDetails = serializePersistingTabsRef.current?.find(
        (item) => item.key === key
      );

      const resolvedLabel = tabDetails?.label || deriveTitleFromKey(key) || key;
      const resolvedType = tabDetails?.type || "NO_CODE_MODEL";
      const resolvedExtension =
        tabDetails?.extension ||
        (resolvedLabel.includes(".")
          ? resolvedLabel.split(".").pop()
          : resolvedLabel);

      return {
        key,
        node: {
          type: resolvedType,
          title: resolvedLabel,
          extension: resolvedExtension,
        },
        label: resolvedLabel,
      };
    }

    // If tab already has node, ensure title is set
    if (tab && (tab.node?.title === undefined || isEmpty(tab.node.title))) {
      tab.node.title =
        tab.label ||
        focussedTab?.label ||
        deriveTitleFromKey(tab.key) ||
        tab.key;
    }
    return tab;
  }

  function addIconToTabTitle(title, type) {
    return (
      <Text>
        {getIconByType(type)}
        <Text className="icon-text">{title}</Text>
      </Text>
    );
  }

  const modifiedTabs = () => {
    return tabItems.map((el) => {
      if (el.type === "SEED_CSV_FILE") {
        const title = el.key.split("/").pop();
        el.label = addIconToTabTitle(title, el.type);
      }
      return el;
    });
  };

  // Handler for opening modal popup
  const handleOpenModal = () => setNewModalPopup(true);

  // Handler for activating database visualizer
  const handleActivateExp = () => {
    // Open the database visualizer tab, same as the bottom DB visualizer icon
    const dbTabType = "ROOT_DB";
    const dbTabKey = "database";
    const dbTabTitle = "Database";

    // Use the memoized existingDbTab instead of finding it again
    if (existingDbTab) {
      onTabChange(existingDbTab.key);
    } else {
      // Create a database tab in the editor
      const dbTabData = {
        key: dbTabKey,
        node: {
          title: dbTabTitle,
          type: dbTabType,
        },
      };

      // Add the tab to opened tabs
      const newTabItem = {
        label: dbTabData.node.title,
        key: dbTabData.key,
        type: dbTabData.node.type,
      };

      serializePersistingTabs(newTabItem);

      newTabItem.label = addIconToTabTitle(newTabItem.label, newTabItem.type);
      newTabItem.children = getTabComponent(dbTabData);

      const newTabItems = [...tabItems, newTabItem];
      setTabItems(newTabItems);
      updateTab(dbTabData);
    }
  };

  // Show StartBanner only when there are no tabs at all (neither in store nor local state)
  if (!focussedTab?.key && tabItems.length === 0) {
    return (
      <StartBanner
        onOpenModal={handleOpenModal}
        onActivateExp={handleActivateExp}
      />
    );
  }

  return (
    <div className="ideEditor">
      {VersionControlBar && <VersionControlBar />}
      <Tabs
        className="editor-tabs"
        hideAdd
        items={modifiedTabs()}
        activeKey={activeTab}
        type="editable-card"
        onChange={onTabChange}
        onEdit={onEdit}
      />
    </div>
  );
});

IdeEditor.propTypes = {
  currentTab: PropTypes.object,
  deletedTab: PropTypes.object,
  onChangeTab: PropTypes.func,
  setNewModalPopup: PropTypes.func,
  setActiveExpTab: PropTypes.func,
};

export { IdeEditor };
