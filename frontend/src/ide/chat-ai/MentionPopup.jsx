import { memo, useState, useEffect, useRef, useMemo, useCallback } from "react";
import PropTypes from "prop-types";
import {
  DatabaseOutlined,
  FolderOutlined,
  TableOutlined,
  ArrowLeftOutlined,
  SearchOutlined,
  FileOutlined,
  LoadingOutlined,
} from "@ant-design/icons";

/* ── Flat search across all schemas/tables and model folders/files ── */

function buildSearchItems(dbLookup, modelsLookup) {
  const items = [];

  // Database tables: schema → table
  Object.entries(dbLookup || {}).forEach(([schema, tables]) => {
    Object.keys(tables).forEach((table) => {
      items.push({
        type: "table",
        label: table,
        description: schema,
        insertText: `@${schema}.${table}`,
        searchText: `${schema} ${table}`.toLowerCase(),
      });
    });
  });

  // Models: folder → model
  Object.entries(modelsLookup || {}).forEach(([folder, models]) => {
    (models || []).forEach((model) => {
      items.push({
        type: "model",
        label: model,
        description: folder,
        insertText: `@models/${folder}/${model}`,
        searchText: `${folder} ${model}`.toLowerCase(),
      });
    });
  });

  return items;
}

/* ── Navigation levels ── */
const LEVEL_ROOT = "root";
const LEVEL_SCHEMAS = "schemas";
const LEVEL_TABLES = "tables";
const LEVEL_MODEL_FOLDERS = "model_folders";
const LEVEL_MODELS = "models";

/* ── Styles ── */

const basePopupStyle = {
  position: "absolute",
  left: 0,
  right: 0,
  backgroundColor: "var(--modal-bg)",
  border: "1px solid var(--border-color-1)",
  borderRadius: "8px",
  boxShadow: "0 6px 16px rgba(0, 0, 0, 0.25)",
  overflow: "hidden",
  zIndex: 1050,
  maxHeight: "320px",
  display: "flex",
  flexDirection: "column",
};

const getPopupStyle = (placement) => ({
  ...basePopupStyle,
  ...(placement === "bottom"
    ? { top: "100%", marginTop: "4px" }
    : { bottom: "100%", marginBottom: "4px" }),
});

const headerStyle = {
  display: "flex",
  alignItems: "center",
  gap: "6px",
  padding: "6px 12px",
  borderBottom: "1px solid var(--border-color-1)",
  flexShrink: 0,
};

const backBtnStyle = {
  cursor: "pointer",
  color: "var(--circle-color)",
  fontSize: "12px",
  display: "flex",
  alignItems: "center",
};

const breadcrumbStyle = {
  fontSize: "11px",
  color: "var(--circle-color)",
  fontWeight: 500,
};

const searchWrapperStyle = {
  display: "flex",
  alignItems: "center",
  gap: "6px",
  padding: "6px 12px",
  borderBottom: "1px solid var(--border-color-1)",
  flexShrink: 0,
};

const searchInputStyle = {
  flex: 1,
  border: "none",
  outline: "none",
  background: "transparent",
  color: "var(--panel-lable)",
  fontSize: "13px",
};

const listStyle = {
  overflowY: "auto",
  flex: 1,
};

const itemStyle = (isActive) => ({
  display: "flex",
  alignItems: "center",
  gap: "8px",
  padding: "8px 12px",
  cursor: "pointer",
  backgroundColor: isActive ? "var(--hover-bg)" : "transparent",
  transition: "background-color 0.15s",
});

const iconStyle = {
  color: "var(--icons-color)",
  fontSize: "14px",
  flexShrink: 0,
};

const labelStyle = {
  fontSize: "13px",
  fontWeight: 500,
  color: "var(--panel-lable)",
};

const descStyle = {
  fontSize: "11px",
  color: "var(--circle-color)",
};

const emptyStyle = {
  padding: "16px 12px",
  fontSize: "12px",
  color: "var(--circle-color)",
  textAlign: "center",
};

const groupHeaderStyle = {
  padding: "4px 12px 2px",
  fontSize: "10px",
  fontWeight: 600,
  color: "var(--circle-color)",
  textTransform: "uppercase",
  letterSpacing: "0.5px",
};

/* ── Component ── */

const MentionPopup = memo(function MentionPopup({
  dbData,
  modelsData,
  seedsData,
  searchText,
  onSelect,
  onClose,
  onSearchChange,
  placement = "top",
}) {
  const [level, setLevel] = useState(LEVEL_ROOT);
  const [selectedSchema, setSelectedSchema] = useState(null);
  const [selectedFolder, setSelectedFolder] = useState(null);
  const [activeIndex, setActiveIndex] = useState(0);
  const listRef = useRef(null);
  const searchInputRef = useRef(null);

  // Build lookup maps
  const { dbLookup, modelsLookup } = useMemo(() => {
    const db = {};
    dbData?.children?.forEach((schema) => {
      const tables = {};
      schema.children?.forEach((tbl) => {
        tables[tbl.title] = tbl.children?.map((col) => col.title) || [];
      });
      db[schema.title] = tables;
    });

    const models = {};
    modelsData?.children?.forEach((folder) => {
      models[folder.title] = folder.children?.map((c) => c.title) || [];
    });

    return { dbLookup: db, modelsLookup: models };
  }, [dbData, modelsData]);

  // Flat search items for global search
  const allSearchItems = useMemo(
    () => buildSearchItems(dbLookup, modelsLookup),
    [dbLookup, modelsLookup]
  );

  // Build the visible items list based on current level + search
  const items = useMemo(() => {
    const query = (searchText || "").toLowerCase().trim();

    // If searching, show flat filtered results
    if (query && level === LEVEL_ROOT) {
      const tableResults = [];
      const modelResults = [];

      allSearchItems.forEach((item) => {
        if (item.searchText.includes(query)) {
          if (item.type === "table") tableResults.push(item);
          else modelResults.push(item);
        }
      });

      const results = [];
      if (tableResults.length > 0) {
        results.push({ type: "group_header", label: "Database Tables" });
        results.push(...tableResults.slice(0, 20));
      }
      if (modelResults.length > 0) {
        results.push({ type: "group_header", label: "Models" });
        results.push(...modelResults.slice(0, 20));
      }
      return results;
    }

    // Hierarchical browsing
    if (level === LEVEL_ROOT) {
      const rootItems = [];
      const dbReady = Object.keys(dbLookup).length > 0;
      // Always show Databases — with loading or schema count
      rootItems.push({
        type: dbReady ? "category" : "loading",
        key: "databases",
        label: "Databases",
        description: dbReady
          ? `${Object.keys(dbLookup).length} schema(s)`
          : "Loading...",
        icon: dbReady ? (
          <DatabaseOutlined style={iconStyle} />
        ) : (
          <LoadingOutlined style={iconStyle} spin />
        ),
      });
      if (Object.keys(modelsLookup).length > 0) {
        rootItems.push({
          type: "category",
          key: "models",
          label: "Models",
          description: `${Object.keys(modelsLookup).length} folder(s)`,
          icon: <FolderOutlined style={iconStyle} />,
        });
      }
      return rootItems;
    }

    if (level === LEVEL_SCHEMAS) {
      return Object.entries(dbLookup)
        .map(([schema, tables]) => ({
          type: "schema",
          label: schema,
          description: `${Object.keys(tables).length} table(s)`,
          icon: <FolderOutlined style={iconStyle} />,
        }))
        .filter((item) => !query || item.label.toLowerCase().includes(query));
    }

    if (level === LEVEL_TABLES && selectedSchema) {
      const tables = dbLookup[selectedSchema] || {};
      return Object.keys(tables)
        .map((table) => ({
          type: "table",
          label: table,
          description: selectedSchema,
          insertText: `@${selectedSchema}.${table}`,
          icon: <TableOutlined style={iconStyle} />,
        }))
        .filter((item) => !query || item.label.toLowerCase().includes(query));
    }

    if (level === LEVEL_MODEL_FOLDERS) {
      return Object.entries(modelsLookup)
        .map(([folder, models]) => ({
          type: "model_folder",
          label: folder,
          description: `${models.length} model(s)`,
          icon: <FolderOutlined style={iconStyle} />,
        }))
        .filter((item) => !query || item.label.toLowerCase().includes(query));
    }

    if (level === LEVEL_MODELS && selectedFolder) {
      const models = modelsLookup[selectedFolder] || [];
      return models
        .map((model) => ({
          type: "model",
          label: model,
          description: selectedFolder,
          insertText: `@models/${selectedFolder}/${model}`,
          icon: <FileOutlined style={iconStyle} />,
        }))
        .filter((item) => !query || item.label.toLowerCase().includes(query));
    }

    return [];
  }, [
    level,
    searchText,
    dbLookup,
    modelsLookup,
    selectedSchema,
    selectedFolder,
    allSearchItems,
  ]);

  // Selectable items (exclude group headers)
  const selectableItems = useMemo(
    () =>
      items.filter(
        (item) => item.type !== "group_header" && item.type !== "loading"
      ),
    [items]
  );

  // Reset active index when items change
  useEffect(() => {
    setActiveIndex(0);
  }, [items.length, level, searchText]);

  // Scroll active item into view
  useEffect(() => {
    if (!listRef.current) return;
    const activeEl = listRef.current.querySelector(
      `[data-index="${activeIndex}"]`
    );
    activeEl?.scrollIntoView({ block: "nearest" });
  }, [activeIndex]);

  // Focus search input when popup opens or level changes
  useEffect(() => {
    // Small delay to ensure DOM is ready
    const timer = setTimeout(() => {
      searchInputRef.current?.focus();
    }, 50);
    return () => clearTimeout(timer);
  }, [level]);

  const goBack = useCallback(() => {
    onSearchChange("");
    if (level === LEVEL_SCHEMAS || level === LEVEL_MODEL_FOLDERS) {
      setLevel(LEVEL_ROOT);
    } else if (level === LEVEL_TABLES) {
      setLevel(LEVEL_SCHEMAS);
      setSelectedSchema(null);
    } else if (level === LEVEL_MODELS) {
      setLevel(LEVEL_MODEL_FOLDERS);
      setSelectedFolder(null);
    }
  }, [level, onSearchChange]);

  const handleItemClick = useCallback(
    (item) => {
      if (item.type === "group_header" || item.type === "loading") return;

      if (item.type === "category") {
        onSearchChange("");
        if (item.key === "databases") setLevel(LEVEL_SCHEMAS);
        else if (item.key === "models") setLevel(LEVEL_MODEL_FOLDERS);
        return;
      }

      if (item.type === "schema") {
        onSearchChange("");
        setSelectedSchema(item.label);
        setLevel(LEVEL_TABLES);
        return;
      }

      if (item.type === "model_folder") {
        onSearchChange("");
        setSelectedFolder(item.label);
        setLevel(LEVEL_MODELS);
        return;
      }

      // Terminal item (table or model) — insert mention
      if (item.insertText) {
        onSelect(item.insertText);
      }
    },
    [onSelect, onSearchChange]
  );

  // Keyboard navigation
  useEffect(() => {
    const handleKeyDown = (e) => {
      if (e.key === "ArrowDown") {
        e.preventDefault();
        e.stopPropagation();
        setActiveIndex((prev) =>
          prev < selectableItems.length - 1 ? prev + 1 : 0
        );
      } else if (e.key === "ArrowUp") {
        e.preventDefault();
        e.stopPropagation();
        setActiveIndex((prev) =>
          prev > 0 ? prev - 1 : selectableItems.length - 1
        );
      } else if (e.key === "Enter" || e.key === "Tab") {
        e.preventDefault();
        e.stopPropagation();
        if (selectableItems[activeIndex]) {
          handleItemClick(selectableItems[activeIndex]);
        }
      } else if (e.key === "Escape") {
        e.preventDefault();
        e.stopPropagation();
        onClose();
      } else if (e.key === "Backspace" && !searchText) {
        if (level !== LEVEL_ROOT) {
          e.preventDefault();
          e.stopPropagation();
          goBack();
        }
      }
    };

    document.addEventListener("keydown", handleKeyDown, true);
    return () => document.removeEventListener("keydown", handleKeyDown, true);
  }, [
    activeIndex,
    selectableItems,
    handleItemClick,
    onClose,
    searchText,
    level,
    goBack,
  ]);

  // Build breadcrumb label
  const breadcrumb = useMemo(() => {
    if (level === LEVEL_ROOT) return "Mention";
    if (level === LEVEL_SCHEMAS) return "Databases";
    if (level === LEVEL_TABLES) return selectedSchema;
    if (level === LEVEL_MODEL_FOLDERS) return "Models";
    if (level === LEVEL_MODELS) return selectedFolder;
    return "";
  }, [level, selectedSchema, selectedFolder]);

  const placeholder = useMemo(() => {
    if (level === LEVEL_ROOT) return "Search tables & models...";
    if (level === LEVEL_SCHEMAS) return "Search schemas...";
    if (level === LEVEL_TABLES) return "Search tables...";
    if (level === LEVEL_MODEL_FOLDERS) return "Search folders...";
    if (level === LEVEL_MODELS) return "Search models...";
    return "Search...";
  }, [level]);

  // Pre-compute selectable index for each item (safe for StrictMode double-render)
  const selectableIndexMap = useMemo(() => {
    const map = new Map();
    let idx = 0;
    items.forEach((item, i) => {
      if (item.type !== "group_header" && item.type !== "loading") {
        map.set(i, idx++);
      }
    });
    return map;
  }, [items]);

  return (
    <div style={getPopupStyle(placement)}>
      {/* Header with back button + breadcrumb */}
      <div style={headerStyle}>
        {level !== LEVEL_ROOT && (
          <span style={backBtnStyle} onClick={goBack}>
            <ArrowLeftOutlined />
          </span>
        )}
        <span style={breadcrumbStyle}>{breadcrumb}</span>
      </div>

      {/* Search input */}
      <div style={searchWrapperStyle}>
        <SearchOutlined
          style={{ color: "var(--circle-color)", fontSize: "12px" }}
        />
        <input
          ref={searchInputRef}
          type="text"
          value={searchText}
          onChange={(e) => onSearchChange(e.target.value)}
          placeholder={placeholder}
          style={searchInputStyle}
        />
      </div>

      {/* Items list */}
      <div ref={listRef} style={listStyle}>
        {items.length === 0 && <div style={emptyStyle}>No results found</div>}
        {items.map((item, idx) => {
          if (item.type === "group_header") {
            return (
              <div key={`gh-${item.label}`} style={groupHeaderStyle}>
                {item.label}
              </div>
            );
          }

          const currentIdx = selectableIndexMap.get(idx);

          return (
            <div
              key={`${item.type}-${item.label}-${item.description || ""}`}
              data-index={currentIdx}
              onClick={() => handleItemClick(item)}
              onMouseEnter={() => setActiveIndex(currentIdx)}
              style={itemStyle(currentIdx === activeIndex)}
            >
              {item.icon || <TableOutlined style={iconStyle} />}
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={labelStyle}>{item.label}</div>
                {item.description && (
                  <div style={descStyle}>{item.description}</div>
                )}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
});

MentionPopup.propTypes = {
  dbData: PropTypes.object,
  modelsData: PropTypes.object,
  seedsData: PropTypes.object,
  searchText: PropTypes.string,
  onSelect: PropTypes.func.isRequired,
  onClose: PropTypes.func.isRequired,
  onSearchChange: PropTypes.func.isRequired,
  placement: PropTypes.oneOf(["top", "bottom"]),
};

MentionPopup.displayName = "MentionPopup";

export { MentionPopup };
