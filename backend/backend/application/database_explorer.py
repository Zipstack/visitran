# UI icons as strings (keep consistent with frontend)

ICON_DATABASE = "DatabaseOutlined"
ICON_SCHEMA = "ClusterOutlined"
ICON_TABLE = "TableOutlined"
ICON_NUMBER = "NumberOutlined"
ICON_TEXT = "FontColorsOutlined"
ICON_TIME = "ClockCircleOutlined"
ICON_DEFAULT = "StringOutlined"


class DatabaseExplorerTree:
    @staticmethod
    def _column_icon_from_dtype(ui_db_type: str) -> str:
        t = (ui_db_type or "").lower()
        if any(k in t for k in ["int", "decimal", "numeric", "float", "double"]):
            return ICON_NUMBER
        if any(k in t for k in ["time", "date", "timestamp"]):
            return ICON_TIME
        if any(k in t for k in ["char", "string", "text"]):
            return ICON_TEXT
        return ICON_DEFAULT

    @classmethod
    def build_ui_tree(cls, project_name: str, default_schema: str, db_meta_json: dict) -> dict:
        """
        Build the UI explorer tree from metadata JSON.
        """
        project_title = f"{project_name}/Database"
        db_meta_json = db_meta_json or {}
        tables_map = db_meta_json.get("tables", {}) or {}
        declared_schemas = db_meta_json.get("schemas") or []

        # Group tables by schema from tables_map
        tables_by_schema = {}
        for _, tdata in tables_map.items():
            schema = tdata.get("schema_name") or ""
            tables_by_schema.setdefault(schema, []).append(tdata)

        inferred_schemas = set(tables_by_schema.keys())
        all_schemas = set(declared_schemas) | inferred_schemas
        sorted_schemas = sorted(all_schemas, key=lambda s: (s is None, str(s).lower()))

        ui_children = []
        for schema_name in sorted_schemas:
            schema_name = schema_name or ""  # ensure string
            schema_tables = tables_by_schema.get(schema_name, [])
            schema_tables_sorted = sorted(schema_tables, key=lambda t: str(t.get("name", "")).lower())

            table_children = []
            for t in schema_tables_sorted:
                table_name = t.get("name", "")
                cols = t.get("columns", []) or []

                cols_sorted = sorted(cols, key=lambda c: str(c.get("name", "")).lower())

                column_children = []
                for c in cols_sorted:
                    raw_dtype = str(c.get("dtype", ""))
                    icon = cls._column_icon_from_dtype(raw_dtype)
                    column_children.append(
                        {
                            "key": f"{project_title}/{schema_name}/{table_name}/{c.get('name','')}",
                            "title": c.get("name", ""),
                            "type": "Column",
                            "is_folder": False,
                            "db_type": raw_dtype,
                            "icon": icon,
                        }
                    )

                table_children.append(
                    {
                        "key": f"{project_title}/{schema_name}/{table_name}",
                        "title": table_name,
                        "type": "Table",
                        "is_folder": False,
                        "icon": ICON_TABLE,
                        "children": column_children,
                    }
                )

            ui_children.append(
                {
                    "key": f"{project_title}/{schema_name}",
                    "title": schema_name,
                    "type": "Schema",
                    "is_folder": True,
                    "icon": ICON_SCHEMA,
                    "children": table_children,
                }
            )

        return {
            "key": project_title,
            "title": "Database",
            "type": "ROOT_DB",
            "is_folder": True,
            "icon": ICON_DATABASE,
            "default_project_schema": default_schema,
            "children": ui_children,
        }
