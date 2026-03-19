import { explorerService } from "../explorer-service";
import { useProjectStore } from "../../../store/project-store";

// Schema color palette based on specified solid colors
const schemaColors = {
  // Primary solid colors as specified
  primary: [
    "#40ddff", // Blue
    "#ff4b6e", // Red
    "#ffb240", // Yellow/Gold
    "#35bee6", // Light Blue
    // Additional shades for more schemas
    "#3aa8cc", // Darker Blue
    "#e6435a", // Darker Red
    "#e69e35", // Darker Yellow
    "#2ca9d0", // Medium Blue
  ],
  // Solid colors for specific schemas
  solidColors: {
    dev: "#00A6ED",
    prod: "#ff4b6e",
    raw: "#ffb240",
    stg: "#35bee6",
    default: "#00A6ED",
  },
};

export const useDbSchemaService = () => {
  const expService = explorerService();
  const { projectId } = useProjectStore();

  // /**
  //  * Fetches database schema information from the explorer
  //  * @returns {Promise} Promise that resolves to the database schema
  //  */
  const fetchDbSchema = async () => {
    try {
      const response = await expService.getDbExplorer(projectId);
      return response.data;
    } catch (error) {
      console.error("Error fetching database schema:", error);
      throw error;
    }
  };

  // /**
  //  * Detects potential foreign key relationships using multiple strategies
  //  * @param {Array} tables - Array of tables with their columns
  //  * @returns {Array} Array of potential foreign key relationships
  //  */
  const detectForeignKeyRelationships = (tables) => {
    const relationships = [];
    const primaryKeys = {};
    const tableColumnMap = {};

    // First pass: identify primary key candidates and build table column maps
    tables.forEach((table) => {
      const tableName = table.title;
      const tableColumns = table.children.map((col) => ({
        name: col.title,
        type: col.db_type || "unknown",
      }));

      // Store all columns for this table
      tableColumnMap[tableName] = tableColumns;

      // Identify primary keys
      const idColumns = table.children.filter(
        (col) =>
          col.title === "id" ||
          col.title === "uuid" ||
          col.title === "key" ||
          col.title === `${tableName}_id` ||
          col.title === `${tableName}_key` ||
          col.title === `${tableName}_uuid`
      );

      if (idColumns.length > 0) {
        primaryKeys[tableName] = idColumns.map((col) => col.title);
      }
    });

    // Strategy 1: Naming convention based detection
    tables.forEach((sourceTable) => {
      const sourceTableName = sourceTable.title;

      sourceTable.children.forEach((column) => {
        // Look for columns ending with _id, _key, _uuid that aren't the table's own ID
        if (
          (column.title.endsWith("_id") ||
            column.title.endsWith("_key") ||
            column.title.endsWith("_uuid")) &&
          column.title !== `${sourceTableName}_id` &&
          column.title !== "id"
        ) {
          // Extract the referenced table name from the column name
          let possibleTableName;
          if (column.title.endsWith("_id")) {
            possibleTableName = column.title.replace("_id", "");
          } else if (column.title.endsWith("_key")) {
            possibleTableName = column.title.replace("_key", "");
          } else if (column.title.endsWith("_uuid")) {
            possibleTableName = column.title.replace("_uuid", "");
          }

          // Check for singular/plural variations
          const singularName = possibleTableName.endsWith("s")
            ? possibleTableName.slice(0, -1)
            : possibleTableName;
          const pluralName = possibleTableName.endsWith("s")
            ? possibleTableName
            : `${possibleTableName}s`;

          // Check if we have a table with this name or its variations
          tables.forEach((targetTable) => {
            const targetTableName = targetTable.title;

            if (
              targetTableName === possibleTableName ||
              targetTableName === singularName ||
              targetTableName === pluralName ||
              targetTableName.includes(possibleTableName)
            ) {
              // Find the primary key in the target table
              const targetColumn = targetTable.children.find(
                (col) =>
                  col.title === "id" ||
                  col.title === "uuid" ||
                  col.title === "key" ||
                  col.title === column.title
              );

              if (targetColumn) {
                relationships.push({
                  sourceTable: sourceTableName,
                  sourceColumn: column.title,
                  targetTable: targetTableName,
                  targetColumn: targetColumn.title,
                  confidence: "high",
                  detectionMethod: "naming_convention",
                });
              }
            }
          });
        }
      });
    });

    // Strategy 2: Type matching for columns with same name
    tables.forEach((sourceTable) => {
      const sourceTableName = sourceTable.title;

      sourceTable.children.forEach((sourceColumn) => {
        // Skip primary keys of the source table
        if (
          sourceColumn.title === "id" ||
          sourceColumn.title === `${sourceTableName}_id`
        ) {
          return;
        }

        // Look for columns with the same name and compatible types in other tables
        tables.forEach((targetTable) => {
          // Skip self-references
          if (targetTable.title === sourceTableName) {
            return;
          }

          const targetTableName = targetTable.title;

          // Check if target table has a column with the same name
          const targetColumn = targetTable.children.find(
            (col) =>
              col.title === sourceColumn.title &&
              (col.db_type === sourceColumn.db_type ||
                (col.db_type &&
                  sourceColumn.db_type &&
                  ((col.db_type.includes("int") &&
                    sourceColumn.db_type.includes("int")) ||
                    (col.db_type.includes("char") &&
                      sourceColumn.db_type.includes("char")))))
          );

          if (
            targetColumn &&
            !relationships.some(
              (r) =>
                r.sourceTable === sourceTableName &&
                r.sourceColumn === sourceColumn.title &&
                r.targetTable === targetTableName &&
                r.targetColumn === targetColumn.title
            )
          ) {
            relationships.push({
              sourceTable: sourceTableName,
              sourceColumn: sourceColumn.title,
              targetTable: targetTableName,
              targetColumn: targetColumn.title,
              confidence: "medium",
              detectionMethod: "type_matching",
            });
          }
        });
      });
    });

    // Deduplicate relationships
    const uniqueRelationships = [];
    const relationshipMap = new Map();

    relationships.forEach((rel) => {
      const key = `${rel.sourceTable}:${rel.sourceColumn}->${rel.targetTable}:${rel.targetColumn}`;
      if (!relationshipMap.has(key)) {
        relationshipMap.set(key, rel);
        uniqueRelationships.push(rel);
      } else {
        // If we have multiple detection methods, prioritize the one with higher confidence
        const existingRel = relationshipMap.get(key);
        if (rel.confidence === "high" && existingRel.confidence !== "high") {
          relationshipMap.set(key, rel);
        }
      }
    });

    return uniqueRelationships;
  };

  // /**
  //  * Transforms database explorer data into ReactFlow nodes and edges
  //  * @param {Object} dbExplorerData - Database explorer data from the API
  //  * @returns {Object} Object containing nodes and edges for ReactFlow
  //  */
  const transformDbExplorerToFlowData = (dbExplorerData) => {
    if (!dbExplorerData || !dbExplorerData.children) {
      return { nodes: [], edges: [] };
    }

    const nodes = [];
    const edges = [];
    // let positionX = 50;
    // let positionY = 50;
    let nodeId = 1;

    // Track tables and their IDs for creating edges
    const tableIdMap = {};
    const allTables = [];
    const schemaMap = {}; // Map to track schemas and their tables

    // Process schemas (which contain tables)
    if (dbExplorerData.type === "ROOT_DB" && dbExplorerData.children) {
      // First pass: collect all schemas and their tables
      dbExplorerData.children.forEach((schema) => {
        if (schema.type === "Schema" && schema.children) {
          const schemaName = schema.title;
          if (!schemaMap[schemaName]) {
            schemaMap[schemaName] = {
              tables: [],
              // Get solid color for this schema or use default
              solidColor:
                schemaColors.solidColors[schemaName] ||
                schemaColors.primary[
                  Object.keys(schemaMap).length % schemaColors.primary.length
                ],
              // Use the same color for border
              color:
                schemaColors.solidColors[schemaName] ||
                schemaColors.primary[
                  Object.keys(schemaMap).length % schemaColors.primary.length
                ],
            };
          }

          // Add tables to this schema
          schema.children.forEach((table) => {
            if (table.type === "Table" && table.children) {
              schemaMap[schemaName].tables.push(table);
              allTables.push(table);
            }
          });
        }
      });

      // Second pass: create nodes for each table with schema-specific styling
      // Group tables by schema and position them accordingly
      let currentX = 100; // Starting X position
      const schemaGap = 100; // Gap between different schemas
      const columnWidth = 300; // Width of each column
      const rowHeight = 320; // Height of each row
      const maxTablesPerColumn = 3; // Maximum tables per column

      // Process each schema
      Object.entries(schemaMap).forEach(([schemaName, schemaInfo]) => {
        // Sort tables alphabetically within each schema
        const sortedTables = [...schemaInfo.tables].sort((a, b) =>
          a.title.localeCompare(b.title)
        );

        // Calculate how many columns we'll need for this schema
        const numTables = sortedTables.length;
        const numColumns = Math.ceil(numTables / maxTablesPerColumn);

        // Track the schema's starting position for later
        const schemaStartX = currentX;

        // Process each table in this schema
        sortedTables.forEach((table, tableIndex) => {
          // Calculate position based on index
          const columnIndex = Math.floor(tableIndex / maxTablesPerColumn);
          const rowIndex = tableIndex % maxTablesPerColumn;

          const posXtemp = columnIndex * columnWidth;
          const posX = schemaStartX + posXtemp;
          const posYtemp = rowIndex * rowHeight;
          const posY = 100 + posYtemp;
          // Store table ID for reference
          const tableId = `${nodeId}`;
          const tableName = table.title;
          tableIdMap[tableName] = tableId;

          // Extract columns and their types
          const schemaFields = [];

          table.children.forEach((column) => {
            if (column.type === "Column") {
              const columnName = column.title;
              const columnType = column.db_type || "unknown";

              schemaFields.push({
                title: columnName,
                type: columnType,
              });
            }
          });

          // Create a node for this table with schema-specific styling
          nodes.push({
            id: tableId,
            position: {
              x: posX,
              y: posY,
            },
            type: "databaseSchema",
            data: {
              label: `${schemaName}.${table.title}`,
              title: `${schemaName}.${table.title}`,
              schema: schemaFields,
              schemaName: schemaName,
              solidColor: schemaInfo.solidColor,
              color: schemaInfo.color,
            },
          });

          // Increment node ID for next table
          nodeId++;
        });

        // Move X position for the next schema
        // Calculate width of this schema and add the gap
        const schemaWidth = numColumns * columnWidth;
        currentX = schemaStartX + schemaWidth + schemaGap;
      });
    }

    // Relationship edges are disabled
    // No edges will be created

    return { nodes, edges };
  };

  return {
    fetchDbSchema,
    transformDbExplorerToFlowData,
    detectForeignKeyRelationships,
  };
};
