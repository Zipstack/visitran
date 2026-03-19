/**
 * Transform explorer tree data to schema summary format for table selection.
 * Reuses data already fetched by getDbExplorer() instead of making a separate API call.
 *
 * @param {Object} dbData - Explorer tree data from getDbExplorer()
 *   Structure: { title: "project", children: [{ title: "schema", children: [{ title: "table", children: columns }] }] }
 * @return {Object} - { schemaSummary, totalTableCount }
 *   schemaSummary format: [{ schema_name, table_count, tables: [{ table_name, column_count }] }]
 */
export function buildSchemaSummaryFromExplorer(dbData) {
  const schemaSummary = [];
  let totalTableCount = 0;

  // dbData.children contains schemas (first level under project)
  const schemas = dbData?.children || [];

  for (const schema of schemas) {
    const tables = [];

    // schema.children contains tables
    for (const table of schema.children || []) {
      tables.push({
        table_name: table.title,
        column_count: table.children?.length || 0,
      });
    }

    totalTableCount += tables.length;

    schemaSummary.push({
      schema_name: schema.title,
      table_count: tables.length,
      tables,
    });
  }

  return { schemaSummary, totalTableCount };
}
