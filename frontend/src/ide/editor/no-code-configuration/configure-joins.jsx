import { Button, Space, Select, Divider, Input, Tooltip } from "antd";
import { DeleteOutlined, PlusCircleOutlined } from "@ant-design/icons";
import PropTypes from "prop-types";
import { useEffect, useMemo, useState } from "react";
import { useImmer } from "use-immer";
import { AnimatePresence, motion } from "framer-motion";

import { ReactComponent as InnerJoin } from "./join-icons/inner-join.svg";
import { ReactComponent as LeftJoin } from "./join-icons/left-join.svg";
import { ReactComponent as FullJoin } from "./join-icons/full-join.svg";
import { ReactComponent as RightJoin } from "./join-icons/right-join.svg";
import { ReactComponent as CrossJoin } from "./join-icons/cross-join.svg";
import { useProjectStore } from "../../../store/project-store";
import { orgStore } from "../../../store/org-store";
import {
  generateKey,
  getFilterCondition,
  getTableFullname,
} from "../../../common/helpers.js";
import { useAxiosPrivate } from "../../../service/axios-service";
import { Filter } from "../no-code-ui/filter/filter.jsx";
import { useNotificationService } from "../../../service/notification-service.js";

function ConfigureJoins({
  joins,
  setJoins,
  sourceTable,
  columnDetails,
  spec,
  modelName,
  onValidationChange,
}) {
  const axios = useAxiosPrivate();
  const { selectedOrgId } = orgStore();
  const schema = spec.source.schema_name;
  const { dbConfigDetails, projectId } = useProjectStore();
  const isSchemaExists = dbConfigDetails?.is_schema_exists ?? false;
  const { notify } = useNotificationService();

  const [allTables, setAllTables] = useState([]);
  const [joinOptionsLoading, setJoinOptionsLoading] = useState(false);

  const [joinWarning, setJoinWarning] = useState("");
  const [columnsCache, updateColumnsCache] = useImmer({});

  const [joinSourceTableColumnDetails, updateJoinSourceCols] = useImmer([]); // LHS per joinIndex
  const [joinTableColumnDetails, updateJoinRhsCols] = useImmer([]); // RHS per joinIndex

  // Per-join loading flags
  const [loadingByJoin, updateLoadingByJoin] = useImmer({
    lhs: {}, // { [joinIndex]: boolean }
    rhs: {}, // { [joinIndex]: boolean }
  });

  // Local filter state consumed by <Filter/>
  const [joinFilterConditions, setJoinFilterConditions] = useImmer([]);

  // Icons & join type options
  const joinIcons = useMemo(
    () => ({
      Inner: <InnerJoin />,
      Left: <LeftJoin />,
      Full: <FullJoin />,
      Right: <RightJoin />,
      Cross: <CrossJoin />,
    }),
    []
  );
  const joinTypes = ["Inner", "Left", "Full", "Right", "Cross"];

  // Transform source columnDetails to include "joins" namespace (consistent with your previous code)
  const transformedColumnDetails = useMemo(() => {
    const clone = structuredClone(columnDetails);
    clone.columns.joins = clone.columns?.joins?.map((el) => {
      return `${sourceTable}.${el}`;
    });
    return clone;
  }, [columnDetails, sourceTable]);

  const fqTable = (schemaName, tableName) =>
    `${isSchemaExists && schemaName ? schemaName + "." : ""}${tableName}`;

  const parseTable = (table) => {
    const [tableName, schemaName] = table?.split(".").reverse() || [];
    return { schemaName, tableName };
  };

  const getJoinDestination = (data) => {
    // converts ["schema","table"] or ["table"] into {schema_name, table_name}
    const [tableName, schemaName] = data?.slice().reverse() || [];
    return {
      schema_name: schemaName ?? null,
      table_name: tableName,
    };
  };

  const setPerJoinLoading = (side, idx, value) => {
    updateLoadingByJoin((draft) => {
      draft[side][idx] = value;
    });
  };

  // Fetch columns for a fully qualified table and store in cache.
  // Returns cached or freshly fetched {columns, dataTypes}
  const fetchColumnsForTable = async (schemaName, tableName) => {
    const key = fqTable(schemaName, tableName);
    if (columnsCache[key]) return columnsCache[key];
    try {
      const requestOptions = {
        method: "GET",
        url: `/api/v1/visitran/${
          selectedOrgId || "default_org"
        }/project/${projectId}/schema/${
          isSchemaExists ? schemaName || schema : "default"
        }/table/${tableName}/columns`,
      };
      const res = await axios(requestOptions);
      const obj = res.data?.column_description;
      const columns = res.data.column_names || [];
      const dataTypes = columns.map((c) => obj?.[c]?.data_type ?? null);

      const payload = {
        columns: columns.map((c) => fqTable(schemaName, tableName) + "." + c),
        dataTypes,
      };

      // Store in cache immutably
      updateColumnsCache((draft) => {
        draft[key] = payload;
      });

      // If another newer request was made, we keep cache anyway (it's idempotent and useful).
      return payload;
    } catch (error) {
      notify({ error });
      updateColumnsCache((draft) => {
        draft[key] = { columns: [], dataTypes: [] };
      });
      return { columns: [], dataTypes: [] };
    }
  };

  // Load LHS/RHS columns for a given join row (non-blocking)
  const ensureJoinSidesReady = async (joinIndex) => {
    const join = joins[joinIndex];
    if (!join || !join?.criteria?.[0]?.condition) return;

    const { lhs, rhs } = join?.criteria?.[0]?.condition || {};

    // LHS (source side)
    if (lhs?.column?.table_name) {
      const lSchema = lhs.column.schema_name || schema || null;
      const lTable = lhs.column.table_name;
      setPerJoinLoading("lhs", joinIndex, true);
      const lhsMeta = await fetchColumnsForTable(lSchema, lTable);
      updateJoinSourceCols((draft) => {
        draft[joinIndex] = {
          columns: lhsMeta.columns,
          dataTypes: lhsMeta.dataTypes,
        };
      });
      setPerJoinLoading("lhs", joinIndex, false);
    }

    // RHS (joined table)
    if (rhs?.column?.table_name) {
      const rSchema = rhs.column.schema_name || null;
      const rTable = rhs.column.table_name;
      setPerJoinLoading("rhs", joinIndex, true);
      const rhsMeta = await fetchColumnsForTable(rSchema, rTable);
      updateJoinRhsCols((draft) => {
        draft[joinIndex] = {
          columns: rhsMeta.columns,
          dataTypes: rhsMeta.dataTypes,
        };
      });

      // If RHS was a placeholder, pick the first available column
      const needsPlaceholderFix =
        rhs?.column?.column_name === "placeholder" &&
        rhsMeta.columns.length > 0;

      if (needsPlaceholderFix) {
        const [firstColFQ] = rhsMeta.columns;
        const rhsType = rhsMeta.dataTypes[0];
        const [s, t, ...rest] = firstColFQ.split(".");
        const schemaName = isSchemaExists ? s : null;
        const tableName = isSchemaExists ? t : s;
        const columnName = rest.length ? rest.join(".") : t;

        const newRhs = getFilterCondition(
          [schemaName, tableName, columnName],
          "COLUMN",
          rhsType
        );

        // Update joinFilterConditions & joins in one pass
        setJoinFilterConditions((draft) => {
          if (!draft[joinIndex]) draft[joinIndex] = [];
          if (draft[joinIndex][0]?.condition?.rhs) {
            draft[joinIndex][0].condition.rhs = newRhs;
          }
        });

        setJoins((draft) => {
          if (draft[joinIndex]?.criteria?.[0]?.condition?.rhs) {
            draft[joinIndex].criteria[0].condition.rhs = newRhs;
          }
        });
      }

      setPerJoinLoading("rhs", joinIndex, false);
    }
  };

  // --------- Tables list (RHS join table dropdown) ----------

  const getAllTables = async () => {
    setJoinOptionsLoading(true);
    try {
      const requestOptions = {
        method: "GET",
        url: `/api/v1/visitran/${
          selectedOrgId || "default_org"
        }/project/${projectId}/schemas/tables?model=${modelName}`,
      };
      const res = await axios(requestOptions);
      const tables = res?.data?.table_names || [];

      // Exclude the model destination table and source table
      const sourceTableFQ = getTableFullname(
        spec?.source?.schema_name,
        spec?.source?.table_name
      );
      const destTableFQ = getTableFullname(
        spec?.model?.schema_name,
        spec?.model?.table_name
      );
      const filtered = tables.filter(
        (table) => table !== destTableFQ && table !== sourceTableFQ
      );
      setAllTables(filtered);
    } catch (error) {
      console.error(error);
      notify({ error });
    } finally {
      setJoinOptionsLoading(false);
    }
  };

  useEffect(() => {
    getAllTables();
  }, []);

  const usedTablesColumnsOnIndex = (index) => {
    const collectallusedtable = new Set();
    if (index >= 0) {
      joins.forEach((el, idx) => {
        if (idx < index) {
          const lhsschema =
            el?.criteria?.[0]?.condition?.lhs?.column?.schema_name;
          const lhstable =
            el?.criteria?.[0]?.condition?.lhs?.column?.table_name;
          const rhsschema =
            el?.criteria?.[0]?.condition?.rhs?.column?.schema_name;
          const rhstable =
            el?.criteria?.[0]?.condition?.rhs?.column?.table_name;
          collectallusedtable.add(
            `${lhsschema ? lhsschema + "." : ""}${lhstable}`
          );
          collectallusedtable.add(
            `${rhsschema ? rhsschema + "." : ""}${rhstable}`
          );
        }
      });
    }
    return Array.from(collectallusedtable);
  };

  const lhsOptions = (index) => {
    // All previously used tables up to this row are valid LHS choices
    const arr = usedTablesColumnsOnIndex(index);
    return arr.map((el) => ({ value: el, label: el }));
  };

  function rhsTableOptions(index, addNew = false) {
    const usedTables = usedTablesColumnsOnIndex(index);
    const options = [];
    for (const el of allTables) {
      if (!usedTables.includes(el)) {
        options.push({ label: el, value: el });
        if (addNew) break;
      }
    }
    return options;
  }

  const joinTypeOptions = useMemo(
    () =>
      joinTypes.map((joinType) => ({
        label: (
          <div className="join-select-items">
            {joinIcons[joinType]}
            {joinType}
          </div>
        ),
        value: joinType,
      })),
    [joinIcons]
  );

  // ---------- Initial criteria builder ----------

  const buildInitialCriteria = (rhsSchemaName, rhsTableName) => {
    const sourceCol = Object.keys(columnDetails.allColumnsDetails)[0];
    const sourceDataType =
      columnDetails?.allColumnsDetails?.[sourceCol]?.data_type;

    return [
      {
        condition: {
          lhs: getFilterCondition(
            [schema, sourceTable, sourceCol],
            "COLUMN",
            sourceDataType
          ),
          operator: "EQ",
          rhs: getFilterCondition(
            [rhsSchemaName ?? null, rhsTableName, "placeholder"],
            "COLUMN",
            "VARCHAR"
          ),
        },
      },
    ];
  };

  const updatJoinfunc = (joinIndex, side, value) => {
    setJoinFilterConditions((draft) => {
      if (!draft[joinIndex]) draft[joinIndex] = [];
      // Ensure 1st condition exists
      if (!draft[joinIndex][0]) {
        draft[joinIndex][0] = {
          id: generateKey(),
          condition: { lhs: {}, rhs: {}, operator: "EQ" },
        };
      }
      draft[joinIndex][0].condition[side] = value;
    });
  };

  const updatejoins = (joinIndex, side, value, schemaName, tableName) => {
    setJoins((draft) => {
      // if RHS table changed, update joined_table
      if (side === "rhs" && schemaName && tableName) {
        draft[joinIndex].joined_table = getJoinDestination(
          isSchemaExists ? [schemaName, tableName] : [tableName]
        );
      }
      draft[joinIndex].criteria.splice(1);
      draft[joinIndex].criteria[0].condition[side] = value;
    });
  };

  const handleRemoveJoin = (joinIndex) => {
    setJoins((draft) => {
      draft.splice(joinIndex, 1);
    });
    updateJoinRhsCols((draft) => {
      draft.splice(joinIndex, 1);
    });
    updateJoinSourceCols((draft) => {
      draft.splice(joinIndex, 1);
    });
    setJoinFilterConditions((draft) => {
      draft.splice(joinIndex, 1);
    });
    updateLoadingByJoin((draft) => {
      delete draft.lhs[joinIndex];
      delete draft.rhs[joinIndex];
    });
  };

  const handleOptionChange = (value, key, joinIndex) => {
    if (key === "destination_table") {
      // RHS table (schema.table or table)
      const { schemaName, tableName } = parseTable(value);
      // Update RHS columns immediately (non-blocking)
      (async () => {
        setPerJoinLoading("rhs", joinIndex, true);
        const rhsMeta = await fetchColumnsForTable(schemaName, tableName);
        updateJoinRhsCols((draft) => {
          draft[joinIndex] = {
            columns: rhsMeta.columns,
            dataTypes: rhsMeta.dataTypes,
          };
        });

        // If RHS first criterion is placeholder, auto-pick first column
        if (rhsMeta.columns.length) {
          const [firstColFQ] = rhsMeta.columns;
          const rhsType = rhsMeta.dataTypes[0];
          const parts = firstColFQ.split(".");
          const sName = isSchemaExists ? parts[0] : null;
          const tName = isSchemaExists ? parts[1] : parts[0];
          const cName = parts.slice(isSchemaExists ? 2 : 1).join(".");

          const rhs = getFilterCondition(
            [sName, tName, cName],
            "COLUMN",
            rhsType
          );
          updatJoinfunc(joinIndex, "rhs", rhs);
          updatejoins(joinIndex, "rhs", rhs, schemaName, tableName);
        }
        setPerJoinLoading("rhs", joinIndex, false);
      })();
    } else if (key === "source_table") {
      // LHS source table change
      const { schemaName, tableName } = parseTable(value);
      (async () => {
        setPerJoinLoading("lhs", joinIndex, true);
        const lhsMeta = await fetchColumnsForTable(schemaName, tableName);
        updateJoinSourceCols((draft) => {
          draft[joinIndex] = {
            columns: lhsMeta.columns,
            dataTypes: lhsMeta.dataTypes,
          };
        });

        if (lhsMeta.columns.length) {
          const [firstColFQ] = lhsMeta.columns;
          const lhsType = lhsMeta.dataTypes[0];
          const parts = firstColFQ.split(".");
          const sName = isSchemaExists ? parts[0] : null;
          const tName = isSchemaExists ? parts[1] : parts[0];
          const cName = parts.slice(isSchemaExists ? 2 : 1).join(".");

          const lhs = getFilterCondition(
            [sName, tName, cName],
            "COLUMN",
            lhsType
          );
          updatJoinfunc(joinIndex, "lhs", lhs);
          updatejoins(joinIndex, "lhs", lhs);
        }
        setPerJoinLoading("lhs", joinIndex, false);
      })();
    } else if (["source", "joined_table"].includes(key)) {
      // Direct column swap (rare path)
      setJoins((draft) => {
        draft[joinIndex]["criteria"][0]["condition"][
          key === "source" ? "lhs" : "rhs"
        ] = getFilterCondition(value.split("."));
      });
    } else if (key === "type") {
      setJoins((draft) => {
        draft[joinIndex].type = value;
        // For Cross join, ensure criteria has at least one dummy entry to prevent crashes
        if (value === "Cross" && draft[joinIndex].criteria.length === 0) {
          const rhsTable = draft[joinIndex].joined_table;
          draft[joinIndex].criteria = buildInitialCriteria(
            rhsTable.schema_name,
            rhsTable.table_name
          );
        }
        // For Full join, keep only the first criteria and reset operator to EQ
        if (value === "Full" && draft[joinIndex].criteria.length > 0) {
          draft[joinIndex].criteria = [draft[joinIndex].criteria[0]];
          if (draft[joinIndex]?.criteria?.[0]?.condition) {
            draft[joinIndex].criteria[0].condition.operator = "EQ";
          }
        }
      });
    } else if (key === "alias_name") {
      // Update joined_table alias_name for self-join support
      setJoins((draft) => {
        if (draft[joinIndex].joined_table) {
          draft[joinIndex].joined_table.alias_name = value || null;
        }
      });
    }
  };

  // Helper to check if a join is a self-join (source table == joined table)
  const isSelfJoin = (joinIndex) => {
    const join = joins[joinIndex];
    if (!join) return false;
    const sourceTableFQ = getTableFullname(
      spec.source.schema_name,
      spec.source.table_name
    );
    const joinedTable = join.joined_table;
    if (!joinedTable) return false;
    const joinedTableFQ = getTableFullname(
      joinedTable.schema_name,
      joinedTable.table_name
    );
    return sourceTableFQ === joinedTableFQ;
  };

  const addNewJoin = () => {
    const rhsTableOpts = rhsTableOptions(joins.length, true);
    if (!rhsTableOpts || rhsTableOpts.length === 0) {
      setJoinWarning("Not enough tables available to perform a join");
      setTimeout(() => setJoinWarning(""), 5000);
      return;
    }

    setJoinWarning("");

    const rhsTableLabel = rhsTableOpts[0].label;
    const { schemaName: rhsSchemaName, tableName: rhsTableName } =
      parseTable(rhsTableLabel);

    const criteria = buildInitialCriteria(rhsSchemaName, rhsTableName);

    // Add join
    setJoins((draft) => {
      draft.push({
        id: generateKey(),
        joined_table: getJoinDestination(
          isSchemaExists ? [rhsSchemaName, rhsTableName] : [rhsTableName]
        ),
        type: joinTypes[0], // default Inner
        criteria,
      });
    });

    // Prime LHS columns from transformedColumnDetails (source side)
    const cols = transformedColumnDetails?.columns?.joins || [];
    updateJoinSourceCols((draft) => {
      draft.push({
        columns: cols.map((el) => `${isSchemaExists ? schema + "." : ""}${el}`),
        dataTypes: transformedColumnDetails.dataTypes,
      });
    });

    // Trigger background loads for both sides (non-blocking)
    const joinIndex = joins.length;
    setTimeout(() => {
      ensureJoinSidesReady(joinIndex);
    }, 0);
  };

  // On mount or when joins change, ensure each row has LHS/RHS metadata (non-blocking).
  useEffect(() => {
    joins.forEach((_, idx) => {
      ensureJoinSidesReady(idx);
    });
  }, [joins.length]);

  // Validate self-joins have aliases and aliases are unique
  useEffect(() => {
    const sourceTableFQ = getTableFullname(
      spec.source.schema_name,
      spec.source.table_name
    );

    // Check for self-join without alias
    const selfJoinWithoutAlias = joins.some((join) => {
      if (!join?.joined_table) return false;
      const joinedTableFQ = getTableFullname(
        join.joined_table.schema_name,
        join.joined_table.table_name
      );
      return sourceTableFQ === joinedTableFQ && !join.joined_table.alias_name;
    });

    // Check for duplicate aliases
    const aliasNames = joins
      .map((join) => join?.joined_table?.alias_name)
      .filter((alias) => alias); // Filter out empty/null aliases
    const hasDuplicateAlias = aliasNames.length !== new Set(aliasNames).size;

    if (selfJoinWithoutAlias) {
      setJoinWarning(
        "Self-join detected: Please provide an alias for the joined table"
      );
    } else if (hasDuplicateAlias) {
      setJoinWarning(
        "Duplicate alias detected: Each join must have a unique alias name"
      );
    } else if (
      joinWarning ===
        "Self-join detected: Please provide an alias for the joined table" ||
      joinWarning ===
        "Duplicate alias detected: Each join must have a unique alias name"
    ) {
      setJoinWarning("");
    }
  }, [joins, spec.source.schema_name, spec.source.table_name]);

  // Notify parent of validation state changes
  useEffect(() => {
    onValidationChange?.(!!joinWarning);
  }, [joinWarning, onValidationChange]);

  return (
    <>
      <Space direction="vertical" className="joins">
        {joins.map(({ id, type: joinType, criteria }, joinIndex) => {
          const rhsSch = criteria?.[0]?.condition?.rhs?.column?.schema_name;
          // For LHS, fall back to model source schema/table when null (visitran-ai sets null for computed columns)
          const lhsTableName =
            criteria?.[0]?.condition?.lhs?.column?.table_name ||
            spec.source.table_name;
          const lhsSch =
            criteria?.[0]?.condition?.lhs?.column?.schema_name ||
            spec.source.schema_name;

          const rhsLoading = !!loadingByJoin.rhs[joinIndex];
          const lhsLoading = !!loadingByJoin.lhs[joinIndex];

          return (
            <div key={id}>
              <Space direction="vertical">
                <Space className="mb-10">
                  <Space direction="vertical">
                    Source Table
                    <Select
                      value={`${lhsSch ? lhsSch + "." : ""}${
                        lhsTableName ?? ""
                      }`}
                      onChange={(value) =>
                        handleOptionChange(value, "source_table", joinIndex)
                      }
                      options={lhsOptions(joinIndex)}
                      className="width-200"
                      showSearch
                      disabled={joinIndex === 0}
                      loading={lhsLoading}
                      placeholder={
                        lhsLoading ? "Loading columns..." : "Select source"
                      }
                      notFoundContent={lhsLoading ? "Loading..." : null}
                    />
                  </Space>
                  <Space direction="vertical">
                    Join Type
                    <Select
                      className="width-100px"
                      value={joinType}
                      onChange={(value) =>
                        handleOptionChange(value, "type", joinIndex)
                      }
                      options={joinTypeOptions}
                      optionLabelProp="value"
                    />
                  </Space>
                  <Space direction="vertical">
                    Join Table
                    <Select
                      value={`${rhsSch ? rhsSch + "." : ""}${
                        criteria?.[0]?.condition?.rhs?.column?.table_name ?? ""
                      }`}
                      onChange={(value) =>
                        handleOptionChange(
                          value,
                          "destination_table",
                          joinIndex
                        )
                      }
                      options={rhsTableOptions(joinIndex)}
                      className="width-200"
                      showSearch
                      loading={joinOptionsLoading || rhsLoading}
                      placeholder={
                        joinOptionsLoading
                          ? "Loading tables..."
                          : rhsLoading
                          ? "Loading columns..."
                          : "Select join table"
                      }
                      notFoundContent={joinOptionsLoading ? "Loading..." : null}
                    />
                  </Space>
                  <Space direction="vertical">
                    <Tooltip
                      title={
                        isSelfJoin(joinIndex)
                          ? "Required: Unique alias to distinguish this table from the source"
                          : "Optional: Use an alias to reference this table"
                      }
                    >
                      Table Alias
                    </Tooltip>
                    <Input
                      value={joins[joinIndex]?.joined_table?.alias_name ?? ""}
                      onChange={(e) =>
                        handleOptionChange(
                          e.target.value,
                          "alias_name",
                          joinIndex
                        )
                      }
                      placeholder={
                        isSelfJoin(joinIndex) ? "e.g., manager" : "Optional"
                      }
                      className="width-150"
                      status={
                        isSelfJoin(joinIndex) &&
                        !joins[joinIndex]?.joined_table?.alias_name
                          ? "error"
                          : ""
                      }
                    />
                  </Space>
                </Space>

                {joinType !== "Cross" && (
                  <Filter
                    filterConditions={joinFilterConditions[joinIndex] ?? []}
                    setFilterConditions={setJoinFilterConditions}
                    columnDetails={transformedColumnDetails}
                    type="join"
                    joinType={joinType}
                    join={joins}
                    setJoins={setJoins}
                    joinIndex={joinIndex}
                    joinColumnDetails={joinTableColumnDetails.map(
                      (d) => d || { columns: [], dataTypes: [] }
                    )}
                    joinSourceTableColumnDetails={
                      undefined /* backward compat noop in child */
                    }
                    joinSourceColumns={joinSourceTableColumnDetails.map(
                      (d) => d || { columns: [], dataTypes: [] }
                    )}
                    spec={spec}
                    getCriteria={(column, rhsDataType, sourceObj = {}) => {
                      const colData = (column || "").split(".");
                      let schemaName;
                      let tableName;
                      let columnName;
                      if (isSchemaExists) {
                        [schemaName, tableName, ...columnName] = colData;
                      } else {
                        [tableName, ...columnName] = colData;
                      }
                      columnName = (columnName || []).join(".");
                      const sourceCol = Object.keys(
                        columnDetails.allColumnsDetails
                      )[0];
                      const {
                        schema_name,
                        table_name,
                        column_name,
                        data_type,
                      } = sourceObj || {};
                      return {
                        condition: {
                          lhs: getFilterCondition(
                            [
                              schema_name || schema,
                              table_name || sourceTable,
                              column_name || sourceCol,
                            ],
                            "COLUMN",
                            data_type ||
                              columnDetails?.allColumnsDetails[sourceCol]
                                ?.data_type
                          ),
                          operator: "EQ",
                          rhs: getFilterCondition(
                            [schemaName, tableName, columnName],
                            "COLUMN",
                            rhsDataType
                          ),
                        },
                      };
                    }}
                    rhsLoading={rhsLoading}
                    lhsLoading={lhsLoading}
                  />
                )}
              </Space>

              <div className="flex-end-container mt-16 mb-16">
                <Button
                  icon={<DeleteOutlined />}
                  onClick={() => handleRemoveJoin(joinIndex)}
                  danger
                >
                  Remove this Join
                </Button>
              </div>
              <Divider className="m-0 mb-10" />
            </div>
          );
        })}
      </Space>

      <Button
        icon={<PlusCircleOutlined />}
        className={joins.length ? "mt-16" : ""}
        onClick={addNewJoin}
        disabled={joins.length >= 5 || joinOptionsLoading}
        loading={joinOptionsLoading}
      >
        Add {joins.length ? "another" : "a"} table to join
      </Button>
      <AnimatePresence>
        {joinWarning && (
          <motion.div
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: "auto" }}
            exit={{ opacity: 0, height: 0 }}
            transition={{ duration: 0.3 }}
            style={{ color: "#d93025", fontSize: 14, marginTop: 8 }}
          >
            {joinWarning}
          </motion.div>
        )}
      </AnimatePresence>
    </>
  );
}
ConfigureJoins.propTypes = {
  joins: PropTypes.arrayOf(PropTypes.object).isRequired,
  setJoins: PropTypes.func.isRequired,
  sourceTable: PropTypes.string,
  columnDetails: PropTypes.object,
  spec: PropTypes.object,
  modelName: PropTypes.string.isRequired,
  onValidationChange: PropTypes.func,
};

ConfigureJoins.defaultProps = {
  sourceTable: null,
};

export { ConfigureJoins };
