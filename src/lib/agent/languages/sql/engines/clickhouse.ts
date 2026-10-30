/**
 * ClickHouse's server overlay: the dialect-bound members of the SQL server facet, as agent mode composes them on
 * ClickHouse. Server tier, read only through `engineServerDialect` in `languages/engines/server.ts`.
 *
 * The catalog is read from `system.columns` and `system.tables`, which ClickHouse filters to what the user may see,
 * so the least-privilege agent user reads them with no grant of its own; `system.data_skipping_indices`, the one
 * catalog table the index read could add, answers that user 497 without a grant on it (all measured on 26.7.1.1315
 * as the least-privilege user under `readonly = 1`). Profiles and composed capture use these same restricted reads.
 */
import {
  type AgentCatalogRead,
  type AgentCatalogSelector,
  type AgentPlanEstimate,
  equalsClause,
} from "../../../composed-sql-primitives";
import type { AgentEngineServerOverlay, SqlServerDialect } from "../../pack";
import type { AgentProfileComposition } from "../../../table-profile";
import {
  numeric,
  qualified,
  text,
  attach,
  emptyTable,
  type AgentCatalogCapture,
  type TableIndex,
} from "../../../catalog-rows";
import type { AgentStatisticsReading } from "../../../schema-stats";

const CLICKHOUSE_STATISTICS = Object.freeze<AgentStatisticsReading>({
  probe: null,
  build: (rows) =>
    new Map(
      rows.map((row) => [
        qualified(row.table_schema, row.table_name),
        { estimatedRows: numeric(row.estimated_rows), columns: [] },
      ]),
    ),
});

/** Invalid catalog data is refused instead of producing an incomplete schema snapshot. */
function columnsFrom(value: unknown) {
  const columns: unknown = typeof value === "string" ? JSON.parse(value) : value;
  if (!Array.isArray(columns)) throw new Error("ClickHouse catalog columns must be an array");
  return columns.map((column: unknown) => {
    if (!Array.isArray(column) || typeof column[1] !== "string" || typeof column[2] !== "string")
      throw new Error("ClickHouse catalog column is malformed");
    return {
      name: column[1],
      type: column[2],
      nullable: /^(?:LowCardinality\()?Nullable\(/.test(column[2]),
      // A ClickHouse sparse primary key does not enforce uniqueness.
      isPrimary: false,
    };
  });
}

const CLICKHOUSE_CATALOG_CAPTURE = Object.freeze<AgentCatalogCapture>({
  executionOnly: true,
  kinds: Object.freeze(["columns", "indexes"] as const),
  kindWords: Object.freeze({ table: "table", view: "view", materialized_view: "materialized_view" }),
  build: (rows, kindOf) => {
    const tables: TableIndex = new Map();
    for (const row of rows.get("columns") ?? []) {
      const name = qualified(row.table_schema, row.table_name);
      tables.set(name, { ...emptyTable(name), kind: kindOf(row.relkind), columns: columnsFrom(row.columns) });
    }
    for (const row of rows.get("indexes") ?? []) {
      attach(tables, qualified(row.table_schema, row.table_name), (table) => {
        table.indexes.push({ name: text(row.index_name), columns: [text(row.key_expression)], unique: false });
      });
    }
    return tables;
  },
});

const CLICKHOUSE_PROFILE_COMPOSITION: AgentProfileComposition = Object.freeze({
  shapeTests: Object.freeze({
    email: (quoted: string, pattern: string) => `${quoted} LIKE '${pattern}'`,
    digitRun: (quoted: string, length: number) => `match(${quoted}, '[0-9]{${length},}')`,
  }),
  textualType: /^(?:(?:Nullable|LowCardinality)\()*(?:String|FixedString\(\d+\))\)*$/i,
  incomparableType: /\b(?:JSON|Object|Dynamic|Variant|AggregateFunction)\b/i,
});

/**
 * The estimating plan, the one the editor's EXPLAIN asks for (`clickhouse-json.ts`): ClickHouse's EXPLAIN describes a
 * statement without running it, and `indexes = 1` names the primary-key and skipping-index ranges it would select.
 * Measured as the least-privilege user under `readonly = 1`: one row whose `explain` cell is the plan as JSON text.
 */
const CLICKHOUSE_PLAN_ESTIMATE: AgentPlanEstimate = Object.freeze({
  kind: "prefix",
  prefix: "EXPLAIN json = 1, indexes = 1",
});

/** The schemas ClickHouse keeps for itself, excluded from every catalog read below. */
const CLICKHOUSE_SCHEMA_EXCLUSION = "database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')";

/**
 * One row per object keeps wide tables within the result row budget. Each tuple holds position, name, type and key
 * membership in column order. Types retain their Nullable and LowCardinality wrappers.
 */
function composeClickhouseColumns(selector: AgentCatalogSelector): string {
  return (
    "SELECT c.database AS table_schema, c.table AS table_name, " +
    "multiIf(t.engine = 'View', 'view', t.engine = 'MaterializedView', 'materialized_view', 'table') AS relkind, " +
    "toJSONString(arraySort(x -> x.1, groupArray(tuple(c.position, c.name, c.type, c.is_in_primary_key)))) AS columns " +
    "FROM system.columns c INNER JOIN system.tables t ON t.database = c.database AND t.name = c.table " +
    "WHERE c.database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')" +
    equalsClause("c.database", selector.schema, "schema", "clickhouse") +
    equalsClause("c.table", selector.table, "table", "clickhouse") +
    " GROUP BY c.database, c.table, t.engine ORDER BY c.database, c.table"
  );
}

/**
 * The foreign-key inventory, which on ClickHouse is always empty: the engine parses REFERENCES in a column definition
 * and keeps nothing of it (`declaresForeignKeys: false` on the provider). The read is still the projection the other
 * engines answer, with no rows, so a run that asks is told there are none rather than refused.
 */
function composeClickhouseRelations(selector: AgentCatalogSelector): string {
  return (
    "SELECT database AS table_schema, table AS table_name, name AS column_name, " +
    "'' AS referenced_schema, '' AS referenced_table, '' AS referenced_column " +
    `FROM system.columns WHERE 0 AND ${CLICKHOUSE_SCHEMA_EXCLUSION}` +
    equalsClause("database", selector.schema, "schema", "clickhouse") +
    equalsClause("table", selector.table, "table", "clickhouse") +
    " ORDER BY database, table"
  );
}

/**
 * The key inventory: each table's primary key and, where it differs, its sorting key, as the expressions
 * `system.tables` holds. A ClickHouse primary key is a sparse index over the sorting order, not a uniqueness
 * constraint, so neither is unique. The expression is kept whole rather than split into columns, because a key may be
 * any expression (`toDate(ts), id`) and a split at its commas would cut a function's arguments apart.
 */
function composeClickhouseIndexes(selector: AgentCatalogSelector): string {
  const narrowing =
    equalsClause("database", selector.schema, "schema", "clickhouse") +
    equalsClause("name", selector.table, "table", "clickhouse");
  const primary =
    "SELECT database AS table_schema, name AS table_name, 'PRIMARY KEY' AS index_name, false AS is_unique, " +
    "true AS is_primary, primary_key AS key_expression " +
    `FROM system.tables WHERE primary_key != '' AND ${CLICKHOUSE_SCHEMA_EXCLUSION}${narrowing}`;
  const sorting =
    "SELECT database AS table_schema, name AS table_name, 'ORDER BY' AS index_name, false AS is_unique, " +
    "false AS is_primary, sorting_key AS key_expression " +
    `FROM system.tables WHERE sorting_key != '' AND sorting_key != primary_key AND ${CLICKHOUSE_SCHEMA_EXCLUSION}${narrowing}`;
  return `${primary} UNION ALL ${sorting} ORDER BY 1, 2, 3`;
}

/**
 * The statistics inventory: one row per table with the row count `system.tables` keeps, which a MergeTree table
 * maintains from its parts and a view or a table of another engine leaves NULL. Nothing here scans a table.
 */
function composeClickhouseStatistics(selector: AgentCatalogSelector): string {
  return (
    "SELECT database AS table_schema, name AS table_name, total_rows AS estimated_rows " +
    `FROM system.tables WHERE ${CLICKHOUSE_SCHEMA_EXCLUSION}` +
    equalsClause("database", selector.schema, "schema", "clickhouse") +
    equalsClause("name", selector.table, "table", "clickhouse") +
    " ORDER BY database, name"
  );
}

/** ClickHouse's catalog read: one composer per kind. */
const CLICKHOUSE_CATALOG_READ: AgentCatalogRead = Object.freeze({
  compose: Object.freeze({
    columns: composeClickhouseColumns,
    relations: composeClickhouseRelations,
    indexes: composeClickhouseIndexes,
    statistics: composeClickhouseStatistics,
  }),
});

export const CLICKHOUSE_SERVER_OVERLAY = {
  dialect: Object.freeze({
    catalogRead: CLICKHOUSE_CATALOG_READ,
    planEstimate: CLICKHOUSE_PLAN_ESTIMATE,
    profileComposition: CLICKHOUSE_PROFILE_COMPOSITION,
    statistics: CLICKHOUSE_STATISTICS,
    catalogCapture: CLICKHOUSE_CATALOG_CAPTURE,
  } satisfies SqlServerDialect),
} as const satisfies AgentEngineServerOverlay<"sql">;
