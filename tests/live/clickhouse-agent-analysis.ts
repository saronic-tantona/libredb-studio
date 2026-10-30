/** Run against a disposable ClickHouse 26.7 server with a SELECT-only fixture account.
 * Fixture: telem.messages(host, received_at, tags, payload), with the three rows documented below.
 * CLICKHOUSE_ANALYSIS_PORT=<loopback port> bun tests/live/clickhouse-agent-analysis.ts
 */
// oxlint-disable no-await-in-loop -- each check finishes before the next query uses the same fixture.
import assert from "node:assert/strict";
import { composeCatalogRead, composeEstimatingExplain } from "@/lib/agent/composed-sql";
import { captureContextSnapshot } from "@/lib/agent/context-snapshot";
import { AgentRunDeadline } from "@/lib/agent/deadline";
import { AGENT_WORKFLOW_BUDGETS } from "@/lib/agent/execution-policy";
import { AgentRepairLedger } from "@/lib/agent/repair-ledger";
import { readSchemaStatistics } from "@/lib/agent/schema-stats";
import { composeTableProfile, readTableProfile } from "@/lib/agent/table-profile";
import type { AgentToolContext } from "@/lib/agent/tools";
import { ExecutionArtifactStore } from "@/lib/db/operations/artifacts";
import { ExecutionBudgetTracker } from "@/lib/db/operations/budgets";
import { createCanonicalOperationRegistry } from "@/lib/db/operations/descriptors";
import { createTargetScope } from "@/lib/db/operations/policy";
import { ClickHouseProvider } from "@/lib/db/providers/sql/clickhouse";
import type { DatabaseConnection, QueryResult } from "@/lib/types";

const port = Number(process.env.CLICKHOUSE_ANALYSIS_PORT);
assert.ok(Number.isInteger(port) && port > 0, "Set CLICKHOUSE_ANALYSIS_PORT to a disposable fixture's loopback port");
const connection: DatabaseConnection = {
  id: "clickhouse-analysis",
  name: "Disposable telemetry",
  type: "clickhouse",
  host: "127.0.0.1",
  port,
  database: "telem",
  user: "agent",
  password: "fixture-password",
  createdAt: new Date(0),
};
const budget = { statementTimeoutMs: 2000, maxResultRows: 200, maxResultBytes: 262144 };
const provider = new ClickHouseProvider(connection, {}, { readOnly: true, sendsStatements: true });
const editor = new ClickHouseProvider({ ...connection, user: "planner" });
await provider.connect();
await editor.connect();
const context: AgentToolContext = {
  runId: "analysis-fixture",
  modelId: "fixture",
  mode: "agent",
  workflowType: "data-analysis",
  actor: { sessionId: "fixture", role: "user" },
  connection,
  capabilities: provider.getCapabilities(),
  labels: provider.getLabels(),
  registry: createCanonicalOperationRegistry(),
  scope: createTargetScope(connection.id),
  tracker: new ExecutionBudgetTracker(),
  artifacts: new ExecutionArtifactStore<QueryResult>({ ttlMs: 60000, maxArtifacts: 32 }),
  deadline: new AgentRunDeadline(AGENT_WORKFLOW_BUDGETS["data-analysis"].policy.budgets.maxTotalRunMs, Date.now),
  repairs: new AgentRepairLedger(),
  acquireProvider: async (_connection, profile) => (profile === "agent-operations" ? editor : provider),
};
try {
  const capture = await captureContextSnapshot(context);
  assert.equal(capture.kind, "captured");
  const table = capture.snapshot.objects.find((object) => object.name === "telem.messages");
  assert.ok(table);
  assert.equal(table.columns.length, 4);
  assert.equal(table.columns[0].type, "LowCardinality(Nullable(String))");
  assert.ok(!("unavailable" in table.indexes));
  assert.ok(table.indexes.every((index) => !index.unique));
  for (const depth of ["basic", "distribution", "pattern"] as const) {
    const sql = composeTableProfile("clickhouse", { segments: ["telem", "messages"], depth }, table.columns);
    const result = await provider.queryReadOnly(sql, budget);
    const profile = readTableProfile(table.name, depth, table.columns, result.rows);
    assert.equal(profile?.rowCount, 3);
    assert.equal(profile?.columns[0].present, 3);
  }
  const statistics = await readSchemaStatistics(context);
  assert.equal(statistics.kind, "read");
  assert.equal(
    (await provider.queryReadOnly(composeCatalogRead("clickhouse", { kind: "relations" }), budget)).rows.length,
    0,
  );
  const sql =
    "SELECT host, max(received_at) AS last_reported_at FROM telem.messages WHERE received_at >= toDateTime64('2026-10-29 19:00:00', 3, 'UTC') AND received_at < toDateTime64('2026-10-29 20:00:00', 3, 'UTC') GROUP BY host ORDER BY host";
  assert.deepEqual((await provider.queryReadOnly(sql, budget)).rows, [
    { host: "boat-a", last_reported_at: "2026-10-29 19:30:00.000" },
    { host: "sim-boat-b", last_reported_at: "2026-10-29 19:45:00.000" },
  ]);
  const explain = composeEstimatingExplain("clickhouse", sql);
  assert.ok((await provider.queryReadOnly(explain.sql, budget, explain.mode)).rows.length > 0);
  for (const statement of [
    "INSERT INTO telem.messages VALUES ('bad', now(), [], '{}')",
    "CREATE TEMPORARY TABLE bad (n UInt64)",
    "SELECT 1 SETTINGS max_threads=100",
    "SELECT 1 SETTINGS max_memory_usage=0",
    "SELECT 1 SETTINGS max_result_bytes=0",
    "SELECT 1 SETTINGS timeout_overflow_mode='break'",
    "SELECT 1 SETTINGS max_result_rows=1000",
  ])
    await assert.rejects(provider.queryReadOnly(statement, budget));
  await assert.rejects(provider.queryReadOnly("SELECT repeat('x', 10000)", { ...budget, maxResultBytes: 100 }));
  await assert.rejects(provider.queryReadOnly("SELECT number FROM numbers(3)", { ...budget, maxResultRows: 2 }));
  assert.equal((await provider.queryReadOnly("SELECT count() AS n FROM telem.messages", budget)).rows[0].n, "3");
  const plan = await captureContextSnapshot({ ...context, mode: "planning" });
  assert.equal(plan.kind, "captured", JSON.stringify(plan));
  console.log(
    "PASS ClickHouse: bounded schema capture, profiles, estimates, telemetry answer, EXPLAIN, immutable budgets, write denial, planning inventory",
  );
} finally {
  await provider.disconnect();
  await editor.disconnect();
}
