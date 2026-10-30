import { expect, test } from "bun:test";
import { composeTableProfile } from "@/lib/agent/table-profile";
import { engineServerDialect } from "@/lib/agent/languages/engines/server";

test("ClickHouse profiles scalar strings without applying string predicates to arrays", () => {
  const sql = composeTableProfile("clickhouse", { segments: ["telem", "messages"], depth: "pattern" }, [
    { name: "host", type: "LowCardinality(Nullable(String))", nullable: true, isPrimary: false },
    { name: "tags", type: "Array(String)", nullable: false, isPrimary: false },
    { name: "payload", type: "JSON", nullable: false, isPrimary: false },
  ]);
  expect(sql).toContain('count("host") AS present_0');
  expect(sql).toContain('match("host",');
  expect(sql).not.toContain('match("tags",');
  expect(sql).not.toContain('DISTINCT "payload"');
  expect(sql).toContain('FROM "telem"."messages"');
});

test("ClickHouse capture preserves nullable types and rejects malformed column inventories", () => {
  const capture = engineServerDialect("clickhouse").catalogCapture;
  if (capture === "unsupported") throw new Error("capture missing");
  const build = (columns: unknown) =>
    capture.build(new Map([["columns", [{ table_schema: "db", table_name: "t", relkind: "view", columns }]]]), (kind) =>
      String(kind),
    );
  expect(build([[1, "value", "Nullable(UInt64)"]]).get("db.t")?.columns[0].nullable).toBe(true);
  expect(build([[1, "value", "LowCardinality(Nullable(String))"]]).get("db.t")?.kind).toBe("view");
  for (const columns of [null, "{}", "invalid-json", [null], [[1, "name", null]]])
    expect(() => build(columns)).toThrow();
  expect(capture.build(new Map(), () => undefined).size).toBe(0);
});
