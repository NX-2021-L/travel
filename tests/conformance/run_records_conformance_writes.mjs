// FLY-TSK-089 (BRD DOC-D14184D324F0 T-2 write half): run every @io-kit/records REC-C group against the WRITABLE local
// server (tests/records_local_server.py --writable): synthetic records in an in-process store, the real corrections
// handlers behind the records door.
//   node run_records_conformance_writes.mjs <port> <token> <other-token>
// Hints name only what travel serves. trip_corrections is the mutable set: create and retire, reason required; undo of a
// create is a retire. Travel declares no amend, no new_version and no recompute, so a case that needs one of those (or a
// hint travel cannot honour, such as moving the clock past the undo window) skips with the runner's own reason and is
// listed in not_declared.json beside this file, with its reason. Exits 1 on any failure, and on any skip not listed.
import { readFileSync } from "node:fs";
import { runConformance } from "@io-kit/records/testing";

const [port, token, other] = process.argv.slice(2);
const results = await runConformance({
  baseUrl: `http://127.0.0.1:${port}/records`,
  doorToken: token,
  readOnly: false,
  hints: {
    mutableSet: "trip_corrections",
    auditSet: "trip_corrections",
    createPatch: { kind: "leave_out", record_ids: ["F-GONE"] },
    auditCreatePatch: { kind: "leave_out", record_ids: ["F-GONE"] },
    readonlyColumn: "author",
    outOfReachSet: "travel_records",
    unreachableKey: "fixture-unreachable-key",
    otherDoorToken: other,
    // test-server-only hooks (records_local_server.py --writable); the function serves neither
    expireUndo: () => fetch(`http://127.0.0.1:${port}/__fixture/expire-undo`, { method: "POST" }).then(() => undefined),
    conflictNextWrite: () => fetch(`http://127.0.0.1:${port}/__fixture/conflict-next-write`, { method: "POST" }).then(() => undefined),
  },
});
const NOT_DECLARED = new Set(Object.keys(JSON.parse(readFileSync(new URL("./not_declared.json", import.meta.url), "utf8"))).filter((k) => k.startsWith("REC-")));
let bad = 0;
for (const r of results) {
  const unexpected = r.status === "fail" || (r.status === "skip" && !NOT_DECLARED.has(r.id));
  if (unexpected) bad++;
  console.log(r.id, r.group, r.status, r.detail ?? "", unexpected ? "<-- unexpected" : "");
}
const count = (s) => results.filter((r) => r.status === s).length;
console.log(`all cases: ${count("pass")} pass, ${count("fail")} fail, ${count("skip")} skip (not declared: ${NOT_DECLARED.size})`);
process.exit(bad ? 1 : 0);
