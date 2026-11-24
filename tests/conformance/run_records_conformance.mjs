// FLY-TSK-083, FLY-TSK-088: run the @io-kit/records REC-C read groups against tests/records_local_server.py.
//   node run_records_conformance.mjs <port> <token>     (run from a directory where @io-kit/records@0.4.0 is installed)
// Read-only mode (contract 0.4.0): exactly the cases in READ_ONLY_CASES run and every case that posts an intent skips,
// so this run sends no write. The write groups are lane 2E's (FLY-TSK-089). Hints name only what travel serves:
// travel_records is the store the trips are built from; it exists behind the door but is not served as a set.
// Exits 1 on any failed case, and on a read-only case that skips for a reason other than "not served".
import { READ_ONLY_CASES, runConformance } from "@io-kit/records/testing";

const [port, token] = process.argv.slice(2);
const results = await runConformance({
  baseUrl: `http://127.0.0.1:${port}/records`,
  doorToken: token,
  readOnly: true,
  hints: { derivedSet: "trips", outOfReachSet: "travel_records" },
});
// Read-only cases travel cannot exercise yet: version_carries (no new_version set) and REC-C53 (needs a mutable set).
const NOT_SERVED = new Set(["REC-C53", "REC-C70"]);
let bad = 0;
for (const r of results) {
  const readOnly = READ_ONLY_CASES.includes(r.id);
  const unexpectedSkip = readOnly && r.status === "skip" && !NOT_SERVED.has(r.id);
  if (r.status === "fail" || unexpectedSkip) bad++;
  if (readOnly) console.log(r.id, r.group, r.status, r.detail ?? "");
}
const count = (s) => results.filter((r) => READ_ONLY_CASES.includes(r.id) && r.status === s).length;
console.log(`read-only cases: ${count("pass")} pass, ${count("fail")} fail, ${count("skip")} skip; write cases skipped: ${results.filter((r) => !READ_ONLY_CASES.includes(r.id)).length}`);
process.exit(bad ? 1 : 0);
