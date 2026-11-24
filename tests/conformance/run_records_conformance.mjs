// FLY-TSK-083: run the @io-kit/records REC-C read groups against tests/records_local_server.py.
//   node run_records_conformance.mjs <port> <token>     (run from a directory where @io-kit/records@0.1.0 is installed)
// Hints name only what travel serves. travel advertises no write capability, so there is no mutable set: every case that
// needs one is reported as it runs (it fails or skips; the runner has no read-only mode).
import { runConformance } from "@io-kit/records/testing";

const [port, token] = process.argv.slice(2);
const results = await runConformance({
  baseUrl: `http://127.0.0.1:${port}/records`,
  doorToken: token,
  hints: { derivedSet: "trips" },
});
for (const r of results) console.log(r.id, r.group, r.status, r.detail ?? "");
