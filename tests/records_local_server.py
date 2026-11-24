"""A tiny local HTTP server over the /records handler with synthetic fixtures and a synthetic door key (FLY-TSK-083).

    python tests/records_local_server.py [port]              READY <port> <token>                 read-only
    python tests/records_local_server.py [port] --writable   READY <port> <token> <other-token>   FLY-TSK-089

The tokens are freshly signed synthetic human-door tokens (the other one has another subject, for the corpus's
"another credential" cases). Nothing here reaches AWS or any live store.

Read-only: the routes read the fixture's static corrections (records_fixture.install).
Writable: an in-process moto store holds the corrections, written and read by the REAL lane_2e_corrections handlers
through the records door; trips are the real engine over the fixture's records and the stored corrections. The
server handles one request at a time, as the door's per-request state assumes.
"""
import os
import sys
import types
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import records_fixture as fx  # noqa: E402

WRITABLE = "--writable" in sys.argv[1:]
HOOKS = {}
os.environ.update(fx.env())
os.environ.setdefault("AWS_REGION", "us-west-2")


def _writable_world():
    os.environ.update({"RECORDS_TABLE_NAME": "fixture-records", "AWS_ACCESS_KEY_ID": "testing",
                       "AWS_SECRET_ACCESS_KEY": "testing", "AWS_SESSION_TOKEN": "testing"})
    import conftest  # noqa: F401  the test settings the function refuses to start without (it plays the deployment)
    try:
        import io_kit_mcp_edge  # noqa: F401
    except ImportError:  # CodeArtifact is unreachable on some machines; CI installs the real package
        stub = types.ModuleType("io_kit_mcp_edge")
        for n in ("BearerError", "EdgeConfig", "JwksUnavailable", "validate_bearer", "wrap"):
            setattr(stub, n, type(n, (Exception,), {}))
        jw = types.ModuleType("io_kit_mcp_edge.jwks")
        jw.fetch_https = lambda *a, **k: {}
        sys.modules["io_kit_mcp_edge"], sys.modules["io_kit_mcp_edge.jwks"] = stub, jw
    import boto3
    from moto import mock_aws
    mock_aws().start()
    import lambda_function as lf
    import lane_2e_corrections
    import lane_3a_trips
    import trip_engine
    d = boto3.resource("dynamodb", region_name=lf.REGION)
    d.create_table(TableName=os.environ["RECORDS_TABLE_NAME"], BillingMode="PAY_PER_REQUEST",
                   KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
                   AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "sk", "AttributeType": "S"}])
    flights = d.create_table(TableName=lf.TABLE_NAME, BillingMode="PAY_PER_REQUEST",
                             KeySchema=[{"AttributeName": "flight_id", "KeyType": "HASH"}],
                             AttributeDefinitions=[{"AttributeName": "flight_id", "AttributeType": "S"}])
    for r in fx.RECORDS:
        flights.put_item(Item={"flight_id": r["flight_id"]})

    async def compute(today=None, near_home_km=None):
        from datetime import date
        used = today or date.fromisoformat(fx.TODAY)
        extra = {} if near_home_km is None else {"near_home_km": near_home_km}
        return trip_engine.compute_trips(fx.RECORDS, [fx.HOME], today=used, corrections=lane_2e_corrections.engine_corrections(),
                                         **extra), used

    lane_3a_trips._compute = compute
    lane_3a_trips._today = lambda: __import__("datetime").date.fromisoformat(fx.TODAY)

    # The corpus's clock and conflict hints (expireUndo, conflictNextWrite), served on this test server only, never by
    # the function: POST /__fixture/expire-undo moves the door's clock past every undo window; POST
    # /__fixture/conflict-next-write makes the next retire answer a conflict at the current version, writing nothing.
    import records_writes
    from datetime import timedelta
    real_now, real_lane_now, skew, armed = records_writes._now, lane_2e_corrections._now, [timedelta(0)], [False]
    records_writes._now = lambda: real_now() + skew[0]  # the host clock moves for the door and the lane together
    lane_2e_corrections._now = lambda: (real_now() + skew[0]).isoformat(timespec="microseconds").replace("+00:00", "Z")
    real_kinds = dict(records_writes._KINDS)

    def armed_kind(kind):
        def run(routes, intent, caller, key):
            was, armed[0] = armed[0], False  # the forced conflict applies to the very next intent only
            if was and kind == "retire":
                current = routes._correction_row(next(c for c in lane_2e_corrections.load_corrections(None, include_retired=True)
                                                       if c["correction_id"] == intent.get("key")))
                raise records_writes.Refusal(409, "Conflict", "Another write landed first.", rows=[current], conflict=True)
            return real_kinds[kind](routes, intent, caller, key)
        return run
    for kind in real_kinds:
        records_writes._KINDS[kind] = armed_kind(kind)
    HOOKS["/__fixture/expire-undo"] = lambda: skew.__setitem__(0, skew[0] + records_writes.UNDO_WINDOW + timedelta(seconds=1))
    HOOKS["/__fixture/conflict-next-write"] = lambda: armed.__setitem__(0, True)


if WRITABLE:
    _writable_world()
else:
    fx.install()
import records_routes  # noqa: E402


class H(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        path, _, qs = self.path.partition("?")
        if self.command == "POST" and path in HOOKS:
            HOOKS[path]()
            self.send_response(204)
            self.send_header("content-length", "0")
            self.end_headers()
            return
        n = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(n).decode() if n else None
        event = {"requestContext": {"http": {"method": self.command, "path": path}}, "rawPath": path, "rawQueryString": qs,
                 "headers": {k: v for k, v in self.headers.items()}, "body": body, "isBase64Encoded": False}
        resp = records_routes.handle(event) or {"statusCode": 404, "headers": {}, "body": ""}
        body = resp["body"].encode()
        self.send_response(resp["statusCode"])
        for k, v in resp["headers"].items():
            self.send_header(k, v)
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_POST = do_DELETE = do_PUT = do_PATCH = do_GET

    def log_message(self, *a):
        pass


if __name__ == "__main__":
    port = next((int(a) for a in sys.argv[1:] if a.isdigit()), 0)
    srv = (HTTPServer if WRITABLE else ThreadingHTTPServer)(("127.0.0.1", port), H)
    other = [fx.token(exp_in=3600, sub="fixture-other-sub")] if WRITABLE else []
    print("READY", srv.server_address[1], fx.token(exp_in=3600), *other, flush=True)
    srv.serve_forever()
