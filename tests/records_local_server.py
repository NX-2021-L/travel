"""A tiny local HTTP server over the /records handler with synthetic fixtures and a synthetic door key (FLY-TSK-083).

    python tests/records_local_server.py [port]      prints one line: READY <port> <token>

The token is a freshly signed synthetic human-door token. Nothing here reaches AWS or any live store.
"""
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import records_fixture as fx  # noqa: E402

os.environ.update(fx.env())
os.environ.setdefault("AWS_REGION", "us-west-2")
fx.install()
import records_routes  # noqa: E402


class H(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        path, _, qs = self.path.partition("?")
        event = {"requestContext": {"http": {"method": "GET", "path": path}}, "rawPath": path, "rawQueryString": qs,
                 "headers": {k: v for k, v in self.headers.items()}}
        resp = records_routes.handle(event) or {"statusCode": 404, "headers": {}, "body": ""}
        body = resp["body"].encode()
        self.send_response(resp["statusCode"])
        for k, v in resp["headers"].items():
            self.send_header(k, v)
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_POST = do_GET

    def log_message(self, *a):
        pass


if __name__ == "__main__":
    srv = ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1]) if len(sys.argv) > 1 else 0), H)
    print("READY", srv.server_address[1], fx.token(exp_in=3600), flush=True)
    srv.serve_forever()
